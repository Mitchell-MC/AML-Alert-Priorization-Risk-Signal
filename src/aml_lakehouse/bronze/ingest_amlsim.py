"""Bronze ingestion for IBM AMLSim's pre-generated sample account/transaction graph.

Reads already-staged, already-extracted files -- scripts/stage_raw_files.py downloads and
extracts the tarball before this job runs (see bronze/ingest_ofac.py for why: no outbound
internet from Databricks serverless compute).

Accounts (nodes.csv) are genuinely static reference data and get a normal batch load into
bronze.txn_accounts here. Transactions are the streaming-style dataset per
docs/03_schema_contracts.md ("bronze.txn_events ... streamed in micro-batches") -- this
module's job is only to load accounts and point at the staged *raw* transactions.csv;
turning it into a simulated live feed and consuming it into bronze.txn_events with
checkpointing is streaming/simulate_txn_feed.py and streaming/txn_stream_ingest.py, kept
separate so "land the source once" and "replay it as a stream" aren't tangled into one job.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import IngestionMetadata

AMLSIM_SAMPLE_URL = "https://raw.githubusercontent.com/IBM/AMLSim/master/sample/20K_fanin200cycle200.tgz"
SCHEMA_VERSION = "1.0"

NODES_SCHEMA = StructType(
    [
        StructField("nodeid", IntegerType()),
        StructField("isFraud", IntegerType()),
        StructField("init_balance", DoubleType()),
        StructField("fraudStep", IntegerType()),
    ]
)


def _split_valid_invalid_accounts(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    is_valid = F.col("nodeid").isNotNull()
    return df.filter(is_valid), df.filter(~is_valid)


def run(spark: SparkSession, environment: str, volume_base: str) -> dict[str, int | str]:
    """Load AMLSim's already-staged accounts into Bronze, point at the staged raw transactions.

    volume_base/amlsim/{nodes.csv,transactions.csv} must already exist (staged by
    scripts/stage_raw_files.py). Returns {"txn_accounts": <row count>,
    "raw_transactions_path": <path for the stream producer to read from>}.
    """
    env = resolve_env(environment)
    dataset_dir = f"{volume_base}/amlsim"

    nodes_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(NODES_SCHEMA)
        .load(f"{dataset_dir}/nodes.csv")
    )
    valid_df, invalid_df = _split_valid_invalid_accounts(nodes_df)

    metadata = IngestionMetadata.create(
        source_file=AMLSIM_SAMPLE_URL, schema_version=SCHEMA_VERSION
    )
    target_table = env.table("bronze", "txn_accounts")
    stamped = valid_df
    for col_name, value in metadata.as_columns().items():
        stamped = stamped.withColumn(col_name, F.lit(value))
    stamped.write.format("delta").mode("append").saveAsTable(target_table)

    if not invalid_df.isEmpty():
        dl_stamped = invalid_df.withColumn("_error_reason", F.lit("nodeid missing"))
        for col_name, value in metadata.as_columns().items():
            dl_stamped = dl_stamped.withColumn(col_name, F.lit(value))
        dl_stamped.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(env.table("bronze", "txn_accounts_dead_letter"))

    # Raw transactions.csv is intentionally left as a plain file on the volume, untouched --
    # streaming/simulate_txn_feed.py reads it from here to drip-feed the streaming source dir.
    return {
        "txn_accounts": stamped.count(),
        "raw_transactions_path": f"{dataset_dir}/transactions.csv",
    }


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    volume = sys.argv[2] if len(sys.argv) > 2 else f"/Volumes/aml_{env_name}/bronze/raw_files"
    result = run(spark_session, env_name, volume)
    for key, value in result.items():
        print(f"{key}: {value}")
