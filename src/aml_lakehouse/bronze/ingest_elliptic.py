"""Bronze ingestion for the Elliptic Bitcoin dataset (secondary network-risk signal).

Reads already-staged files -- scripts/stage_raw_files.py fetches this via kagglehub before
this job runs (Databricks serverless compute has no outbound internet egress, see
bronze/ingest_ofac.py). See docs/03_schema_contracts.md's "Elliptic integration caveat" --
this feeds silver.elliptic_txn_risk / silver.network_risk_reference, not the primary
transaction pipeline.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import IngestionMetadata

KAGGLE_DATASET = "ellipticco/elliptic-data-set"
SCHEMA_VERSION = "1.0"
NUM_FEATURE_COLUMNS = 165

CLASSES_SCHEMA = StructType(
    [
        StructField("txId", StringType()),
        StructField("class", StringType()),  # "1" | "2" | "unknown" -- kept as string on purpose
    ]
)

EDGELIST_SCHEMA = StructType(
    [
        StructField("txId1", StringType()),
        StructField("txId2", StringType()),
    ]
)


def _read_features(spark: SparkSession, path: str) -> DataFrame:
    # Headerless: col 1 = txId, col 2 = time_step, cols 3-167 = 165 anonymized/PCA-derived
    # features. Collapsed into one array<double> column rather than 165 named columns --
    # they aren't individually interpretable (see dataset_inspection_notes.md), so 165
    # separate contract columns would be pure noise.
    raw_columns = ["txId", "time_step"] + [f"f{i}" for i in range(1, NUM_FEATURE_COLUMNS + 1)]
    schema = StructType(
        [StructField("txId", StringType()), StructField("time_step", IntegerType())]
        + [StructField(name, DoubleType()) for name in raw_columns[2:]]
    )
    raw_df = spark.read.format("csv").option("header", "false").schema(schema).load(path)
    feature_cols = [F.col(f"f{i}") for i in range(1, NUM_FEATURE_COLUMNS + 1)]
    return raw_df.select(
        "txId", "time_step", F.array(*feature_cols).alias("features")
    )


def run(spark: SparkSession, environment: str, volume_base: str) -> dict[str, int]:
    """volume_base/elliptic/elliptic_txs_{classes,edgelist,features}.csv must already exist
    (staged by scripts/stage_raw_files.py)."""
    env = resolve_env(environment)
    dataset_dir = f"{volume_base}/elliptic"

    metadata = IngestionMetadata.create(
        source_file=f"kaggle:{KAGGLE_DATASET}", schema_version=SCHEMA_VERSION
    )
    counts: dict[str, int] = {}

    classes_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(CLASSES_SCHEMA)
        .load(f"{dataset_dir}/elliptic_txs_classes.csv")
    )
    edgelist_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(EDGELIST_SCHEMA)
        .load(f"{dataset_dir}/elliptic_txs_edgelist.csv")
    )
    features_df = _read_features(spark, f"{dataset_dir}/elliptic_txs_features.csv")

    for df, table_name in (
        (classes_df, "elliptic_txs_classes"),
        (edgelist_df, "elliptic_txs_edgelist"),
        (features_df, "elliptic_txs_features"),
    ):
        stamped = df
        for col_name, value in metadata.as_columns().items():
            stamped = stamped.withColumn(col_name, F.lit(value))
        stamped.write.format("delta").mode("append").saveAsTable(env.table("bronze", table_name))
        counts[table_name] = stamped.count()

    return counts


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    volume = sys.argv[2] if len(sys.argv) > 2 else f"/Volumes/aml_{env_name}/bronze/raw_files"
    result = run(spark_session, env_name, volume)
    for table_name, row_count in result.items():
        print(f"{table_name}: {row_count} rows")
