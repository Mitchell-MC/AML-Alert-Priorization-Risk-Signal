"""Bronze ingestion for OpenSanctions' sanctions + PEP collections.

Reads already-staged files -- see scripts/stage_raw_files.py and the note in
bronze/ingest_ofac.py about why fetching happens outside Databricks (no serverless egress).
The "latest" alias URL noted below is what the staging script fetches from, recorded here as
lineage documentation, not something this job calls directly.

Uses OpenSanctions' stable "latest" alias path (data.opensanctions.org/datasets/latest/...)
rather than the timestamped run-specific URL shown on the dataset's web page -- the
run-specific URL (.../sanctions/20260718194701-jrj/...) changes on every publish and would
silently start 404ing in production ingestion code. The "latest" alias always resolves to
the current publish.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import IngestionMetadata

SCHEMA_VERSION = "1.0"

_COLUMNS = (
    "id", "schema", "name", "aliases", "birth_date", "countries", "addresses",
    "identifiers", "sanctions", "phones", "emails", "program_ids", "dataset",
    "first_seen", "last_seen", "last_change",
)
TARGETS_SIMPLE_SCHEMA = StructType([StructField(c, StringType()) for c in _COLUMNS])

# (OpenSanctions collection slug, target Bronze table name)
_COLLECTIONS: dict[str, str] = {
    "sanctions": "opensanctions_sanctions",
    "peps": "opensanctions_peps",
}


def _read_targets_simple_csv(spark: SparkSession, path: str) -> DataFrame:
    # OpenSanctions' aliases field uses RFC4180 doubled-quote escaping for names containing
    # literal quotes/semicolons -- header=true here because, unlike OFAC, this export ships
    # with a real header row, but the schema is still passed explicitly so column types/order
    # are pinned rather than inferred (inference would silently drift if OpenSanctions ever
    # reorders or adds columns).
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .schema(TARGETS_SIMPLE_SCHEMA)
        .load(path)
    )


def _split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    is_valid = F.col("id").isNotNull() & (F.trim(F.col("id")) != "")
    return df.filter(is_valid), df.filter(~is_valid)


def run(spark: SparkSession, environment: str, volume_base: str) -> dict[str, int]:
    """Ingest the already-staged sanctions and PEP collections into <catalog>.bronze.opensanctions_*.

    volume_base: a Unity Catalog Volume path, e.g. /Volumes/aml_dev/bronze/raw_files -- files
    must already exist at volume_base/opensanctions/<collection>_targets_simple.csv.
    Returns {table_name: valid_row_count}.
    """
    env = resolve_env(environment)
    counts: dict[str, int] = {}

    for collection, table_name in _COLLECTIONS.items():
        url = f"https://data.opensanctions.org/datasets/latest/{collection}/targets.simple.csv"
        local_path = f"{volume_base}/opensanctions/{collection}_targets_simple.csv"

        raw_df = _read_targets_simple_csv(spark, local_path)
        valid_df, invalid_df = _split_valid_invalid(raw_df)

        metadata = IngestionMetadata.create(source_file=url, schema_version=SCHEMA_VERSION)
        target_table = env.table("bronze", table_name)
        stamped = valid_df
        for col_name, value in metadata.as_columns().items():
            stamped = stamped.withColumn(col_name, F.lit(value))
        stamped.write.format("delta").mode("append").saveAsTable(target_table)
        counts[table_name] = stamped.count()

        if not invalid_df.isEmpty():
            dl_stamped = invalid_df.withColumn("_error_reason", F.lit("missing id"))
            for col_name, value in metadata.as_columns().items():
                dl_stamped = dl_stamped.withColumn(col_name, F.lit(value))
            dl_stamped.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(env.table("bronze", f"{table_name}_dead_letter"))

    return counts


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    volume = sys.argv[2] if len(sys.argv) > 2 else f"/Volumes/aml_{env_name}/bronze/raw_files"
    result = run(spark_session, env_name, volume)
    for table_name, row_count in result.items():
        print(f"{table_name}: {row_count} rows")
