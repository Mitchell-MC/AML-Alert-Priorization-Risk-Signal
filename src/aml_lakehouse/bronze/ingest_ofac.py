"""Bronze ingestion for OFAC SDN, ALT (aliases), ADD (addresses), and SDN_COMMENTS.

Run as a Databricks job task (see resources/jobs/bronze_ingestion_job.yml). Parses the
already-staged publication files with an explicit all-string schema (the source files are
headerless -- there's no header row to infer column names from), and appends to
bronze.ofac_* with the standard ingestion-metadata columns.

Files are staged into the Volume by scripts/stage_raw_files.py *before* this job runs, not
downloaded by this job itself -- Databricks Free Edition serverless compute has no outbound
internet egress (confirmed by an actual failed run, not assumed: a first version of this job
tried to urlopen() the OFAC endpoint directly and hit "Temporary failure in name resolution").
See docs/02_environment_and_branching.md for the staging step this depends on.

Every field is StringType by design (see docs/03_schema_contracts.md): OFAC's own files carry
no type information, and casting belongs in Silver, not Bronze, per the raw-fidelity contract.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import IngestionMetadata

OFAC_BASE_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports"
SCHEMA_VERSION = "1.0"

SDN_SCHEMA = StructType(
    [
        StructField("ent_num", StringType()),
        StructField("sdn_name", StringType()),
        StructField("sdn_type", StringType()),
        StructField("program", StringType()),
        StructField("title", StringType()),
        StructField("call_sign", StringType()),
        StructField("vess_type", StringType()),
        StructField("tonnage", StringType()),
        StructField("grt", StringType()),
        StructField("vess_flag", StringType()),
        StructField("vess_owner", StringType()),
        StructField("remarks", StringType()),
    ]
)

ALT_SCHEMA = StructType(
    [
        StructField("ent_num", StringType()),
        StructField("alt_num", StringType()),
        StructField("alt_type", StringType()),
        StructField("alt_name", StringType()),
        StructField("alt_remarks", StringType()),
    ]
)

ADD_SCHEMA = StructType(
    [
        StructField("ent_num", StringType()),
        StructField("add_num", StringType()),
        StructField("address", StringType()),
        StructField("city_state_zip_etc", StringType()),
        StructField("country", StringType()),
        StructField("add_remarks", StringType()),
    ]
)

COMMENTS_SCHEMA = StructType(
    [
        StructField("ent_num", StringType()),
        StructField("remarks_text", StringType()),
    ]
)

# (source filename, schema, target Bronze table name)
_FILES: dict[str, tuple[str, StructType, str]] = {
    "sdn": ("SDN.CSV", SDN_SCHEMA, "ofac_sdn"),
    "alt": ("ALT.CSV", ALT_SCHEMA, "ofac_alt"),
    "add": ("ADD.CSV", ADD_SCHEMA, "ofac_add"),
    "comments": ("SDN_COMMENTS.CSV", COMMENTS_SCHEMA, "ofac_sdn_comments"),
}


def _read_ofac_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    # multiLine=true is required, not optional: several OFAC remark/comment fields contain
    # literal embedded newlines inside quoted values, which single-line CSV parsing would
    # split into corrupt partial records.
    return (
        spark.read.format("csv")
        .option("header", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .schema(schema)
        .load(path)
    )


def _split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """A row is valid if ent_num is present and numeric -- OFAC's own primary key.

    Spark's CSV reader is lenient about ragged quoted rows for an all-string schema (it pads
    or truncates rather than raising), so a business-rule check on the primary key is what
    actually catches structurally broken rows here, not Spark's built-in _corrupt_record
    (which only fires on type-cast failures against a typed schema).
    """
    is_valid = F.col("ent_num").rlike(r"^\d+$")
    return df.filter(is_valid), df.filter(~is_valid | F.col("ent_num").isNull())


def _write_bronze(df: DataFrame, target_table: str, metadata: "IngestionMetadata") -> DataFrame:
    stamped = df
    for col_name, value in metadata.as_columns().items():
        stamped = stamped.withColumn(col_name, F.lit(value))
    stamped.write.format("delta").mode("append").saveAsTable(target_table)
    return stamped


def _write_dead_letter(df: DataFrame, target_table: str, metadata: "IngestionMetadata", reason: str) -> None:
    if df.isEmpty():
        return
    stamped = df.withColumn("_error_reason", F.lit(reason))
    for col_name, value in metadata.as_columns().items():
        stamped = stamped.withColumn(col_name, F.lit(value))
    stamped.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)


def run(spark: SparkSession, environment: str, volume_base: str) -> dict[str, int]:
    """Ingest all four already-staged OFAC files into <catalog>.bronze.ofac_*.

    volume_base: a Unity Catalog Volume path, e.g. /Volumes/aml_dev/bronze/raw_files --
    files must already exist at volume_base/ofac/<filename>, staged by
    scripts/stage_raw_files.py (this job cannot reach the internet itself).
    Returns {table_name: valid_row_count} for the caller to log against gold.ops_control.
    """
    env = resolve_env(environment)
    counts: dict[str, int] = {}

    for _key, (filename, schema, table_name) in _FILES.items():
        url = f"{OFAC_BASE_URL}/{filename}"  # recorded for lineage only, not fetched here
        local_path = f"{volume_base}/ofac/{filename}"

        raw_df = _read_ofac_csv(spark, local_path, schema)
        valid_df, invalid_df = _split_valid_invalid(raw_df)

        metadata = IngestionMetadata.create(source_file=url, schema_version=SCHEMA_VERSION)
        target_table = env.table("bronze", table_name)
        stamped = _write_bronze(valid_df, target_table, metadata)
        counts[table_name] = stamped.count()

        _write_dead_letter(
            invalid_df,
            env.table("bronze", f"{table_name}_dead_letter"),
            metadata,
            reason="ent_num missing or non-numeric",
        )

    return counts


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    volume = sys.argv[2] if len(sys.argv) > 2 else f"/Volumes/aml_{env_name}/bronze/raw_files"
    result = run(spark_session, env_name, volume)
    for table_name, row_count in result.items():
        print(f"{table_name}: {row_count} rows")
