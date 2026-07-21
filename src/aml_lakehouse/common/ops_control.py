"""Shared writer for gold.ops_control -- the silent-failure/ops-health control table every
pipeline (streaming or batch) reports into. See docs/03_schema_contracts.md and the
freshness SLA in docs/01_nonfunctional_requirements.md.
"""
from __future__ import annotations

from datetime import datetime, timezone

FRESH_THRESHOLD_SECONDS = 15 * 60  # NFR: no more than 15 minutes stale under normal operation
DEGRADED_THRESHOLD_SECONDS = 3 * FRESH_THRESHOLD_SECONDS


def _ops_control_schema():
    # Imported lazily, not at module top level -- this module's pure-Python pieces
    # (freshness_status) are unit-tested without pyspark installed; only record_batch()
    # actually needs Spark, so only it should require the import.
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    # Explicit schema, not inferred -- confirmed by an actual failed run that
    # spark.createDataFrame([row]) can't infer a type for a column that's None on every row
    # of a single-row batch (expected_row_count is legitimately always None from the
    # streaming caller, since it has no independent "expected" count to compare against).
    # CANNOT_DETERMINE_TYPE.
    return StructType(
        [
            StructField("pipeline_name", StringType()),
            StructField("business_process", StringType()),
            StructField("impacted_table", StringType()),
            StructField("batch_id", StringType()),
            StructField("last_successful_batch_id", StringType()),
            StructField("last_good_batch_age_minutes", DoubleType()),
            StructField("processed_row_count", IntegerType()),
            StructField("expected_row_count", IntegerType()),
            StructField("lag_seconds", DoubleType()),
            StructField("freshness_status", StringType()),
            StructField("dead_letter_count", IntegerType()),
            StructField("checkpoint_offset", StringType()),
            StructField("upstream_owner", StringType()),
            StructField("upstream_contact", StringType()),
            StructField("runtime_seconds", DoubleType()),
            StructField("estimated_cloud_cost_usd", DoubleType()),
            StructField("recorded_at", TimestampType()),
        ]
    )


def freshness_status(lag_seconds: float) -> str:
    if lag_seconds <= FRESH_THRESHOLD_SECONDS:
        return "fresh"
    if lag_seconds <= DEGRADED_THRESHOLD_SECONDS:
        return "stale"
    return "degraded"


def record_batch(
    spark,
    ops_control_table: str,
    pipeline_name: str,
    batch_id: str,
    processed_row_count: int,
    expected_row_count: int | None,
    lag_seconds: float,
    dead_letter_count: int,
    checkpoint_offset: str | None,
    last_successful_batch_id: str | None = None,
    business_process: str = "unspecified",
    impacted_table: str = "unspecified",
    last_good_batch_age_minutes: float | None = None,
    upstream_owner: str | None = None,
    upstream_contact: str | None = None,
    runtime_seconds: float | None = None,
    estimated_cloud_cost_usd: float | None = None,
) -> None:
    row = (
        pipeline_name,
        business_process,
        impacted_table,
        batch_id,
        last_successful_batch_id or batch_id,
        last_good_batch_age_minutes,
        processed_row_count,
        expected_row_count,
        lag_seconds,
        freshness_status(lag_seconds),
        dead_letter_count,
        checkpoint_offset,
        upstream_owner,
        upstream_contact,
        runtime_seconds,
        estimated_cloud_cost_usd,
        datetime.now(timezone.utc),
    )
    spark.createDataFrame([row], schema=_ops_control_schema()).write.format("delta").mode(
        "append"
    ).saveAsTable(ops_control_table)
