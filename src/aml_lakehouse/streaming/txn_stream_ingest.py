"""Structured Streaming consumer for transaction events -> bronze.txn_events.

Reliability features (Phase 2/6 of the plan):
  - checkpointing: file-source offsets + foreachBatch state, standard exactly-once semantics
    for file source -> Delta sink.
  - watermarking: bounds dedup state (see below) rather than growing it forever.
  - watermark-bounded dropDuplicates: defense-in-depth against duplicate delivery on top of
    (not instead of) the checkpoint's own exactly-once guarantee -- e.g. if a producer bug
    ever re-delivers the same logical event across two different files.
  - malformed-record quarantine: rows failing validation go to bronze.txn_events_dead_letter
    instead of being silently dropped or silently null-cast.
  - ops_control reporting: every micro-batch reports row counts, dead-letter counts, and lag
    into gold.ops_control so ingestion stalls are visible rather than silent.

Reads whatever directory simulate_txn_feed.py (or, in a real deployment, the actual payments
platform's event landing zone) drops files into.

**Trigger mode, corrected after an actual failed run**: Databricks Free Edition serverless
compute rejects an infinite/always-on trigger outright --
`[INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED] Trigger type ProcessingTime is not supported for
this cluster type. Use a different trigger type e.g. AvailableNow, Once.` Serverless compute
is built for bounded, ephemeral execution, not a process that polls forever. This job uses
`trigger(availableNow=True)`: one run processes everything currently available then
terminates on its own. The freshness SLA in docs/01_nonfunctional_requirements.md (data no
more than ~15 min stale) is met by *scheduling* this job to run every few minutes (a
Databricks Job schedule, not an in-process loop) rather than by a continuously-running query
-- each scheduled run resumes from the last checkpoint exactly like a restart would, which
also means the streaming resilience drill's "stop/restart, confirm no duplicate
amplification" check is really just "does the next scheduled run behave correctly," not a
special case. A provisioned (non-serverless) cluster would support a true always-on
`processingTime` trigger if that becomes a requirement later.
"""
from __future__ import annotations

from time import perf_counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import IngestionMetadata
from aml_lakehouse.common.ops_control import record_batch
from aml_lakehouse.common.risk_guardrails import require_invalid_ratio_below
from aml_lakehouse.common.schema_drift import record_schema_snapshot, table_fields
from aml_lakehouse.common.structured_logging import get_logger, log_event
from aml_lakehouse.common.time_anchor import step_to_event_time_expr
from aml_lakehouse.common.upstream_registry import get_dependency_metadata

RAW_SCHEMA = StructType(
    [
        StructField("sourceNodeId", StringType()),
        StructField("targetNodeId", StringType()),
        StructField("value", StringType()),
        StructField("time", StringType()),
    ]
)

WATERMARK_DELAY = "10 minutes"
SCHEMA_VERSION = "1.0"
MAX_INVALID_RATIO = 0.10
LOGGER = get_logger(__name__)


def _add_validity_and_event_time(raw_stream: DataFrame) -> DataFrame:
    # try_cast, not cast, throughout this function -- confirmed by an actual failed run that
    # this workspace runs Spark in ANSI mode, where plain cast() *raises* on unparseable input
    # (e.g. the literal string "NOT_A_NUMBER") instead of returning null. A validity check
    # that crashes on the exact malformed input it exists to catch defeats the entire
    # quarantine design, so every cast used for *checking* validity must be a try_cast.
    source_valid = F.col("sourceNodeId").rlike(r"^\d+$")
    target_valid = F.col("targetNodeId").rlike(r"^\d+$")
    value_valid = (F.trim(F.col("value")) != "") & F.expr(
        "try_cast(value AS DOUBLE)"
    ).isNotNull()
    time_valid = F.expr("try_cast(time AS INT)") > 0

    # coalesce(..., False) is required, not decorative: any null sub-expression (e.g. a null
    # sourceNodeId makes rlike() return null, not false) would otherwise make the whole AND
    # chain null. filter(col) drops both false AND null rows, so an un-coalesced null here
    # would make bad rows vanish silently instead of landing in the dead-letter table --
    # exactly the silent-failure mode this pipeline exists to avoid.
    is_valid = F.coalesce(source_valid & target_valid & value_valid & time_valid, F.lit(False))

    enriched = raw_stream.withColumn("_is_valid", is_valid).withColumn(
        "event_time", F.expr(step_to_event_time_expr("try_cast(time AS INT)"))
    )
    # Invalid rows may have a null event_time (unparseable time); coalesce to current
    # processing time so the watermark still advances even on a batch that's all garbage.
    return enriched.withColumn("event_time", F.coalesce(F.col("event_time"), F.current_timestamp()))


_NATURAL_KEY_COLUMNS = ("sourceNodeId", "targetNodeId", "value", "time")


def _merge_append(spark: SparkSession, source_df: DataFrame, target_table: str) -> None:
    """Idempotent append: MERGE ... WHEN NOT MATCHED THEN INSERT keyed on the natural
    (sourceNodeId, targetNodeId, value, time) tuple, instead of a plain .write.mode("append").

    Found necessary by an actual resilience test, not designed in speculatively: an early
    version used plain append, and a crash-mid-batch followed by rerunning the job against
    the *same* checkpoint location produced exactly 2x duplicate rows (235,406 total vs
    117,703 distinct) -- Spark's checkpoint offset tracking did not, by itself, prevent a
    reprocessed batch from re-inserting rows that had already been committed. The
    watermark-bounded dropDuplicates upstream only dedupes within one continuous query's
    state; it does not protect against this cross-restart case. MERGE keyed on the row's own
    natural identity does: replaying the same batch after a restart just no-ops on rows that
    already exist instead of inserting them again.
    """
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(target_table):
        source_df.limit(0).write.format("delta").saveAsTable(target_table)

    target = DeltaTable.forName(spark, target_table)
    match_condition = " AND ".join(f"t.{c} = s.{c}" for c in _NATURAL_KEY_COLUMNS)
    (
        target.alias("t")
        .merge(source_df.alias("s"), match_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _make_process_batch(spark: SparkSession, env, metadata: IngestionMetadata):
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        started = perf_counter()
        dependency_meta = get_dependency_metadata("txn_stream_ingest")
        # No .persist() here -- serverless compute rejects it outright
        # ([NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported), confirmed by an
        # actual failed run. valid_df/invalid_df get recomputed from batch_df on each action
        # below; fine at this micro-batch's data volume, not worth fighting the platform for.
        valid_df = batch_df.filter(F.col("_is_valid")).drop("_is_valid")
        invalid_df = batch_df.filter(~F.col("_is_valid")).drop("_is_valid")

        processed_count = valid_df.count()
        dead_letter_count = invalid_df.count()
        invalid_ratio = require_invalid_ratio_below(
            valid_count=processed_count,
            invalid_count=dead_letter_count,
            max_invalid_ratio=MAX_INVALID_RATIO,
            dataset="bronze.txn_events stream",
        )

        log_event(
            LOGGER,
            "txn_stream_ingest.batch_quality",
            pipeline_name="txn_stream_ingest",
            batch_id=str(batch_id),
            processed_count=processed_count,
            dead_letter_count=dead_letter_count,
            invalid_ratio=round(invalid_ratio, 6),
            circuit_breaker_max_invalid_ratio=MAX_INVALID_RATIO,
        )

        if processed_count > 0:
            # try_cast again here even though these rows already passed _is_valid -- cheap
            # extra safety net against ANSI mode raising on any edge case the validity check
            # didn't anticipate, consistent with _add_validity_and_event_time above.
            cast_df = valid_df.select(
                F.expr("try_cast(sourceNodeId AS INT)").alias("sourceNodeId"),
                F.expr("try_cast(targetNodeId AS INT)").alias("targetNodeId"),
                F.expr("try_cast(value AS DOUBLE)").alias("value"),
                F.expr("try_cast(time AS INT)").alias("time"),
                "event_time",
                "_ingested_at",
                "_batch_id",
                "_source_file",
                "_schema_version",
            )
            _merge_append(spark, cast_df, env.table("bronze", "txn_events"))

        if dead_letter_count > 0:
            dl_df = invalid_df.withColumn(
                "_error_reason", F.lit("failed sourceNodeId/targetNodeId/value/time validation")
            )
            dl_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
                env.table("bronze", "txn_events_dead_letter")
            )

        if processed_count > 0:
            lag_row = valid_df.agg(
                (
                    F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("event_time"))
                ).alias("lag_seconds")
            ).collect()[0]
            lag_seconds = float(lag_row["lag_seconds"])
        else:
            lag_seconds = float("inf")

        record_batch(
            spark,
            env.table("gold", "ops_control"),
            pipeline_name="txn_stream_ingest",
            batch_id=str(batch_id),
            processed_row_count=processed_count,
            expected_row_count=None,
            lag_seconds=lag_seconds,
            dead_letter_count=dead_letter_count,
            checkpoint_offset=str(batch_id),
            business_process=dependency_meta["business_process"],
            impacted_table=dependency_meta["impacted_table"],
            last_good_batch_age_minutes=(lag_seconds / 60.0 if lag_seconds != float("inf") else None),
            upstream_owner=dependency_meta["upstream_owner"],
            upstream_contact=dependency_meta["upstream_contact"],
            runtime_seconds=perf_counter() - started,
            estimated_cloud_cost_usd=round(processed_count * 0.000002, 6),
        )

        if processed_count > 0 and spark.catalog.tableExists(env.table("bronze", "txn_events")):
            drift = record_schema_snapshot(
                spark,
                env.table("gold", "ops_schema_drift"),
                pipeline_name="txn_stream_ingest",
                table_name=env.table("bronze", "txn_events"),
                fields=table_fields(spark, env.table("bronze", "txn_events")),
            )
            log_event(
                LOGGER,
                "txn_stream_ingest.schema_snapshot",
                pipeline_name="txn_stream_ingest",
                table_name=env.table("bronze", "txn_events"),
                is_breaking=drift["is_breaking"],
                previous_hash=drift["previous_hash"],
                current_hash=drift["current_hash"],
            )

        log_event(
            LOGGER,
            "txn_stream_ingest.batch_complete",
            pipeline_name="txn_stream_ingest",
            batch_id=str(batch_id),
            lag_seconds=lag_seconds,
            processed_count=processed_count,
            dead_letter_count=dead_letter_count,
        )

    return process_batch


def run(
    spark: SparkSession,
    environment: str,
    source_dir: str,
    checkpoint_base: str,
) -> StreamingQuery:
    env = resolve_env(environment)

    raw_stream = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(RAW_SCHEMA)
        .load(source_dir)
    )
    enriched = _add_validity_and_event_time(raw_stream)

    watermarked = enriched.withWatermark("event_time", WATERMARK_DELAY).dropDuplicates(
        ["sourceNodeId", "targetNodeId", "value", "time"]
    )

    metadata = IngestionMetadata.create(source_file=source_dir, schema_version=SCHEMA_VERSION)
    stamped = watermarked
    for col_name, value in metadata.as_columns().items():
        stamped = stamped.withColumn(col_name, F.lit(value))

    checkpoint_location = f"{checkpoint_base}/txn_stream_ingest"
    return (
        stamped.writeStream.foreachBatch(_make_process_batch(spark, env, metadata))
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .start()
    )


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    src_dir = sys.argv[2] if len(sys.argv) > 2 else f"/Volumes/aml_{env_name}/bronze/raw_files/txn_stream_source"
    ckpt_base = sys.argv[3] if len(sys.argv) > 3 else f"/Volumes/aml_{env_name}/bronze/checkpoints"
    # Optional bounded runtime in seconds -- unset means run forever (the real production
    # shape, e.g. as an always-on Databricks Job). Set for validation/demo job runs so the
    # job task actually terminates instead of running until manually cancelled.
    timeout_seconds = int(sys.argv[4]) if len(sys.argv) > 4 else None

    streaming_query = run(spark_session, env_name, src_dir, ckpt_base)
    if timeout_seconds is not None:
        streaming_query.awaitTermination(timeout=timeout_seconds)
        streaming_query.stop()
    else:
        streaming_query.awaitTermination()
