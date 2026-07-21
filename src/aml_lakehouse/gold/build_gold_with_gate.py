"""Build Gold tables with reconciliation gate checks and ops telemetry."""
from __future__ import annotations

from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import new_batch_id
from aml_lakehouse.common.ops_control import record_batch
from aml_lakehouse.common.risk_guardrails import DataContractError
from aml_lakehouse.common.sql_runner import run_sql_script
from aml_lakehouse.common.upstream_registry import get_dependency_metadata

SOURCE_TARGET_ROW_RATIO_FLOOR = 0.50
SOURCE_TARGET_ROW_RATIO_CEIL = 1.50


def _reconciliation_checks(spark: SparkSession, catalog: str) -> dict[str, float]:
    txns = spark.sql(f"SELECT COUNT(*) AS c FROM {catalog}.silver.transaction").collect()[0]["c"]
    profiles = spark.sql(
        f"SELECT COUNT(*) AS c FROM {catalog}.gold.entity_risk_profile"
    ).collect()[0]["c"]
    alerts = spark.sql(
        f"SELECT COUNT(*) AS c FROM {catalog}.gold.prioritized_alert_queue"
    ).collect()[0]["c"]

    if txns <= 0 or profiles <= 0:
        raise DataContractError(
            "Gold reconciliation gate failed: source/target row counts must be non-zero"
        )

    ratio = profiles / txns
    if ratio < SOURCE_TARGET_ROW_RATIO_FLOOR or ratio > SOURCE_TARGET_ROW_RATIO_CEIL:
        raise DataContractError(
            "Gold reconciliation gate failed: row ratio outside tolerance "
            f"({ratio:.4f} not in [{SOURCE_TARGET_ROW_RATIO_FLOOR}, {SOURCE_TARGET_ROW_RATIO_CEIL}])"
        )

    return {"silver_transaction_rows": txns, "gold_profile_rows": profiles, "gold_alert_rows": alerts}


def run(spark: SparkSession, environment: str) -> dict[str, float]:
    started = perf_counter()
    env = resolve_env(environment)
    sql_path = Path(__file__).with_name("build_gold.sql")
    run_sql_script(spark, str(sql_path), catalog=env.catalog)

    metrics = _reconciliation_checks(spark, env.catalog)
    dep = get_dependency_metadata("gold_month_end_publish")
    batch_id = new_batch_id()
    record_batch(
        spark,
        env.table("gold", "ops_control"),
        pipeline_name="gold_month_end_publish",
        batch_id=batch_id,
        processed_row_count=int(metrics["gold_profile_rows"]),
        expected_row_count=int(metrics["silver_transaction_rows"]),
        lag_seconds=0.0,
        dead_letter_count=0,
        checkpoint_offset=None,
        business_process=dep["business_process"],
        impacted_table=dep["impacted_table"],
        last_good_batch_age_minutes=0.0,
        upstream_owner=dep["upstream_owner"],
        upstream_contact=dep["upstream_contact"],
        runtime_seconds=perf_counter() - started,
        estimated_cloud_cost_usd=round(float(metrics["gold_profile_rows"]) * 0.0000008, 6),
    )
    return metrics


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    result = run(spark_session, env_name)
    for key, value in result.items():
        print(f"{key}: {value}")