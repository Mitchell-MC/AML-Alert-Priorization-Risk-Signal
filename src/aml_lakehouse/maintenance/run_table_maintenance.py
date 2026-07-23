"""Scheduled table maintenance: liquid clustering, compaction (OPTIMIZE), and history
cleanup (VACUUM) for the high-volume Delta tables.

Why this exists (docs/12_performance_and_layout.md): the continuously-appended streaming
target and the rebuilt Silver/Gold tables accumulate small files ("file confetti") that make
reads and MERGEs crawl. This job compacts them on a schedule and keeps the physical layout
matched to how each table is actually filtered.

Two categories of table:

  * CLUSTERED_INLINE -- clustered/partitioned in their CTAS (gold.*, silver.transaction).
    CREATE OR REPLACE re-declares the spec every build, so here we only OPTIMIZE + VACUUM.
  * NEEDS_CLUSTERING -- created by DataFrame overwrite or the streaming MERGE with no
    clustering spec. We set liquid clustering + file-size properties once (the spec survives
    subsequent `mode("overwrite")` writes and streaming appends because those replace data,
    not table metadata), then OPTIMIZE + VACUUM.

Serverless vs. classic (docs/12): on serverless, predictive optimization may already compact
managed tables automatically, so this job is partly belt-and-suspenders there; on classic /
dedicated clusters it is load-bearing because auto-compaction is not guaranteed. It is
written to be correct on both -- OPTIMIZE and VACUUM are idempotent and safe to re-run.

Fail-loud by design (no broad exception handling -- blocked by scripts/check_ai_risk_patterns.py):
a table that does not exist yet is skipped via an explicit tableExists() check with a logged
reason, not a swallowed error. Any real failure (permissions, corrupt table) propagates.

VACUUM uses the default 7-day (168h) retention, which is intentionally >= the day-over-day
time-travel window that resources/ops/day_over_day_variance.sql relies on. Do not lower it.
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.structured_logging import get_logger, log_event

LOGGER = get_logger(__name__)

VACUUM_RETAIN_HOURS = 168  # 7 days -- the Delta default; preserves the day-over-day window.
TABLE_PROPERTIES = {
    "delta.targetFileSize": "134217728",  # ~128MB
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

# (schema, table): clustered/partitioned inline in the CTAS -- OPTIMIZE + VACUUM only.
CLUSTERED_INLINE = (
    ("silver", "transaction"),
    ("gold", "entity_risk_profile"),
    ("gold", "prioritized_alert_queue"),
)

# (schema, table): clustering column(s) -- set liquid clustering + properties, then maintain.
NEEDS_CLUSTERING = {
    ("bronze", "txn_events"): ("sourceNodeId", "targetNodeId"),
    ("silver", "account"): ("account_id",),
    ("silver", "match_candidate"): ("account_id",),
    ("silver", "network_risk_reference"): ("account_id",),
}


def _set_properties(spark: SparkSession, table: str) -> None:
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({props})")


def _set_clustering(spark: SparkSession, table: str, columns: tuple[str, ...]) -> None:
    spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(columns)})")


def _optimize_and_vacuum(spark: SparkSession, table: str) -> None:
    # OPTIMIZE on a liquid-clustered table also (re)clusters; on the partitioned fact it
    # compacts within partitions. No ZORDER -- liquid clustering supersedes it.
    spark.sql(f"OPTIMIZE {table}")
    spark.sql(f"VACUUM {table} RETAIN {VACUUM_RETAIN_HOURS} HOURS")


def run(spark: SparkSession, environment: str) -> dict[str, int]:
    env = resolve_env(environment)
    maintained = 0
    skipped = 0

    for schema, name in CLUSTERED_INLINE:
        table = env.table(schema, name)
        if not spark.catalog.tableExists(table):
            log_event(LOGGER, "table_maintenance.skip", table=table, reason="does_not_exist")
            skipped += 1
            continue
        _optimize_and_vacuum(spark, table)
        log_event(LOGGER, "table_maintenance.optimized", table=table, clustering="inline")
        maintained += 1

    for (schema, name), columns in NEEDS_CLUSTERING.items():
        table = env.table(schema, name)
        if not spark.catalog.tableExists(table):
            log_event(LOGGER, "table_maintenance.skip", table=table, reason="does_not_exist")
            skipped += 1
            continue
        _set_clustering(spark, table, columns)
        _set_properties(spark, table)
        _optimize_and_vacuum(spark, table)
        log_event(
            LOGGER,
            "table_maintenance.clustered_and_optimized",
            table=table,
            clustering=", ".join(columns),
        )
        maintained += 1

    log_event(LOGGER, "table_maintenance.complete", maintained=maintained, skipped=skipped)
    return {"maintained": maintained, "skipped": skipped}


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    run(spark_session, env_name)
