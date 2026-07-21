"""Replay valid rows from bronze.txn_events_dead_letter back into bronze.txn_events."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from aml_lakehouse.common.config import resolve_env


def run(spark: SparkSession, environment: str, limit: int = 10000) -> int:
    env = resolve_env(environment)
    dead_letter = env.table("bronze", "txn_events_dead_letter")
    target = env.table("bronze", "txn_events")

    df = spark.sql(
        f"""
        SELECT
          try_cast(sourceNodeId AS INT) AS sourceNodeId,
          try_cast(targetNodeId AS INT) AS targetNodeId,
          try_cast(value AS DOUBLE) AS value,
          try_cast(time AS INT) AS time,
          event_time,
          _ingested_at,
          _batch_id,
          _source_file,
          _schema_version
        FROM {dead_letter}
        WHERE try_cast(sourceNodeId AS INT) IS NOT NULL
          AND try_cast(targetNodeId AS INT) IS NOT NULL
          AND try_cast(value AS DOUBLE) IS NOT NULL
          AND try_cast(time AS INT) IS NOT NULL
        LIMIT {int(limit)}
        """
    )
    count = df.count()
    if count > 0:
        df.write.format("delta").mode("append").saveAsTable(target)
    return count


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    max_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    replayed = run(spark_session, env_name, max_rows)
    print(f"replayed_rows: {replayed}")