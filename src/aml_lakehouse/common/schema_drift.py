"""Schema snapshot and drift detection helpers."""
from __future__ import annotations

from hashlib import sha256
import json


def normalize_schema(fields: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(fields, key=lambda f: f["name"])


def schema_hash(fields: list[dict[str, str]]) -> str:
    normalized = normalize_schema(fields)
    payload = "|".join(f"{f['name']}:{f['type']}" for f in normalized)
    return sha256(payload.encode("utf-8")).hexdigest()


def diff_schema(
    previous_fields: list[dict[str, str]],
    current_fields: list[dict[str, str]],
) -> dict[str, object]:
    prev = {f["name"]: f["type"] for f in previous_fields}
    curr = {f["name"]: f["type"] for f in current_fields}

    removed = sorted(set(prev) - set(curr))
    added = sorted(set(curr) - set(prev))
    type_changed = sorted(
        name for name in (set(prev) & set(curr)) if prev[name] != curr[name]
    )
    breaking = bool(removed or type_changed)
    return {
        "added": added,
        "removed": removed,
        "type_changed": [{"name": n, "from": prev[n], "to": curr[n]} for n in type_changed],
        "is_breaking": breaking,
        "previous_hash": schema_hash(previous_fields),
        "current_hash": schema_hash(current_fields),
    }


def table_fields(spark, table_name: str) -> list[dict[str, str]]:
    schema = spark.table(table_name).schema
    return [{"name": f.name, "type": str(f.dataType)} for f in schema.fields]


def record_schema_snapshot(
    spark,
    tracking_table: str,
    pipeline_name: str,
    table_name: str,
    fields: list[dict[str, str]],
) -> dict[str, object]:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {tracking_table} (
          pipeline_name STRING,
          table_name STRING,
          schema_hash STRING,
          schema_json STRING,
          drift_json STRING,
          is_breaking BOOLEAN,
          observed_at TIMESTAMP
        ) USING DELTA
        """
    )

    current_json = json.dumps(normalize_schema(fields), sort_keys=True)
    previous_rows = spark.sql(
        f"""
        SELECT schema_json
        FROM {tracking_table}
        WHERE table_name = '{table_name}'
        ORDER BY observed_at DESC
        LIMIT 1
        """
    ).collect()

    if previous_rows:
        previous_fields = json.loads(previous_rows[0]["schema_json"])
        drift = diff_schema(previous_fields, fields)
    else:
        drift = {
            "added": [f["name"] for f in normalize_schema(fields)],
            "removed": [],
            "type_changed": [],
            "is_breaking": False,
            "previous_hash": None,
            "current_hash": schema_hash(fields),
        }

    spark.sql(
        f"""
        INSERT INTO {tracking_table}
        VALUES (
          '{pipeline_name}',
          '{table_name}',
          '{drift['current_hash']}',
          '{current_json.replace("'", "''")}',
          '{json.dumps(drift, sort_keys=True).replace("'", "''")}',
          {str(drift['is_breaking']).lower()},
          current_timestamp()
        )
        """
    )
    return drift
