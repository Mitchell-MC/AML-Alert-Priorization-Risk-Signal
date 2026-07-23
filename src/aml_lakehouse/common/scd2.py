"""SCD2 helpers for the table-driven config tables (gold.scoring_rule, silver.entity_type_map).

The whole point of table-driven design (docs/13_table_driven_config.md) is that a policy change
is a *data* change, not a code change. This module is that data-change primitive: it closes the
currently-effective row (stamps `valid_to`, clears `is_current`) and opens a new one
(`valid_from` = the effective date, `valid_to` = NULL, `is_current` = true), preserving the full
begin/end-dated history the pipeline reads via `WHERE is_current`.

The SQL builders are pure functions (no Spark) so the SCD2 transition logic is unit-tested
directly. `apply_scd2_change` is the thin Spark executor. Fail-loud by design -- no broad
exception handling (blocked by scripts/check_ai_risk_patterns.py).

Values are passed as SQL literal fragments (e.g. "25", "'person'"). These tables are operated
by engineers applying reviewed policy changes, not fed arbitrary user input; keep it that way.
"""
from __future__ import annotations

from collections import OrderedDict

SCD2_COLUMNS = ("valid_from", "valid_to", "is_current")


def close_current_sql(table: str, key_predicate: str, effective_date: str) -> str:
    """Close the currently-effective version(s) matching key_predicate as of effective_date."""
    return (
        f"UPDATE {table} SET valid_to = DATE'{effective_date}', is_current = false "
        f"WHERE ({key_predicate}) AND is_current"
    )


def insert_new_sql(table: str, business_columns: "OrderedDict[str, str]", effective_date: str) -> str:
    """Open a new currently-effective version from effective_date with an open-ended valid_to."""
    columns = list(business_columns.keys()) + list(SCD2_COLUMNS)
    values = list(business_columns.values()) + [f"DATE'{effective_date}'", "CAST(NULL AS DATE)", "true"]
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"


def apply_scd2_change(
    spark,
    table: str,
    key_predicate: str,
    business_columns: "OrderedDict[str, str]",
    effective_date: str,
) -> None:
    """Close the current version and open a new one, in that order (Delta ACID per-statement).

    Example -- bump the burst weight from today:
        apply_scd2_change(
            spark, "aml_dev.gold.scoring_rule",
            key_predicate="rule_key = 'pts_burst'",
            business_columns=OrderedDict([
                ("rule_key", "'pts_burst'"),
                ("rule_value", "25"),
                ("description", "'raised burst weight after Q3 tuning'"),
            ]),
            effective_date="2026-07-22",
        )
    The next pipeline run picks up rule_value = 25 with no code change; the prior 15-point
    version remains queryable with its begin/end dates for "what was in effect when" audits.
    """
    spark.sql(close_current_sql(table, key_predicate, effective_date))
    spark.sql(insert_new_sql(table, business_columns, effective_date))
