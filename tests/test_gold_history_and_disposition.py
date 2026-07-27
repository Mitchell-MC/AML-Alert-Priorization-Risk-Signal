"""Locks the audit-history and reviewer-disposition additions to build_gold.sql.

Gold's entity_risk_profile and prioritized_alert_queue are CREATE OR REPLACE (liquid clustering
requires an inline CTAS, see docs/12_performance_and_layout.md), which means every rebuild used
to silently erase prior as_of_date scores and reset every alert's status back to 'new'. These
tests pin the fix -- append-only history tables plus a disposition table joined into the
queue -- the same way test_reference_config.py pins the table-driven scoring policy, without
executing Spark.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GOLD_SQL = (REPO_ROOT / "src" / "aml_lakehouse" / "gold" / "build_gold.sql").read_text()


def test_entity_risk_profile_history_is_append_only():
    assert "CREATE TABLE IF NOT EXISTS {catalog}.gold.entity_risk_profile_history" in GOLD_SQL
    assert "LIKE {catalog}.gold.entity_risk_profile" in GOLD_SQL
    assert "INSERT INTO {catalog}.gold.entity_risk_profile_history" in GOLD_SQL
    # never CREATE OR REPLACE on the history table -- that would defeat its purpose
    assert "CREATE OR REPLACE TABLE {catalog}.gold.entity_risk_profile_history" not in GOLD_SQL


def test_entity_risk_profile_history_insert_is_idempotent_per_as_of_date():
    assert "WHERE h.entity_id = p.entity_id AND h.as_of_date = p.as_of_date" in GOLD_SQL


def test_prioritized_alert_queue_history_is_append_only():
    assert "CREATE TABLE IF NOT EXISTS {catalog}.gold.prioritized_alert_queue_history" in GOLD_SQL
    assert "LIKE {catalog}.gold.prioritized_alert_queue" in GOLD_SQL
    assert "INSERT INTO {catalog}.gold.prioritized_alert_queue_history" in GOLD_SQL
    assert "CREATE OR REPLACE TABLE {catalog}.gold.prioritized_alert_queue_history" not in GOLD_SQL


def test_alert_disposition_table_is_created_if_not_exists_not_replaced():
    assert "CREATE TABLE IF NOT EXISTS {catalog}.gold.alert_disposition" in GOLD_SQL
    assert "CREATE OR REPLACE TABLE {catalog}.gold.alert_disposition" not in GOLD_SQL


def test_prioritized_alert_queue_status_comes_from_disposition_not_hardcoded():
    # the old hard-coded status literal is gone from the final SELECT
    assert "COALESCE(d.status, 'new') AS status" in GOLD_SQL
    assert "LEFT JOIN latest_disposition d ON a.alert_id = d.alert_id" in GOLD_SQL
    # latest-per-alert_id via ROW_NUMBER, not a raw table scan that could return duplicates
    assert "ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY reviewed_at DESC)" in GOLD_SQL
