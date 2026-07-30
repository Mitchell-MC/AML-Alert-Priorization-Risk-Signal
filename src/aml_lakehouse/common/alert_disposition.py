"""Reviewer-disposition recorder for gold.prioritized_alert_queue (docs/03_schema_contracts.md).

gold.prioritized_alert_queue is CREATE OR REPLACE (see build_gold.sql), so a reviewer's
'reviewed'/'suppressed' call can't live as a plain column on it -- the next rebuild would reset
every alert back to 'new'. gold.alert_disposition is the durable side: insert-only, one row per
review action, never UPDATEd. build_gold.sql LEFT JOINs the latest disposition per alert_id back
onto the freshly-scored queue so a reviewer's call survives rebuilds and "who decided what and
when" is itself an audit trail.

The SQL builder is a pure function (no Spark) so it's unit-tested directly, same split as
common/scd2.py. Fail-loud by design -- no broad exception handling
(blocked by scripts/check_ai_risk_patterns.py).
"""
from __future__ import annotations

_VALID_STATUSES = ("new", "reviewed", "suppressed")


def record_disposition_sql(
    table: str,
    alert_id: str,
    status: str,
    reviewed_by: str,
    reviewed_at: str,
    note: str | None = None,
) -> str:
    """Build the INSERT that records one reviewer disposition for one alert.

    Args:
        table (str): fully-qualified gold.alert_disposition table name.
        alert_id (str): the alert being dispositioned.
        status (str): one of 'new', 'reviewed', 'suppressed'.
        reviewed_by (str): reviewer identity (e.g. email or principal name).
        reviewed_at (str): ISO timestamp the disposition takes effect.
        note (str | None): optional free-text rationale.

    Returns:
        str: the INSERT statement, ready to execute.

    Raises:
        ValueError: if status is not one of the valid dispositions.
    """
    if status not in _VALID_STATUSES:
        raise ValueError(f"unknown status {status!r}, expected one of {_VALID_STATUSES}")
    alert_id_sql = alert_id.replace("'", "''")
    reviewed_by_sql = reviewed_by.replace("'", "''")
    reviewed_at_sql = reviewed_at.replace("'", "''")
    note_sql = "NULL" if note is None else "'{}'".format(note.replace("'", "''"))
    return (
        f"INSERT INTO {table} (alert_id, status, reviewed_by, reviewed_at, note) "
        f"VALUES ('{alert_id_sql}', '{status}', '{reviewed_by_sql}', "
        f"TIMESTAMP'{reviewed_at_sql}', {note_sql})"
    )


def record_disposition(
    spark,
    table: str,
    alert_id: str,
    status: str,
    reviewed_by: str,
    reviewed_at: str,
    note: str | None = None,
) -> None:
    """Execute record_disposition_sql against Spark.

    Example -- a reviewer suppresses a false positive:
        record_disposition(
            spark, "aml_dev.gold.alert_disposition",
            alert_id="a1b2c3...",
            status="suppressed",
            reviewed_by="jane.doe@example.com",
            reviewed_at="2026-07-27T14:30:00",
            note="confirmed legitimate payroll batch",
        )
    The next Gold build's LEFT JOIN picks up status='suppressed' for this alert_id with no code
    change; the row filter in resources/ddl/01_ownership_and_access.sql then hides it from
    business/BI consumers per the ownership model.

    Args:
        spark: active SparkSession.
        table (str): fully-qualified gold.alert_disposition table name.
        alert_id (str): the alert being dispositioned.
        status (str): one of 'new', 'reviewed', 'suppressed'.
        reviewed_by (str): reviewer identity (e.g. email or principal name).
        reviewed_at (str): ISO timestamp the disposition takes effect.
        note (str | None): optional free-text rationale.

    Returns:
        None

    Raises:
        ValueError: if status is not one of the valid dispositions.
    """
    spark.sql(record_disposition_sql(table, alert_id, status, reviewed_by, reviewed_at, note))
