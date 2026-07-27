import pytest

from aml_lakehouse.common.alert_disposition import record_disposition_sql


def test_record_disposition_builds_insert_with_note():
    sql = record_disposition_sql(
        "aml_dev.gold.alert_disposition",
        alert_id="abc123",
        status="suppressed",
        reviewed_by="jane.doe@example.com",
        reviewed_at="2026-07-27T14:30:00",
        note="confirmed legitimate payroll batch",
    )
    assert sql == (
        "INSERT INTO aml_dev.gold.alert_disposition (alert_id, status, reviewed_by, reviewed_at, note) "
        "VALUES ('abc123', 'suppressed', 'jane.doe@example.com', "
        "TIMESTAMP'2026-07-27T14:30:00', 'confirmed legitimate payroll batch')"
    )


def test_record_disposition_defaults_note_to_null():
    sql = record_disposition_sql(
        "aml_dev.gold.alert_disposition",
        alert_id="abc123",
        status="reviewed",
        reviewed_by="jane.doe@example.com",
        reviewed_at="2026-07-27T14:30:00",
    )
    assert sql.endswith("TIMESTAMP'2026-07-27T14:30:00', NULL)")


def test_record_disposition_rejects_unknown_status():
    with pytest.raises(ValueError, match="unknown status"):
        record_disposition_sql(
            "aml_dev.gold.alert_disposition",
            alert_id="abc123",
            status="escalated",
            reviewed_by="jane.doe@example.com",
            reviewed_at="2026-07-27T14:30:00",
        )
