import pytest

from aml_lakehouse.common.expectations import (
    accepted_values,
    build_check_sql,
    not_null,
    range_check,
    relationship,
    run_contract,
    unique,
)
from aml_lakehouse.common.risk_guardrails import DataContractError


def test_not_null_sql_has_no_where_by_default():
    sql = build_check_sql("gold.prioritized_alert_queue", not_null("alert_id"))
    assert sql == (
        "SELECT COUNT(*) AS violations FROM gold.prioritized_alert_queue "
        "WHERE alert_id IS NULL"
    )


def test_not_null_sql_scopes_to_where_clause():
    sql = build_check_sql(
        "silver.account", not_null("account_id", where="account_id != 0")
    )
    assert "(account_id IS NULL) AND (account_id != 0)" in sql


def test_unique_sql_groups_and_counts_duplicates():
    sql = build_check_sql("gold.prioritized_alert_queue", unique("alert_id"))
    assert "GROUP BY alert_id HAVING COUNT(*) > 1" in sql


def test_accepted_values_sql_quotes_each_value():
    check = accepted_values("status", ["new", "reviewed", "suppressed"])
    sql = build_check_sql("gold.prioritized_alert_queue", check)
    assert "status NOT IN ('new', 'reviewed', 'suppressed')" in sql


def test_range_check_sql_flags_out_of_bounds_either_side():
    check = range_check("composite_risk_score", 0, 100)
    sql = build_check_sql("gold.entity_risk_profile", check)
    assert "composite_risk_score < 0 OR composite_risk_score > 100" in sql


def test_relationship_sql_anti_joins_to_parent():
    check = relationship("account_id", ref_table="silver.account", ref_column="account_id")
    sql = build_check_sql("gold.prioritized_alert_queue", check)
    assert "FROM gold.prioritized_alert_queue c" in sql
    assert "NOT EXISTS (SELECT 1 FROM silver.account p WHERE p.account_id = c.account_id)" in sql


def test_build_check_sql_rejects_unknown_kind():
    bad_check = not_null("x")
    object.__setattr__(bad_check, "kind", "bogus")
    with pytest.raises(ValueError):
        build_check_sql("t", bad_check)


class _FakeResult:
    def __init__(self, violations: int):
        self._violations = violations

    def collect(self):
        return [{"violations": self._violations}]


class _FakeSpark:
    def __init__(self, violations_by_sql_substring: dict[str, int]):
        self._violations_by_sql_substring = violations_by_sql_substring

    def sql(self, query: str):
        for substring, violations in self._violations_by_sql_substring.items():
            if substring in query:
                return _FakeResult(violations)
        return _FakeResult(0)


def test_run_contract_passes_when_no_violations():
    spark = _FakeSpark({})
    run_contract(spark, "gold.prioritized_alert_queue", [not_null("alert_id"), unique("alert_id")])


def test_run_contract_raises_with_dataset_column_and_kind_in_message():
    spark = _FakeSpark({"alert_id IS NULL": 3})
    with pytest.raises(DataContractError, match="alert_id.*not_null.*3 violating row"):
        run_contract(
            spark,
            "gold.prioritized_alert_queue",
            [not_null("alert_id")],
            dataset="gold.prioritized_alert_queue",
        )


def test_run_contract_stops_at_first_violation():
    spark = _FakeSpark({"NOT IN": 5})
    checks = [
        accepted_values("status", ["new", "reviewed", "suppressed"]),
        accepted_values("escalation_reason", ["sanctions_hard_override", "behavioral_threshold"]),
    ]
    with pytest.raises(DataContractError):
        run_contract(spark, "gold.prioritized_alert_queue", checks)
