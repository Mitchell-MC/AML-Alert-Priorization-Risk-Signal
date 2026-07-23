from collections import OrderedDict

from aml_lakehouse.common.scd2 import close_current_sql, insert_new_sql


def test_close_current_stamps_valid_to_and_clears_is_current():
    sql = close_current_sql("aml_dev.gold.scoring_rule", "rule_key = 'pts_burst'", "2026-07-22")
    assert sql == (
        "UPDATE aml_dev.gold.scoring_rule SET valid_to = DATE'2026-07-22', is_current = false "
        "WHERE (rule_key = 'pts_burst') AND is_current"
    )


def test_insert_new_opens_open_ended_current_version():
    cols = OrderedDict([("rule_key", "'pts_burst'"), ("rule_value", "25")])
    sql = insert_new_sql("aml_dev.gold.scoring_rule", cols, "2026-07-22")
    assert sql == (
        "INSERT INTO aml_dev.gold.scoring_rule (rule_key, rule_value, valid_from, valid_to, is_current) "
        "VALUES ('pts_burst', 25, DATE'2026-07-22', CAST(NULL AS DATE), true)"
    )
