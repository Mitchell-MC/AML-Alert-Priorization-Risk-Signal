from aml_lakehouse.common.schema_drift import diff_schema


def test_schema_drift_detects_added_column_as_non_breaking():
    previous = [{"name": "id", "type": "IntegerType()"}]
    current = [
        {"name": "id", "type": "IntegerType()"},
        {"name": "extra", "type": "StringType()"},
    ]
    diff = diff_schema(previous, current)
    assert diff["added"] == ["extra"]
    assert diff["is_breaking"] is False


def test_schema_drift_detects_removed_column_as_breaking():
    previous = [
        {"name": "id", "type": "IntegerType()"},
        {"name": "value", "type": "StringType()"},
    ]
    current = [{"name": "id", "type": "IntegerType()"}]
    diff = diff_schema(previous, current)
    assert diff["removed"] == ["value"]
    assert diff["is_breaking"] is True


def test_schema_drift_detects_type_change_as_breaking():
    previous = [{"name": "amount", "type": "DoubleType()"}]
    current = [{"name": "amount", "type": "StringType()"}]
    diff = diff_schema(previous, current)
    assert diff["type_changed"] == [
        {"name": "amount", "from": "DoubleType()", "to": "StringType()"}
    ]
    assert diff["is_breaking"] is True
