import pytest

from aml_lakehouse.common.risk_guardrails import (
    DataContractError,
    require_columns,
    require_invalid_ratio_below,
    require_non_empty,
)


def test_require_columns_raises_on_missing():
    with pytest.raises(DataContractError):
        require_columns(
            actual_columns=["a", "b"],
            required_columns=["a", "b", "c"],
            dataset="sample.dataset",
        )


def test_require_non_empty_raises_for_zero_rows():
    with pytest.raises(DataContractError):
        require_non_empty(0, "sample.dataset")


def test_require_invalid_ratio_below_raises_above_threshold():
    with pytest.raises(DataContractError):
        require_invalid_ratio_below(
            valid_count=80,
            invalid_count=20,
            max_invalid_ratio=0.10,
            dataset="bronze.txn_events stream",
        )


def test_require_invalid_ratio_below_returns_ratio_below_threshold():
    ratio = require_invalid_ratio_below(
        valid_count=95,
        invalid_count=5,
        max_invalid_ratio=0.10,
        dataset="bronze.txn_events stream",
    )
    assert ratio == 0.05