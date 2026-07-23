from datetime import datetime, timezone

from aml_lakehouse.common.time_anchor import (
    SYNTHETIC_TIME_ANCHOR,
    STEP_INTERVAL,
    step_to_epoch_seconds,
    step_to_event_time,
    step_to_event_time_expr,
)


def test_step_zero_is_the_anchor():
    assert step_to_event_time(0) == SYNTHETIC_TIME_ANCHOR


def test_step_advances_by_the_interval():
    assert step_to_event_time(5) == SYNTHETIC_TIME_ANCHOR + 5 * STEP_INTERVAL


def test_step_ordering_is_monotonic():
    assert step_to_event_time(3) < step_to_event_time(4)


def test_epoch_seconds_reconstruct_the_scalar_mapping():
    # timestamp_seconds(n) in Spark == datetime.fromtimestamp(n, UTC); this proves the epoch
    # arithmetic embedded in the SQL expression yields exactly step_to_event_time().
    for step in (0, 1, 5, 24, 1000, 99999):
        via_epoch = datetime.fromtimestamp(step_to_epoch_seconds(step), tz=timezone.utc)
        assert via_epoch == step_to_event_time(step)


def test_expr_is_builtin_only_and_carries_the_step_expression():
    expr = step_to_event_time_expr("try_cast(time AS INT)")
    # built-in timestamp_seconds, no udf() call, and the caller's step expression is inlined
    assert expr == "timestamp_seconds(1767225600 + (try_cast(time AS INT)) * 3600)"
    assert "udf" not in expr.lower()
