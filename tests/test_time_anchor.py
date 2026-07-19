from aml_lakehouse.common.time_anchor import SYNTHETIC_TIME_ANCHOR, STEP_INTERVAL, step_to_event_time


def test_step_zero_is_the_anchor():
    assert step_to_event_time(0) == SYNTHETIC_TIME_ANCHOR


def test_step_advances_by_the_interval():
    assert step_to_event_time(5) == SYNTHETIC_TIME_ANCHOR + 5 * STEP_INTERVAL


def test_step_ordering_is_monotonic():
    assert step_to_event_time(3) < step_to_event_time(4)
