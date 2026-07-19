from aml_lakehouse.common.ops_control import (
    DEGRADED_THRESHOLD_SECONDS,
    FRESH_THRESHOLD_SECONDS,
    freshness_status,
)


def test_within_sla_is_fresh():
    assert freshness_status(0) == "fresh"
    assert freshness_status(FRESH_THRESHOLD_SECONDS) == "fresh"


def test_just_over_sla_is_stale():
    assert freshness_status(FRESH_THRESHOLD_SECONDS + 1) == "stale"
    assert freshness_status(DEGRADED_THRESHOLD_SECONDS) == "stale"


def test_far_over_sla_is_degraded():
    assert freshness_status(DEGRADED_THRESHOLD_SECONDS + 1) == "degraded"


def test_infinite_lag_is_degraded():
    assert freshness_status(float("inf")) == "degraded"
