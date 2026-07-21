from aml_lakehouse.common.ops_control import DEGRADED_THRESHOLD_SECONDS, freshness_status
from aml_lakehouse.common.risk_guardrails import is_permission_error


def test_late_data_classified_as_degraded_for_alerting():
    assert freshness_status(DEGRADED_THRESHOLD_SECONDS + 60) == "degraded"


def test_permission_loss_detection_works_for_common_errors():
    assert is_permission_error(PermissionError("access denied")) is True
    assert is_permission_error(RuntimeError("User does not have permission on table")) is True
    assert is_permission_error(RuntimeError("random transient issue")) is False
