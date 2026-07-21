"""Shared fail-loud guardrails for pipeline reliability.

These helpers intentionally raise explicit exceptions when quality or schema contracts are
broken so jobs fail fast instead of silently serving inaccurate data.
"""
from __future__ import annotations


class DataContractError(RuntimeError):
    """Raised when required data contracts are violated."""


def is_permission_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return isinstance(exc, PermissionError) or "permission" in text or "access denied" in text


def require_columns(actual_columns: list[str], required_columns: list[str], dataset: str) -> None:
    missing = sorted(set(required_columns) - set(actual_columns))
    if missing:
        raise DataContractError(
            f"{dataset} is missing required columns: {missing}. "
            f"Actual columns: {sorted(actual_columns)}"
        )


def require_non_empty(row_count: int, dataset: str) -> None:
    if row_count <= 0:
        raise DataContractError(f"{dataset} has no rows; refusing to run downstream logic")


def require_invalid_ratio_below(
    valid_count: int,
    invalid_count: int,
    max_invalid_ratio: float,
    dataset: str,
) -> float:
    total = valid_count + invalid_count
    if total <= 0:
        raise DataContractError(f"{dataset} batch is empty; cannot evaluate quality")

    invalid_ratio = invalid_count / total
    if invalid_ratio > max_invalid_ratio:
        raise DataContractError(
            f"{dataset} invalid ratio {invalid_ratio:.2%} exceeded limit {max_invalid_ratio:.2%}"
        )
    return invalid_ratio