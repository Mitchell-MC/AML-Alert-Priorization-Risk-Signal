"""Synthetic event-time anchor for AMLSim's simulation-tick `time` column.

AMLSim's `time` is an integer simulation step, not a real timestamp (see
dataset_inspection_notes.md and docs/03_schema_contracts.md's open items). Both the
streaming ingestion job and Silver transaction modeling need to agree on the same
step-to-timestamp mapping, so it's centralized here rather than duplicated.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

SYNTHETIC_TIME_ANCHOR = datetime(2026, 1, 1, tzinfo=timezone.utc)
STEP_INTERVAL = timedelta(hours=1)


def step_to_event_time(step: int) -> datetime:
    return SYNTHETIC_TIME_ANCHOR + step * STEP_INTERVAL


def step_to_epoch_seconds(step: int) -> int:
    """The same mapping as step_to_event_time(), expressed as a UTC epoch-seconds integer.
    Single source of truth shared by the Python scalar above and the Spark SQL expression
    below, so they can never drift apart."""
    return int(SYNTHETIC_TIME_ANCHOR.timestamp()) + step * int(STEP_INTERVAL.total_seconds())


def step_to_event_time_expr(step_col_expr: str) -> str:
    """Spark SQL expression (built-in functions only -- no Python UDF) that maps an AMLSim
    step-index column to event_time, row-for-row identical to step_to_event_time().

    `timestamp_seconds(n)` is Spark's built-in "instant from UTC epoch seconds", the exact
    inverse of the epoch arithmetic in step_to_epoch_seconds(), so this stays timezone-safe
    regardless of session timezone. Replaces the previous `@F.udf` scalar, keeping the work
    in the engine (Photon/Catalyst) instead of paying a per-row Python serialization round
    trip -- see docs/12_performance_and_layout.md on the built-in > SQL > UDF hierarchy.

    `step_col_expr` is any SQL expression yielding the integer step (e.g. a try_cast); a null
    step yields a null timestamp, matching the previous UDF's None handling.
    """
    anchor_epoch = int(SYNTHETIC_TIME_ANCHOR.timestamp())
    step_seconds = int(STEP_INTERVAL.total_seconds())
    return f"timestamp_seconds({anchor_epoch} + ({step_col_expr}) * {step_seconds})"
