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
