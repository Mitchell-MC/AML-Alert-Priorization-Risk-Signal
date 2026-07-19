"""Shared Bronze ingestion-metadata columns and batch-id generation.

Every Bronze table in docs/03_schema_contracts.md carries the same four metadata columns
(_ingested_at, _source_file/_source_offset, _schema_version, _batch_id). Centralizing the
column defs and batch-id format here means every ingestion job stamps batches the same way,
which is what lets gold.ops_control join across sources on batch_id.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone


def new_batch_id() -> str:
    """A sortable, unique batch id: <UTC timestamp><short random suffix>."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True)
class IngestionMetadata:
    batch_id: str
    ingested_at: datetime
    source_file: str
    schema_version: str

    @classmethod
    def create(cls, source_file: str, schema_version: str) -> "IngestionMetadata":
        return cls(
            batch_id=new_batch_id(),
            ingested_at=datetime.now(timezone.utc),
            source_file=source_file,
            schema_version=schema_version,
        )

    def as_columns(self) -> dict:
        """Literal values to attach to every row of a Bronze DataFrame for this batch."""
        return {
            "_batch_id": self.batch_id,
            "_ingested_at": self.ingested_at,
            "_source_file": self.source_file,
            "_schema_version": self.schema_version,
        }


# Bronze null-sentinel policy (docs/03_schema_contracts.md): OFAC's "-0-" is preserved as-is
# in Bronze for raw fidelity and converted to real NULL only at Bronze->Silver. Centralized
# here so Silver normalization always checks the same sentinel value.
OFAC_NULL_SENTINEL = "-0-"
