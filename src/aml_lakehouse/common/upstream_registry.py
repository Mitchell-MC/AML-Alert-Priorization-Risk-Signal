"""Load upstream dependency ownership metadata for alert routing."""
from __future__ import annotations

import json
from pathlib import Path


def _registry_path() -> Path:
    return Path(__file__).resolve().parents[3] / "resources" / "ops" / "upstream_dependencies.json"


def load_upstream_registry() -> dict[str, dict[str, str]]:
    path = _registry_path()
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def get_dependency_metadata(pipeline_name: str) -> dict[str, str]:
    return load_upstream_registry().get(
        pipeline_name,
        {
            "business_process": "unspecified",
            "impacted_table": "unspecified",
            "upstream_owner": "unknown",
            "upstream_contact": "unknown",
        },
    )