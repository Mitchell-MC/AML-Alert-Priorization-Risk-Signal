"""HTTP download helper for landing raw source files into a Unity Catalog Volume.

Uses stdlib urllib rather than requests -- no extra dependency, and urllib's default
redirect handling is exactly what's needed for OFAC's download endpoint (it 302s to a
time-limited signed S3 URL, see memory reference notes / dataset_inspection_notes.md).
"""
from __future__ import annotations

import urllib.request
from pathlib import Path

_USER_AGENT = "aml-lakehouse-bronze-ingestion/0.1"


def download_to_path(url: str, dest_path: str, timeout_seconds: int = 60) -> int:
    """Download url to dest_path (a Unity Catalog Volume path or local path). Returns bytes written.

    Raises urllib.error.URLError / HTTPError on failure -- callers should let Bronze jobs
    fail loudly on a download error rather than silently ingesting nothing (see the freshness
    SLA in docs/01_nonfunctional_requirements.md: a missed refresh must be visible, not silent).
    """
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        data = response.read()
    Path(dest_path).write_bytes(data)
    return len(data)
