"""Structured logging helpers for incident-friendly telemetry."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def log_event(logger: logging.Logger, event_name: str, **payload) -> None:
    envelope = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "event": event_name,
        **payload,
    }
    logger.info(json.dumps(envelope, default=str, sort_keys=True))