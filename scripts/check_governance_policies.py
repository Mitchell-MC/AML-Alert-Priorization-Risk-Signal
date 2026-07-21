"""Governance policy checks enforced in CI.

- blocks accidental secret material committed to repo
- blocks high-risk real-PII fields from entering SQL models
"""
from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCAN_GLOBS = ("src/**/*.py", "src/**/*.sql", "docs/**/*.md", "resources/**/*.sql")

SECRET_PATTERNS = (
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"(?i)aws_secret_access_key\s*[:=]\s*[\"'][A-Za-z0-9/+=]{20,}[\"']"),
    re.compile(r"(?i)databricks_token\s*[:=]\s*[\"'][A-Za-z0-9\-_]{20,}[\"']"),
    re.compile(r"-----BEGIN (RSA|OPENSSH|EC) PRIVATE KEY-----"),
)

BLOCKED_PII_FIELD_PATTERNS = (
    re.compile(r"\bssn\b", re.IGNORECASE),
    re.compile(r"\bsocial_security\b", re.IGNORECASE),
    re.compile(r"\bcredit_card\b", re.IGNORECASE),
    re.compile(r"\bpassport_number\b", re.IGNORECASE),
)


def _iter_files() -> list[Path]:
    files: list[Path] = []
    for pattern in SCAN_GLOBS:
        files.extend(PROJECT_ROOT.glob(pattern))
    return files


def main() -> int:
    issues: list[str] = []
    for path in _iter_files():
        text = path.read_text(encoding="utf-8")
        rel = path.relative_to(PROJECT_ROOT)

        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                issues.append(f"{rel}: possible secret detected ({pattern.pattern})")

        if path.suffix.lower() == ".sql":
            for pattern in BLOCKED_PII_FIELD_PATTERNS:
                if pattern.search(text):
                    issues.append(f"{rel}: blocked PII field pattern detected ({pattern.pattern})")

    if issues:
        print("Governance policy checks failed:")
        for issue in issues:
            print(f" - {issue}")
        return 1

    print("Governance policy checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())