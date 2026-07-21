"""PR gate: block high-risk contract changes without schema/test updates."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CRITICAL_PATTERNS = (
    "src/aml_lakehouse/silver/",
    "src/aml_lakehouse/gold/",
    "src/aml_lakehouse/common/ops_control.py",
    "src/aml_lakehouse/common/schema_drift.py",
)
REQUIRED_COMPANION_FILES = {
    "docs/03_schema_contracts.md",
    "tests/test_sql_runner.py",
}


def _run(args: list[str]) -> str:
    return subprocess.check_output(args, cwd=PROJECT_ROOT, text=True).strip()


def _changed_files() -> set[str]:
    base_ref = os.getenv("GITHUB_BASE_REF")
    if base_ref:
        _run(["git", "fetch", "origin", base_ref])
        merge_base = _run(["git", "merge-base", f"origin/{base_ref}", "HEAD"])
        diff = _run(["git", "diff", "--name-only", f"{merge_base}...HEAD"])
        return {line for line in diff.splitlines() if line}

    diff = _run(["git", "diff", "--name-only", "HEAD~1..HEAD"])
    return {line for line in diff.splitlines() if line}


def main() -> int:
    changed = _changed_files()
    if not changed:
        print("Contract risk check: no changed files detected.")
        return 0

    touched_critical = any(
        any(path.startswith(pattern) for pattern in CRITICAL_PATTERNS) for path in changed
    )
    if not touched_critical:
        print("Contract risk check: no critical contract surfaces changed.")
        return 0

    if not any(path in changed for path in REQUIRED_COMPANION_FILES):
        print("Contract risk check failed: critical pipeline/contract files changed without companion updates.")
        print("Expected at least one of:")
        for path in sorted(REQUIRED_COMPANION_FILES):
            print(f" - {path}")
        print("Changed files:")
        for path in sorted(changed):
            print(f" - {path}")
        return 1

    print("Contract risk check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
