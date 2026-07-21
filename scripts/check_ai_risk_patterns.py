"""Fail CI when risky coding patterns likely to cause silent failures are introduced."""
from __future__ import annotations

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIRS = (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts")


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for source_dir in SOURCE_DIRS:
        files.extend(source_dir.rglob("*.py"))
    return files


def _is_broad_exception(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return True
    return isinstance(handler.type, ast.Name) and handler.type.id == "Exception"


def _has_pass_or_silent_body(handler: ast.ExceptHandler) -> bool:
    if not handler.body:
        return True
    if all(isinstance(node, ast.Pass) for node in handler.body):
        return True
    if all(isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) for node in handler.body):
        return True
    return False


def _scan_file(path: Path) -> list[str]:
    issues: list[str] = []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        for handler in node.handlers:
            if _is_broad_exception(handler):
                issues.append(
                    f"{path.relative_to(PROJECT_ROOT)}:{handler.lineno} broad except detected; catch specific errors only"
                )
            if _has_pass_or_silent_body(handler):
                issues.append(
                    f"{path.relative_to(PROJECT_ROOT)}:{handler.lineno} silent except body detected; fail loud or log+raise"
                )
    return issues


def main() -> int:
    issues: list[str] = []
    for path in _iter_python_files():
        issues.extend(_scan_file(path))

    if issues:
        print("AI/code safety audit failed:")
        for issue in issues:
            print(f" - {issue}")
        return 1

    print("AI/code safety audit passed: no broad/silent exception handlers found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())