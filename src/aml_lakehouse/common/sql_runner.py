"""Executes a multi-statement .sql file against a Spark/Databricks SQL session.

Splits on a statement-terminating ';' followed by a blank line -- simple and correct for
this repo's own hand-authored .sql files (each CREATE OR REPLACE TABLE statement is followed
by a blank line before the next one, and none of them have a semicolon inside a string
literal). Not a general-purpose SQL parser -- don't reuse this on arbitrary SQL files.
"""
from __future__ import annotations

import re
from pathlib import Path

_STATEMENT_SPLIT = re.compile(r";\s*\n\s*\n")


def load_statements(sql_path: str, **substitutions: str) -> list[str]:
    text = Path(sql_path).read_text()
    for key, value in substitutions.items():
        text = text.replace(f"{{{key}}}", value)
    statements = _STATEMENT_SPLIT.split(text)
    return [s.strip().rstrip(";").strip() for s in statements if s.strip()]


def run_sql_script(spark, sql_path: str, **substitutions: str) -> None:
    for statement in load_statements(sql_path, **substitutions):
        spark.sql(statement)
