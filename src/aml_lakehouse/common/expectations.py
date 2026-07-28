"""Generic, parameterized data-contract checks -- dbt-style generic tests, translated to
this repo's own idiom: pure SQL-string builders (unit-testable without Spark, same as
`sql_runner.py`) plus a thin runner that executes them and raises through the existing
`DataContractError` from `risk_guardrails.py`.

Each check names the exact rule from `docs/03_schema_contracts.md` it enforces (not_null,
unique, accepted_values, range, relationship) so a table's full contract can be declared as
one list of `ContractCheck` next to the build script that creates it, instead of only living
in prose.
"""
from __future__ import annotations

from dataclasses import dataclass

from aml_lakehouse.common.risk_guardrails import DataContractError


@dataclass(frozen=True)
class ContractCheck:
    """One declarative data-contract rule for a single column.

    Args:
        kind (str): Rule name -- one of "not_null", "unique", "accepted_values", "range",
            "relationship".
        column (str): Column the rule applies to.
        values (list[str] | None): Allowed values, for "accepted_values".
        min_value (float | None): Inclusive lower bound, for "range".
        max_value (float | None): Inclusive upper bound, for "range".
        ref_table (str | None): Fully-qualified parent table, for "relationship".
        ref_column (str | None): Parent column `column` must exist in, for "relationship".
        where (str | None): SQL boolean expression restricting which rows the rule applies
            to (e.g. an OFAC `-0-` sentinel row that is legitimately exempt) -- mirrors
            dbt's per-test `where` config.
    """

    kind: str
    column: str
    values: list[str] | None = None
    min_value: float | None = None
    max_value: float | None = None
    ref_table: str | None = None
    ref_column: str | None = None
    where: str | None = None


def not_null(column: str, where: str | None = None) -> ContractCheck:
    """Build a not_null contract check."""
    return ContractCheck(kind="not_null", column=column, where=where)


def unique(column: str) -> ContractCheck:
    """Build a unique contract check."""
    return ContractCheck(kind="unique", column=column)


def accepted_values(column: str, values: list[str], where: str | None = None) -> ContractCheck:
    """Build an accepted_values contract check."""
    return ContractCheck(kind="accepted_values", column=column, values=values, where=where)


def range_check(
    column: str, min_value: float, max_value: float, where: str | None = None
) -> ContractCheck:
    """Build an inclusive-range contract check."""
    return ContractCheck(
        kind="range", column=column, min_value=min_value, max_value=max_value, where=where
    )


def relationship(
    column: str, ref_table: str, ref_column: str, where: str | None = None
) -> ContractCheck:
    """Build a foreign-key-style relationship contract check."""
    return ContractCheck(
        kind="relationship",
        column=column,
        ref_table=ref_table,
        ref_column=ref_column,
        where=where,
    )


def _and_where(clause: str, where: str | None) -> str:
    return f"({clause}) AND ({where})" if where else clause


def build_check_sql(table: str, check: ContractCheck) -> str:
    """Compile a `ContractCheck` into a `SELECT COUNT(*) AS violations ...` query.

    Args:
        table (str): Fully-qualified table the check runs against.
        check (ContractCheck): The rule to compile.

    Returns:
        str: A query returning a single `violations` column -- zero means the rule holds.

    Raises:
        ValueError: If `check.kind` is not a recognized rule name.
    """
    column = check.column

    if check.kind == "not_null":
        clause = _and_where(f"{column} IS NULL", check.where)
        return f"SELECT COUNT(*) AS violations FROM {table} WHERE {clause}"

    if check.kind == "unique":
        return (
            f"SELECT COUNT(*) AS violations FROM ("
            f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL "
            f"GROUP BY {column} HAVING COUNT(*) > 1) dup"
        )

    if check.kind == "accepted_values":
        quoted = ", ".join(f"'{value}'" for value in check.values or [])
        clause = _and_where(f"{column} IS NOT NULL AND {column} NOT IN ({quoted})", check.where)
        return f"SELECT COUNT(*) AS violations FROM {table} WHERE {clause}"

    if check.kind == "range":
        clause = _and_where(
            f"{column} IS NOT NULL AND ({column} < {check.min_value} OR {column} > {check.max_value})",
            check.where,
        )
        return f"SELECT COUNT(*) AS violations FROM {table} WHERE {clause}"

    if check.kind == "relationship":
        clause = _and_where(
            f"c.{column} IS NOT NULL AND NOT EXISTS ("
            f"SELECT 1 FROM {check.ref_table} p WHERE p.{check.ref_column} = c.{column})",
            check.where,
        )
        return f"SELECT COUNT(*) AS violations FROM {table} c WHERE {clause}"

    raise ValueError(f"Unknown contract check kind: {check.kind}")


def run_contract(spark, table: str, checks: list[ContractCheck], dataset: str | None = None) -> None:
    """Run every check against real data and fail loud on the first violation.

    Args:
        spark: A Spark (or duck-typed equivalent) session exposing `.sql(query).collect()`.
        table (str): Fully-qualified table the checks run against.
        checks (list[ContractCheck]): Rules declared for this table.
        dataset (str | None): Label for error messages; defaults to `table`.

    Raises:
        DataContractError: On the first rule with one or more violating rows.
    """
    dataset = dataset or table
    for check in checks:
        sql = build_check_sql(table, check)
        violations = spark.sql(sql).collect()[0]["violations"]
        if violations > 0:
            raise DataContractError(
                f"{dataset}.{check.column} failed '{check.kind}' contract: "
                f"{violations} violating row(s)"
            )
