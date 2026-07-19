"""Environment/catalog resolution shared by every Bronze/Silver/Gold job.

See docs/02_environment_and_branching.md — environment separation is done via Unity
Catalog catalogs (aml_dev / aml_test / aml_prod_sim) in a single Databricks Free Edition
workspace, not separate workspaces or deployment targets.
"""
from __future__ import annotations

from dataclasses import dataclass

_VALID_ENVIRONMENTS = ("dev", "test", "prod_sim")

_CATALOG_BY_ENV = {
    "dev": "aml_dev",
    "test": "aml_test",
    "prod_sim": "aml_prod_sim",
}


@dataclass(frozen=True)
class EnvConfig:
    environment: str
    catalog: str

    @property
    def bronze(self) -> str:
        return f"{self.catalog}.bronze"

    @property
    def silver(self) -> str:
        return f"{self.catalog}.silver"

    @property
    def gold(self) -> str:
        return f"{self.catalog}.gold"

    def table(self, layer: str, name: str) -> str:
        if layer not in ("bronze", "silver", "gold"):
            raise ValueError(f"unknown layer: {layer!r}, expected bronze/silver/gold")
        return f"{self.catalog}.{layer}.{name}"


def resolve_env(environment: str) -> EnvConfig:
    if environment not in _VALID_ENVIRONMENTS:
        raise ValueError(
            f"unknown environment {environment!r}, expected one of {_VALID_ENVIRONMENTS}"
        )
    return EnvConfig(environment=environment, catalog=_CATALOG_BY_ENV[environment])
