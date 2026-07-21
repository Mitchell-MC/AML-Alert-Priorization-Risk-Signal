"""Synthetic bridge from Elliptic's graph-topology risk to a subset of AMLSim accounts.

See docs/03_schema_contracts.md's "Elliptic integration caveat": Elliptic (Bitcoin
transactions) and AMLSim (simulated remittance accounts) describe unrelated underlying
activity and share no real join key. This assigns network_risk_exposure to a BRIDGE_RATE
fraction of accounts, sampled from Elliptic's real illicit_neighbor_ratio distribution --
deterministic given the same seed, same category of assumption as
matching/synthetic_identity.py's near-miss collision seeding, and disclosed the same way in
docs/00_business_charter.md and the README.
"""
from __future__ import annotations

import random
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import new_batch_id
from aml_lakehouse.common.ops_control import record_batch
from aml_lakehouse.common.risk_guardrails import DataContractError, require_non_empty
from aml_lakehouse.common.schema_drift import record_schema_snapshot, table_fields
from aml_lakehouse.common.structured_logging import get_logger, log_event
from aml_lakehouse.common.upstream_registry import get_dependency_metadata

NETWORK_RISK_SCHEMA = StructType(
    [
        StructField("account_id", StringType()),
        StructField("network_risk_exposure", DoubleType()),
        StructField("linkage_note", StringType()),
    ]
)

LINKAGE_NOTE = (
    "Simulated bridge: network_risk_exposure is sampled from the Elliptic Bitcoin dataset's "
    "real illicit-neighbor-ratio distribution and assigned to this account for demonstration "
    "purposes only. Elliptic transactions and this AMLSim account describe unrelated "
    "underlying activity -- there is no real join key between them."
)

BRIDGE_RATE = 0.05
LOGGER = get_logger(__name__)


def run(spark: SparkSession, environment: str, seed: int = 123) -> int:
    started = perf_counter()
    env = resolve_env(environment)
    dependency_meta = get_dependency_metadata("build_network_risk_reference")

    ratios = [
        row["illicit_neighbor_ratio"]
        for row in spark.sql(
            f"SELECT illicit_neighbor_ratio FROM {env.table('silver', 'elliptic_txn_risk')} "
            "WHERE illicit_neighbor_ratio IS NOT NULL"
        ).collect()
    ]
    account_ids = [
        row["account_id"]
        for row in spark.sql(f"SELECT account_id FROM {env.table('silver', 'account')}").collect()
    ]

    require_non_empty(len(ratios), env.table("silver", "elliptic_txn_risk"))
    require_non_empty(len(account_ids), env.table("silver", "account"))

    drift = record_schema_snapshot(
        spark,
        env.table("gold", "ops_schema_drift"),
        pipeline_name="build_network_risk_reference",
        table_name=env.table("silver", "elliptic_txn_risk"),
        fields=table_fields(spark, env.table("silver", "elliptic_txn_risk")),
    )
    if drift["is_breaking"]:
        raise DataContractError(
            f"Breaking schema drift detected for {env.table('silver', 'elliptic_txn_risk')}: {drift}"
        )

    if not ratios or not account_ids:
        raise ValueError(
            "silver.elliptic_txn_risk and silver.account must both be populated before "
            "running this job -- got "
            f"{len(ratios)} known-ratio Elliptic rows and {len(account_ids)} accounts."
        )

    rng = random.Random(seed)
    records = [
        (account_id, float(rng.choice(ratios)), LINKAGE_NOTE)
        for account_id in account_ids
        if rng.random() < BRIDGE_RATE
    ]

    df = spark.createDataFrame(records, schema=NETWORK_RISK_SCHEMA)
    df.write.format("delta").mode("overwrite").saveAsTable(
        env.table("silver", "network_risk_reference")
    )

    batch_id = new_batch_id()
    record_batch(
        spark,
        env.table("gold", "ops_control"),
        pipeline_name="build_network_risk_reference",
        batch_id=batch_id,
        processed_row_count=len(records),
        expected_row_count=len(account_ids),
        lag_seconds=0.0,
        dead_letter_count=0,
        checkpoint_offset=None,
        business_process=dependency_meta["business_process"],
        impacted_table=dependency_meta["impacted_table"],
        last_good_batch_age_minutes=0.0,
        upstream_owner=dependency_meta["upstream_owner"],
        upstream_contact=dependency_meta["upstream_contact"],
        runtime_seconds=perf_counter() - started,
        estimated_cloud_cost_usd=round(len(records) * 0.000001, 6),
    )
    log_event(
        LOGGER,
        "build_network_risk_reference.completed",
        pipeline_name="build_network_risk_reference",
        batch_id=batch_id,
        ratios_seen=len(ratios),
        accounts_seen=len(account_ids),
        bridge_rate=BRIDGE_RATE,
        output_rows=len(records),
    )
    return len(records)


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    count = run(spark_session, env_name)
    print(f"silver.network_risk_reference: {count} rows")
