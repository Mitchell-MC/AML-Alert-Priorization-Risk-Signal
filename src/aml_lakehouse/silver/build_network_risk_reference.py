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

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from aml_lakehouse.common.config import resolve_env

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


def run(spark: SparkSession, environment: str, seed: int = 123) -> int:
    env = resolve_env(environment)

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
    return len(records)


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    count = run(spark_session, env_name)
    print(f"silver.network_risk_reference: {count} rows")
