"""Silver: synthetic account identities + watchlist match candidates.

Two things happen here, in one job because they share the same driver-side collect step:

1. Assign a synthetic customer identity to every AMLSim account (matching/synthetic_identity.py),
   seeding ~2% as deliberate near-miss collisions against real OFAC names -- AMLSim's raw
   account graph has no names (see dataset_inspection_notes.md). Written to silver.account.
2. Screen every account's synthetic identity against the watchlist (matching/bulk_match.py)
   and persist every match at or above the weak-confidence floor to silver.match_candidate.

**Scope decision, made after benchmarking against real data**: matching runs against OFAC
only (19,169 entities), not the full unified watchlist (OFAC + OpenSanctions sanctions +
PEPs = 838,309 entities). A real timed benchmark against real OFAC names + the actual
synthetic identity generator measured ~8ms/candidate for 19,169 entities; scaling that
linearly-ish to the full 838K-entity watchlist would put a 20,000-account run at roughly two
hours on a single driver process -- not viable for a batch Silver job at this project's
scale. Screening 747K PEP entities against every account is a genuinely different engineering
problem (approximate nearest-neighbor search / a real search index, not a driver-side
rapidfuzz loop) that's out of scope here; OFAC alone is still the core "sanctions screening"
story this project is built to demonstrate. Documented, not silently narrowed.

Both steps collect their (bounded, tens-of-thousands-of-rows) inputs to the driver rather
than running as distributed Spark transformations -- deliberate: the actual logic
(rapidfuzz scoring, synthetic name generation) is plain Python with no natural Spark-native
parallel structure, and collecting is fine at this row count. A `mapInPandas` partition-level
parallelization is the natural next step if the account volume grew by an order of magnitude
or more.
"""
from __future__ import annotations

from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from aml_lakehouse.common.config import resolve_env
from aml_lakehouse.common.ingestion_metadata import new_batch_id
from aml_lakehouse.common.ops_control import record_batch
from aml_lakehouse.common.risk_guardrails import DataContractError, require_columns, require_non_empty
from aml_lakehouse.common.schema_drift import record_schema_snapshot, table_fields
from aml_lakehouse.common.structured_logging import get_logger, log_event
from aml_lakehouse.common.upstream_registry import get_dependency_metadata
from aml_lakehouse.matching.bulk_match import match_many_against_watchlist
from aml_lakehouse.matching.fuzzy_match import WatchlistEntity
from aml_lakehouse.matching.synthetic_identity import assign_synthetic_identities

LOGGER = get_logger(__name__)

ACCOUNT_SCHEMA = StructType(
    [
        StructField("account_id", StringType()),
        StructField("synthetic_holder_name", StringType()),
        StructField("synthetic_country", StringType()),
        StructField("synthetic_dob", StringType()),
        StructField("is_seeded_collision", BooleanType()),
        StructField("seeded_from_entity_id", StringType()),
        StructField("init_balance", DoubleType()),
    ]
)

MATCH_CANDIDATE_SCHEMA = StructType(
    [
        StructField("match_id", StringType()),
        StructField("account_id", StringType()),
        StructField("entity_id", StringType()),
        StructField("entity_name", StringType()),
        StructField("match_confidence_band", StringType()),
        StructField("match_score", DoubleType()),
        StructField("matched_on_field", StringType()),
        StructField("matched_on_text", StringType()),
        StructField("country_match", BooleanType()),
        StructField("dob_match", BooleanType()),
    ]
)

WATCHLIST_SOURCE_SYSTEM = "ofac"


def _load_watchlist(spark: SparkSession, catalog: str) -> list[WatchlistEntity]:
    rows = spark.sql(
        f"""
        SELECT e.entity_id, e.primary_name, e.countries, e.birth_date,
               collect_list(a.alias_name) AS aliases
        FROM {catalog}.silver.entity e
        LEFT JOIN {catalog}.silver.entity_alias a ON e.entity_id = a.entity_id
        WHERE e.source_system = '{WATCHLIST_SOURCE_SYSTEM}'
        GROUP BY e.entity_id, e.primary_name, e.countries, e.birth_date
        """
    ).collect()

    watchlist = []
    for row in rows:
        country = row["countries"][0] if row["countries"] else None
        watchlist.append(
            WatchlistEntity(
                entity_id=row["entity_id"],
                primary_name=row["primary_name"] or "",
                aliases=tuple(a for a in row["aliases"] if a),
                country=country,
                dob=row["birth_date"],
            )
        )
    return watchlist


def run(spark: SparkSession, environment: str) -> dict[str, int]:
    started = perf_counter()
    env = resolve_env(environment)
    dependency_meta = get_dependency_metadata("build_accounts_and_matches")

    accounts_df = spark.sql(
        f"""
        SELECT nodeid, init_balance FROM {env.table("bronze", "txn_accounts")}
        WHERE _batch_id = (SELECT MAX(_batch_id) FROM {env.table("bronze", "txn_accounts")})
        """
    )
    require_columns(
        actual_columns=accounts_df.columns,
        required_columns=["nodeid", "init_balance"],
        dataset=env.table("bronze", "txn_accounts"),
    )
    account_rows = accounts_df.collect()
    require_non_empty(len(account_rows), env.table("bronze", "txn_accounts"))

    drift = record_schema_snapshot(
        spark,
        env.table("gold", "ops_schema_drift"),
        pipeline_name="build_accounts_and_matches",
        table_name=env.table("bronze", "txn_accounts"),
        fields=table_fields(spark, env.table("bronze", "txn_accounts")),
    )
    if drift["is_breaking"]:
        raise DataContractError(
            f"Breaking schema drift detected for {env.table('bronze', 'txn_accounts')}: {drift}"
        )
    account_ids = [str(r["nodeid"]) for r in account_rows]
    balances = {str(r["nodeid"]): r["init_balance"] for r in account_rows}

    watchlist = _load_watchlist(spark, env.catalog)
    require_non_empty(len(watchlist), f"{env.catalog}.silver.entity watchlist")
    watchlist_names = [(w.entity_id, w.primary_name) for w in watchlist]

    identities = assign_synthetic_identities(
        account_ids, watchlist_names, seed=42, collision_rate=0.02
    )

    account_records = [
        (
            identity.account_id,
            identity.synthetic_name,
            identity.synthetic_country,
            identity.synthetic_dob,
            identity.is_seeded_collision,
            identity.seeded_from_entity_id,
            balances.get(identity.account_id),
        )
        for identity in identities
    ]
    accounts_out = spark.createDataFrame(account_records, schema=ACCOUNT_SCHEMA)
    accounts_out.write.format("delta").mode("overwrite").saveAsTable(
        env.table("silver", "account")
    )

    candidates = [
        (identity.synthetic_name, identity.synthetic_country, identity.synthetic_dob)
        for identity in identities
    ]
    match_results = match_many_against_watchlist(candidates, watchlist)

    batch_id = new_batch_id()
    match_records = [
        (
            f"{batch_id}-{i}",
            identities[m.candidate_index].account_id,
            m.entity_id,
            m.entity_name,
            m.confidence_band,
            m.match_score,
            m.matched_on_field,
            m.matched_on_text,
            m.country_match,
            m.dob_match,
        )
        for i, m in enumerate(match_results)
    ]
    matches_out = spark.createDataFrame(match_records, schema=MATCH_CANDIDATE_SCHEMA)
    matches_out.write.format("delta").mode("overwrite").saveAsTable(
        env.table("silver", "match_candidate")
    )

    batch_id = new_batch_id()
    record_batch(
        spark,
        env.table("gold", "ops_control"),
        pipeline_name="build_accounts_and_matches",
        batch_id=batch_id,
        processed_row_count=accounts_out.count() + matches_out.count(),
        expected_row_count=len(account_ids) + len(candidates),
        lag_seconds=0.0,
        dead_letter_count=0,
        checkpoint_offset=None,
        business_process=dependency_meta["business_process"],
        impacted_table=dependency_meta["impacted_table"],
        last_good_batch_age_minutes=0.0,
        upstream_owner=dependency_meta["upstream_owner"],
        upstream_contact=dependency_meta["upstream_contact"],
        runtime_seconds=perf_counter() - started,
        estimated_cloud_cost_usd=round((accounts_out.count() + matches_out.count()) * 0.000001, 6),
    )

    log_event(
        LOGGER,
        "build_accounts_and_matches.completed",
        pipeline_name="build_accounts_and_matches",
        batch_id=batch_id,
        accounts=len(account_ids),
        watchlist_entities=len(watchlist),
        matches=matches_out.count(),
        seeded_collisions=sum(1 for i in identities if i.is_seeded_collision),
    )

    return {
        "silver.account": accounts_out.count(),
        "silver.match_candidate": matches_out.count(),
        "seeded_collisions": sum(1 for i in identities if i.is_seeded_collision),
    }


if __name__ == "__main__":
    import sys

    spark_session = SparkSession.builder.getOrCreate()
    env_name = sys.argv[1] if len(sys.argv) > 1 else "dev"
    result = run(spark_session, env_name)
    for key, value in result.items():
        print(f"{key}: {value}")
