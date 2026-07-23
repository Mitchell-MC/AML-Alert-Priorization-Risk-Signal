"""Locks the table-driven config to the exact behavior it replaced.

Because the Gold scoring path can't be run in this (Spark-less) test env, these tests pin the
seeded SCD2 values to the literals the hard-coded CASE expressions used, and verify the SQL now
reads the config tables instead of hard-coding the policy. Together they prove the refactor is
behavior-preserving *and* actually table-driven, without executing Spark.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GOLD_SQL = (REPO_ROOT / "src" / "aml_lakehouse" / "gold" / "build_gold.sql").read_text()
ENTITIES_SQL = (REPO_ROOT / "src" / "aml_lakehouse" / "silver" / "build_entities.sql").read_text()


def test_scoring_rule_seed_matches_the_original_literals():
    # (rule_key, original hard-coded literal) -- every weight/threshold/band the CASE logic used
    expected = {
        "pts_watchlist_exact": 50,
        "pts_watchlist_strong": 30,
        "pts_watchlist_weak": 10,
        "pts_velocity": 10,
        "pts_diversity": 10,
        "pts_round_dollar": 10,
        "pts_burst": 15,
        "pts_risky_counterparty": 20,
        "pts_network_risk": 15,
        "thr_velocity_7d": 20,
        "thr_diversity_30d": 15,
        "thr_round_dollar_ratio": 0.5,
        "thr_network_risk": 0.5,
        "thr_burst_multiplier": 3,
        "score_cap": 100,
        "band_high_min": 70,
        "band_medium_min": 40,
        "alert_min_score": 40,
    }
    for key, value in expected.items():
        assert f"('{key}', {value}," in GOLD_SQL, f"seed for {key}={value} missing/changed"


def test_gold_scoring_is_table_driven_not_hardcoded():
    # reads the SCD2 table via a current-version filter
    assert "{catalog}.gold.scoring_rule" in GOLD_SQL
    assert "WHERE is_current" in GOLD_SQL
    assert "CROSS JOIN rules r" in GOLD_SQL
    # the old hard-coded scoring literals are gone from the scoring expressions
    assert "THEN 50 WHEN best_band_rank = 2 THEN 30" not in GOLD_SQL
    assert "WHEN velocity_7d > 20 THEN 10" not in GOLD_SQL
    assert "composite_risk_score >= 70 THEN 'high'" not in GOLD_SQL


def test_entity_type_map_seed_covers_every_original_case_branch():
    expected_rows = (
        "('ofac', 'individual', 'person'",
        "('ofac', 'vessel', 'vessel'",
        "('ofac', 'aircraft', 'aircraft'",
        "('ofac', '', 'organization'",
        "('opensanctions', 'Person', 'person'",
        "('opensanctions', 'Organization', 'organization'",
        "('opensanctions', 'Vessel', 'vessel'",
        "('opensanctions', 'Airplane', 'aircraft'",
    )
    for row in expected_rows:
        assert row in ENTITIES_SQL, f"entity_type_map seed row missing: {row}"


def test_entity_type_is_table_driven_not_hardcoded():
    assert "{catalog}.silver.entity_type_map" in ENTITIES_SQL
    assert "COALESCE(m.unified_value, 'other') AS entity_type" in ENTITIES_SQL
    # original CASE branches are gone
    assert "WHEN sdn_type = 'individual' THEN 'person'" not in ENTITIES_SQL
    assert "WHEN schema = 'Person' THEN 'person'" not in ENTITIES_SQL
