"""Test fixtures are drawn from real OFAC SDN rows (ent_num 36, AEROCARIBBEAN AIRLINES / its
'AERO-CARIBBEAN' alias) recorded in dataset_inspection_notes.md, plus synthetic pairs
calibrated against actual rapidfuzz.fuzz.WRatio scores rather than guessed thresholds.
"""
from aml_lakehouse.matching.fuzzy_match import (
    NAME_SCORE_FLOOR,
    STRONG_THRESHOLD,
    WatchlistEntity,
    match_against_watchlist,
    match_one,
)

AEROCARIBBEAN = WatchlistEntity(
    entity_id="OFAC-36",
    primary_name="AEROCARIBBEAN AIRLINES",
    aliases=("AERO-CARIBBEAN",),
    country="CU",
    dob=None,
)


def test_exact_match_on_primary_name():
    result = match_one("AeroCaribbean Airlines", AEROCARIBBEAN)
    assert result is not None
    assert result.confidence_band == "exact"
    assert result.match_score == 100.0
    assert result.explanation.matched_on_field == "primary_name"


def test_exact_match_via_alias_scores_higher_than_primary_name():
    # "AERO CARIBBEAN" normalizes identically to the alias "AERO-CARIBBEAN" but is a much
    # looser match against the primary name "AEROCARIBBEAN AIRLINES" (~87 unboosted) -- the
    # matcher must pick the alias, not settle for the weaker primary-name score.
    result = match_one("Aero Caribbean", AEROCARIBBEAN)
    assert result is not None
    assert result.confidence_band == "exact"
    assert result.explanation.matched_on_field == "alias"
    assert result.explanation.matched_on_text == "AERO-CARIBBEAN"


def test_strong_match_minor_typo():
    result = match_one("Aerocarribean Airlines", AEROCARIBBEAN)  # extra 'r'
    assert result is not None
    assert result.confidence_band == "strong"
    assert result.match_score >= STRONG_THRESHOLD
    assert result.match_score < 100.0


WEAK_BASE_CANDIDATE = "Aerocaribbean Support Services"  # WRatio 86.67 vs the normalized
# alias ("AERO CARIBBEAN"), 70.57 vs the primary name -- verified against the real
# normalize_name() + rapidfuzz output before being hard-coded here, not guessed. 86.67 sits
# inside [NAME_SCORE_FLOOR, STRONG_THRESHOLD) with enough headroom that +3 (country) keeps it
# weak (89.67) while +3+5 (country+dob) crosses into strong (94.67).


def test_weak_match_partial_name_no_corroboration():
    result = match_one(WEAK_BASE_CANDIDATE, AEROCARIBBEAN)
    assert result is not None
    assert NAME_SCORE_FLOOR <= result.match_score < STRONG_THRESHOLD
    assert result.confidence_band == "weak"
    assert result.explanation.matched_on_field == "alias"


def test_below_floor_returns_none():
    result = match_one("Completely Unrelated Trading Co", AEROCARIBBEAN)
    assert result is None


def test_country_bonus_alone_insufficient_to_change_band():
    weak_no_bonus = match_one(WEAK_BASE_CANDIDATE, AEROCARIBBEAN)
    weak_with_country = match_one(WEAK_BASE_CANDIDATE, AEROCARIBBEAN, candidate_country="CU")
    assert weak_no_bonus.confidence_band == "weak"
    assert weak_with_country.explanation.country_match is True
    assert weak_with_country.match_score > weak_no_bonus.match_score
    # a +3 country bonus alone (86.67 -> 89.67) shouldn't cross the 90 strong threshold
    assert weak_with_country.confidence_band == "weak"


def test_country_and_dob_bonus_together_can_cross_into_strong():
    entity_with_dob = WatchlistEntity(
        entity_id="OFAC-36",
        primary_name="AEROCARIBBEAN AIRLINES",
        aliases=("AERO-CARIBBEAN",),
        country="CU",
        dob="01 Jan 1970",
    )
    boosted = match_one(
        WEAK_BASE_CANDIDATE,
        entity_with_dob,
        candidate_country="CU",
        candidate_dob="1970-01-01",
    )
    assert boosted is not None
    assert boosted.explanation.country_match is True
    assert boosted.explanation.dob_match is True
    # 86.67 base + 3 (country) + 5 (dob) = 94.67 -> crosses the 90 strong threshold
    assert boosted.confidence_band == "strong"


def test_country_mismatch_gets_no_bonus():
    result = match_one(WEAK_BASE_CANDIDATE, AEROCARIBBEAN, candidate_country="US")
    assert result.explanation.country_match is False
    assert result.explanation.bonus_applied == 0.0


def test_match_against_watchlist_orders_by_score_and_respects_top_n():
    watchlist = [
        WatchlistEntity(entity_id="A", primary_name="AEROCARIBBEAN AIRLINES"),
        WatchlistEntity(entity_id="B", primary_name="AEROCARRIBEAN AIRLINES"),
        WatchlistEntity(entity_id="C", primary_name="BANCO NACIONAL DE CUBA"),
    ]
    results = match_against_watchlist("AeroCaribbean Airlines", watchlist, top_n=2)
    assert len(results) == 2
    assert results[0].entity_id == "A"
    assert results[0].confidence_band == "exact"
    assert results[1].entity_id == "B"
    assert results[0].match_score >= results[1].match_score


def test_match_against_watchlist_excludes_below_floor_entities():
    watchlist = [
        WatchlistEntity(entity_id="A", primary_name="AEROCARIBBEAN AIRLINES"),
        WatchlistEntity(entity_id="C", primary_name="BANCO NACIONAL DE CUBA"),
    ]
    results = match_against_watchlist("AeroCaribbean Airlines", watchlist, top_n=5)
    assert {m.entity_id for m in results} == {"A"}
