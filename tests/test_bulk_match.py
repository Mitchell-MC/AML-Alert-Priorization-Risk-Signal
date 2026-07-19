"""Cross-validates bulk_match.match_many_against_watchlist against the already-tested
fuzzy_match.match_one() -- both must agree on score/band/explanation for identical inputs,
since bulk_match exists purely as a faster implementation of the same policy, not a
different one.
"""
from aml_lakehouse.matching.bulk_match import match_many_against_watchlist
from aml_lakehouse.matching.fuzzy_match import WatchlistEntity, match_one

AEROCARIBBEAN = WatchlistEntity(
    entity_id="OFAC-36",
    primary_name="AEROCARIBBEAN AIRLINES",
    aliases=("AERO-CARIBBEAN",),
    country="CU",
    dob=None,
)
BANCO_CUBA = WatchlistEntity(
    entity_id="OFAC-306",
    primary_name="BANCO NACIONAL DE CUBA",
    aliases=("NATIONAL BANK OF CUBA",),
    country="CU",
    dob=None,
)
WATCHLIST = [AEROCARIBBEAN, BANCO_CUBA]

# Same fixture strings already verified against real rapidfuzz output in test_fuzzy_match.py
WEAK_BASE_CANDIDATE = "Aerocaribbean Support Services"


def test_bulk_exact_match_agrees_with_match_one():
    single = match_one("AeroCaribbean Airlines", AEROCARIBBEAN)
    bulk = match_many_against_watchlist([("AeroCaribbean Airlines", None, None)], WATCHLIST)
    bulk_result = next(r for r in bulk if r.entity_id == "OFAC-36")

    assert bulk_result.confidence_band == single.confidence_band == "exact"
    assert bulk_result.match_score == single.match_score == 100.0


def test_bulk_alias_match_agrees_with_match_one():
    single = match_one("Aero Caribbean", AEROCARIBBEAN)
    bulk = match_many_against_watchlist([("Aero Caribbean", None, None)], WATCHLIST)
    bulk_result = next(r for r in bulk if r.entity_id == "OFAC-36")

    assert bulk_result.confidence_band == single.confidence_band == "exact"
    assert bulk_result.matched_on_field == single.explanation.matched_on_field == "alias"


def test_bulk_weak_match_agrees_with_match_one():
    single = match_one(WEAK_BASE_CANDIDATE, AEROCARIBBEAN)
    bulk = match_many_against_watchlist([(WEAK_BASE_CANDIDATE, None, None)], WATCHLIST)
    bulk_result = next((r for r in bulk if r.entity_id == "OFAC-36"), None)

    assert bulk_result is not None
    assert bulk_result.confidence_band == single.confidence_band == "weak"
    assert abs(bulk_result.match_score - single.match_score) < 0.01


def test_bulk_country_and_dob_bonus_agrees_with_match_one():
    entity_with_dob = WatchlistEntity(
        entity_id="OFAC-36",
        primary_name="AEROCARIBBEAN AIRLINES",
        aliases=("AERO-CARIBBEAN",),
        country="CU",
        dob="01 Jan 1970",
    )
    single = match_one(
        WEAK_BASE_CANDIDATE, entity_with_dob, candidate_country="CU", candidate_dob="1970-01-01"
    )
    bulk = match_many_against_watchlist(
        [(WEAK_BASE_CANDIDATE, "CU", "1970-01-01")], [entity_with_dob]
    )
    bulk_result = bulk[0]

    assert bulk_result.confidence_band == single.confidence_band == "strong"
    assert bulk_result.match_score == single.match_score
    assert bulk_result.country_match == single.explanation.country_match is True
    assert bulk_result.dob_match == single.explanation.dob_match is True


def test_bulk_below_floor_excluded():
    bulk = match_many_against_watchlist(
        [("Completely Unrelated Trading Co", None, None)], WATCHLIST
    )
    assert bulk == []


def test_bulk_handles_multiple_candidates_independently():
    candidates = [
        ("AeroCaribbean Airlines", None, None),  # exact match to entity A
        ("National Bank of Cuba", None, None),  # exact match to entity B's alias
        ("Totally Unrelated Corp", None, None),  # no match
    ]
    bulk = match_many_against_watchlist(candidates, WATCHLIST)

    cand0_matches = [r for r in bulk if r.candidate_index == 0]
    cand1_matches = [r for r in bulk if r.candidate_index == 1]
    cand2_matches = [r for r in bulk if r.candidate_index == 2]

    assert {m.entity_id for m in cand0_matches} == {"OFAC-36"}
    assert {m.entity_id for m in cand1_matches} == {"OFAC-306"}
    assert cand2_matches == []


def test_empty_inputs_return_empty():
    assert match_many_against_watchlist([], WATCHLIST) == []
    assert match_many_against_watchlist([("Someone", None, None)], []) == []
