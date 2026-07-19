from rapidfuzz import fuzz

from aml_lakehouse.matching.normalize import normalize_name
from aml_lakehouse.matching.synthetic_identity import (
    FIRST_NAMES,
    LAST_NAMES,
    assign_synthetic_identities,
)

ACCOUNT_IDS = [f"ACC{i:04d}" for i in range(200)]
WATCHLIST = [
    ("OFAC-36", "AEROCARIBBEAN AIRLINES"),
    ("OFAC-173", "ANGLO-CARIBBEAN CO., LTD."),
]


def test_deterministic_given_same_seed():
    result_a = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    result_b = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    assert result_a == result_b


def test_different_seed_gives_different_output():
    result_a = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    result_b = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=99, collision_rate=0.02)
    assert result_a != result_b


def test_assigns_one_identity_per_account():
    result = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    assert [r.account_id for r in result] == ACCOUNT_IDS


def test_collision_rate_produces_expected_count_at_fixed_seed():
    # exact count verified by running the generator, not guessed -- 5 of 200 at seed=42,
    # collision_rate=0.02 (~2.5%, consistent with the target rate).
    result = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    collisions = [r for r in result if r.is_seeded_collision]
    assert len(collisions) == 5


def test_zero_collision_rate_seeds_nothing():
    result = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.0)
    assert all(not r.is_seeded_collision for r in result)
    assert all(r.seeded_from_entity_id is None for r in result)


def test_empty_watchlist_seeds_nothing_even_with_positive_rate():
    result = assign_synthetic_identities(ACCOUNT_IDS, [], seed=42, collision_rate=1.0)
    assert all(not r.is_seeded_collision for r in result)


def test_seeded_collisions_reference_a_real_watchlist_entity():
    result = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    watchlist_ids = {entity_id for entity_id, _ in WATCHLIST}
    for identity in result:
        if identity.is_seeded_collision:
            assert identity.seeded_from_entity_id in watchlist_ids


def test_seeded_collision_names_stay_close_to_the_source_name():
    watchlist_by_id = dict(WATCHLIST)
    result = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    for identity in result:
        if not identity.is_seeded_collision:
            continue
        source_name = watchlist_by_id[identity.seeded_from_entity_id]
        score = fuzz.WRatio(normalize_name(identity.synthetic_name), normalize_name(source_name))
        # near-miss transforms (typo, reorder, added suffix/initial) should stay recognizably
        # close to the original -- this is what makes them useful positives for the matcher.
        assert score >= 70, f"{identity.synthetic_name!r} strayed too far from {source_name!r} ({score})"


def test_non_collision_identities_use_the_synthetic_name_pool():
    result = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    for identity in result:
        if identity.is_seeded_collision:
            continue
        tokens = identity.synthetic_name.split()
        assert tokens[0] in FIRST_NAMES
        assert tokens[1] in LAST_NAMES


def test_dob_and_country_are_always_populated():
    result = assign_synthetic_identities(ACCOUNT_IDS, WATCHLIST, seed=42, collision_rate=0.02)
    for identity in result:
        assert identity.synthetic_country
        assert len(identity.synthetic_dob) == 10  # YYYY-MM-DD
