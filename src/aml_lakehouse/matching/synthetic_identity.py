"""Synthetic account-holder identity generation for AMLSim accounts.

AMLSim's account graph has no names/PII (see dataset_inspection_notes.md) -- this module
fabricates them so the fuzzy-matching module in fuzzy_match.py has real candidates to score,
including a deliberately seeded subset of near-miss collisions against real OFAC/OpenSanctions
names so the confidence bands have true positives to find, not just true negatives.

Every identity produced here is synthetic. See docs/00_business_charter.md's assumptions
section for why this must be disclosed plainly rather than presented as real KYC data.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Sequence

FIRST_NAMES = (
    "James", "Maria", "Wei", "Fatima", "Carlos", "Aisha", "David", "Yuki", "Mohammed", "Elena",
    "Ahmed", "Sofia", "Chen", "Priya", "John", "Anna", "Kwame", "Lucia", "Ivan", "Ngozi",
    "Ricardo", "Mei", "Omar", "Isabella", "Sanjay", "Grace", "Diego", "Amara", "Viktor", "Noor",
    "Hassan", "Camila", "Kenji", "Zainab", "Miguel", "Olga", "Tariq", "Fatou", "Andrei", "Layla",
)
LAST_NAMES = (
    "Garcia", "Smith", "Zhang", "Khan", "Rodriguez", "Silva", "Kim", "Ali", "Johnson", "Petrov",
    "Nguyen", "Ibrahim", "Martinez", "Osei", "Chowdhury", "Rossi", "Kowalski", "Diallo", "Santos",
    "Lopez", "Abdullah", "Kumar", "Mensah", "Volkov", "Hernandez", "Suzuki", "Haidari", "Costa",
    "Brown", "Okoro", "Torres", "Ahmadi", "Wong", "Mwangi", "Ferreira", "Novak", "Sharma", "Diaz",
)
COMMON_SUFFIXES = ("", "", "", "JR", "SR", "II")  # weighted toward no suffix


@dataclass(frozen=True)
class SyntheticIdentity:
    account_id: str
    synthetic_name: str
    synthetic_country: str
    synthetic_dob: str
    is_seeded_collision: bool
    seeded_from_entity_id: str | None = None


def make_random_name(rng: random.Random) -> str:
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    suffix = rng.choice(COMMON_SUFFIXES)
    return f"{first} {last}" + (f" {suffix}" if suffix else "")


def make_random_dob(rng: random.Random, min_age: int = 18, max_age: int = 85) -> str:
    year = 2026 - rng.randint(min_age, max_age)
    month = rng.randint(1, 12)
    day = rng.randint(1, 28)  # avoid month-length edge cases, doesn't need to be exact
    return f"{year:04d}-{month:02d}-{day:02d}"


_NEAR_MISS_TRANSFORMS = (
    "add_middle_initial",
    "add_suffix",
    "swap_adjacent_chars",
    "drop_char",
    "reorder_tokens",
    "add_extra_whitespace",
)


def make_near_miss_variant(rng: random.Random, real_name: str) -> str:
    """Produce a plausible near-miss corruption of a real watchlist name.

    Picks one transform per call so the seeded collisions span a realistic range of
    strong-to-weak fuzzy matches rather than all being the same kind of typo.
    """
    transform = rng.choice(_NEAR_MISS_TRANSFORMS)
    name = real_name

    if transform == "add_middle_initial":
        tokens = name.split()
        if len(tokens) >= 2:
            initial = rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            tokens.insert(1, f"{initial}.")
            name = " ".join(tokens)
    elif transform == "add_suffix":
        name = f"{name} {rng.choice(('JR', 'SR', 'II', 'LLC'))}"
    elif transform == "swap_adjacent_chars":
        if len(name) >= 2:
            i = rng.randint(0, len(name) - 2)
            chars = list(name)
            chars[i], chars[i + 1] = chars[i + 1], chars[i]
            name = "".join(chars)
    elif transform == "drop_char":
        if len(name) >= 4:
            i = rng.randint(1, len(name) - 2)
            name = name[:i] + name[i + 1 :]
    elif transform == "reorder_tokens":
        tokens = name.split()
        if len(tokens) >= 2:
            rng.shuffle(tokens)
            name = " ".join(tokens)
    elif transform == "add_extra_whitespace":
        tokens = name.split()
        name = "  ".join(tokens)

    return name


def assign_synthetic_identities(
    account_ids: Sequence[str],
    watchlist_names: Sequence[tuple[str, str]],
    countries: Sequence[str] = ("US", "GB", "MX", "PH", "NG", "IN", "CU", "RU"),
    seed: int = 42,
    collision_rate: float = 0.02,
) -> list[SyntheticIdentity]:
    """Assign a synthetic identity to every account, seeding collision_rate of them as
    near-miss variants of a randomly chosen real watchlist entity.

    Deterministic given the same inputs and seed -- required for the plan's "deterministic
    unit tests for entity matching confidence behavior" verification gate.
    """
    rng = random.Random(seed)
    identities: list[SyntheticIdentity] = []

    for account_id in account_ids:
        country = rng.choice(countries)
        dob = make_random_dob(rng)

        if watchlist_names and rng.random() < collision_rate:
            entity_id, real_name = rng.choice(watchlist_names)
            synthetic_name = make_near_miss_variant(rng, real_name)
            identities.append(
                SyntheticIdentity(
                    account_id=account_id,
                    synthetic_name=synthetic_name,
                    synthetic_country=country,
                    synthetic_dob=dob,
                    is_seeded_collision=True,
                    seeded_from_entity_id=entity_id,
                )
            )
        else:
            identities.append(
                SyntheticIdentity(
                    account_id=account_id,
                    synthetic_name=make_random_name(rng),
                    synthetic_country=country,
                    synthetic_dob=dob,
                    is_seeded_collision=False,
                )
            )

    return identities
