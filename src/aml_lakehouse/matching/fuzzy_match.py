"""Fuzzy entity matching against a sanctions/PEP watchlist, with explainable confidence bands.

Scoring policy (v1 — see docs/03_schema_contracts.md "Open items" for where these constants
are declared as an intentional, tunable design choice rather than a black box):

1. Base signal is name similarity (rapidfuzz WRatio) between the candidate and every
   watchlist entity's primary name + aliases; the best-scoring name per entity wins.
2. A name score below NAME_SCORE_FLOOR is dropped entirely — corroborating country/DOB
   agreement cannot rescue a weak name match. This keeps the policy from "manufacturing"
   a match out of a coincidental country hit on a barely-similar name.
3. Above the floor, exact country and/or DOB agreement add a small, fixed, capped bonus.
   This is deliberately simple and additive (not a trained/weighted model) so every point of
   the final score is traceable to a specific, named reason — required by the audit/
   explainability NFR in docs/01_nonfunctional_requirements.md.
4. Confidence bands: "exact" (normalized candidate name is byte-identical to a watchlist
   name/alias), "strong" (boosted score >= STRONG_THRESHOLD), "weak" (boosted score >=
   NAME_SCORE_FLOOR). Bands below "weak" are not returned — matches the business charter's
   watchlist-match definition.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from rapidfuzz import fuzz

from aml_lakehouse.matching.normalize import normalize_dob, normalize_name

NAME_SCORE_FLOOR = 78.0
STRONG_THRESHOLD = 90.0
COUNTRY_MATCH_BONUS = 3.0
DOB_MATCH_BONUS = 5.0

ConfidenceBand = str  # "exact" | "strong" | "weak"


@dataclass(frozen=True)
class WatchlistEntity:
    """One entity to match against — a unified view of silver.entity + silver.entity_alias."""

    entity_id: str
    primary_name: str
    aliases: Sequence[str] = field(default_factory=tuple)
    country: str | None = None
    dob: str | None = None


@dataclass(frozen=True)
class MatchExplanation:
    raw_name_score: float
    matched_on_text: str
    matched_on_field: str  # "primary_name" | "alias"
    country_match: bool
    dob_match: bool
    bonus_applied: float


@dataclass(frozen=True)
class MatchCandidate:
    entity_id: str
    entity_name: str
    match_score: float
    confidence_band: ConfidenceBand
    explanation: MatchExplanation


def _best_name_score(candidate_normalized: str, target_raw: str) -> tuple[float, str]:
    target_normalized = normalize_name(target_raw)
    if not candidate_normalized or not target_normalized:
        return 0.0, target_normalized
    if candidate_normalized == target_normalized:
        return 100.0, target_normalized
    return fuzz.WRatio(candidate_normalized, target_normalized), target_normalized


def match_one(
    candidate_name: str,
    entity: WatchlistEntity,
    candidate_country: str | None = None,
    candidate_dob: str | None = None,
) -> MatchCandidate | None:
    """Score one candidate identity against one watchlist entity. None if below the floor."""
    candidate_normalized = normalize_name(candidate_name)

    best_score = -1.0
    best_text = entity.primary_name
    best_field = "primary_name"
    for field_name, text in (
        ("primary_name", entity.primary_name),
        *(("alias", alias) for alias in entity.aliases),
    ):
        score, _ = _best_name_score(candidate_normalized, text)
        if score > best_score:
            best_score, best_text, best_field = score, text, field_name

    if best_score < NAME_SCORE_FLOOR:
        return None

    is_exact_name = normalize_name(best_text) == candidate_normalized and candidate_normalized != ""

    country_match = bool(
        candidate_country and entity.country and candidate_country.strip().upper() == entity.country.strip().upper()
    )
    candidate_dob_norm = normalize_dob(candidate_dob)
    entity_dob_norm = normalize_dob(entity.dob)
    dob_match = bool(candidate_dob_norm and entity_dob_norm and candidate_dob_norm == entity_dob_norm)

    bonus = (COUNTRY_MATCH_BONUS if country_match else 0.0) + (DOB_MATCH_BONUS if dob_match else 0.0)
    final_score = min(100.0, best_score + bonus)

    if is_exact_name:
        band: ConfidenceBand = "exact"
    elif final_score >= STRONG_THRESHOLD:
        band = "strong"
    else:
        band = "weak"

    return MatchCandidate(
        entity_id=entity.entity_id,
        entity_name=entity.primary_name,
        match_score=round(final_score, 2),
        confidence_band=band,
        explanation=MatchExplanation(
            raw_name_score=round(best_score, 2),
            matched_on_text=best_text,
            matched_on_field=best_field,
            country_match=country_match,
            dob_match=dob_match,
            bonus_applied=bonus,
        ),
    )


def match_against_watchlist(
    candidate_name: str,
    watchlist: Sequence[WatchlistEntity],
    candidate_country: str | None = None,
    candidate_dob: str | None = None,
    top_n: int = 5,
) -> list[MatchCandidate]:
    """Score a candidate identity against an entire watchlist, best matches first.

    O(len(watchlist) * avg_aliases) — fine for the portfolio-scale watchlists here (tens of
    thousands of entities). At real production scale this same per-pair scoring function is
    what would run inside a blocked/partitioned Spark UDF rather than a flat Python loop.
    """
    results = [
        match_one(candidate_name, entity, candidate_country, candidate_dob)
        for entity in watchlist
    ]
    matches = [m for m in results if m is not None]
    matches.sort(key=lambda m: m.match_score, reverse=True)
    return matches[:top_n]
