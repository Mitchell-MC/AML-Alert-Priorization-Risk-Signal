"""Bulk matching for Silver-layer execution: candidates screened against the full watchlist
using first-character blocking + rapidfuzz.process.cdist per block, then the *same*
confidence-band policy as fuzzy_match.match_one() applied to each surviving result.

Why blocking: a naive full cdist of 20K AMLSim accounts against ~38K OFAC names+aliases
(766M pairs) was benchmarked directly against this repo's real watchlist size and did not
finish `fuzz.WRatio` scoring within 60 seconds -- confirmed by an actual timed run, not
assumed. Blocking candidates and watchlist entries into buckets by the first normalized
character of the full name and only scoring within matching buckets cuts the comparison
space by roughly the number of buckets (~26x), which is what makes this tractable at
portfolio scale.

Known, deliberate limitation: single-key first-character blocking misses a match if the
candidate's *first* character differs from the true name (e.g. a typo'd first letter, or a
transliteration variant starting with a different letter). This is the standard
precision/recall trade real entity-resolution blocking always makes -- a production system
would use multiple blocking keys (e.g. also block on last-token or a phonetic key like
Soundex/double metaphone) to catch first-letter variation; single-key blocking is the
documented v1 choice here to keep the SQL/Python simple while still being fast enough to run
end to end.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence

import numpy as np
from rapidfuzz import fuzz, process

from aml_lakehouse.matching.fuzzy_match import (
    COUNTRY_MATCH_BONUS,
    DOB_MATCH_BONUS,
    NAME_SCORE_FLOOR,
    STRONG_THRESHOLD,
    ConfidenceBand,
    WatchlistEntity,
)
from aml_lakehouse.matching.normalize import normalize_dob, normalize_name


@dataclass(frozen=True)
class BulkMatchResult:
    candidate_index: int
    entity_id: str
    entity_name: str
    match_score: float
    confidence_band: ConfidenceBand
    raw_name_score: float
    matched_on_text: str
    matched_on_field: str
    country_match: bool
    dob_match: bool


def _block_key(normalized_name: str) -> str:
    return normalized_name[0] if normalized_name else ""


def _flatten_watchlist(watchlist: Sequence[WatchlistEntity]):
    texts: list[str] = []
    entity_idx: list[int] = []
    fields: list[str] = []
    for i, entity in enumerate(watchlist):
        texts.append(normalize_name(entity.primary_name))
        entity_idx.append(i)
        fields.append("primary_name")
        for alias in entity.aliases:
            texts.append(normalize_name(alias))
            entity_idx.append(i)
            fields.append("alias")
    return texts, entity_idx, fields


def match_many_against_watchlist(
    candidates: Sequence[tuple[str, str | None, str | None]],  # (name, country, dob)
    watchlist: Sequence[WatchlistEntity],
) -> list[BulkMatchResult]:
    if not candidates or not watchlist:
        return []

    flat_texts, flat_entity_idx, flat_fields = _flatten_watchlist(watchlist)
    candidate_names_normalized = [normalize_name(c[0]) for c in candidates]

    # Bucket watchlist columns and candidate rows by first-character block key.
    watchlist_buckets: dict[str, list[int]] = defaultdict(list)
    for col_j, text in enumerate(flat_texts):
        watchlist_buckets[_block_key(text)].append(col_j)

    candidate_buckets: dict[str, list[int]] = defaultdict(list)
    for cand_i, name in enumerate(candidate_names_normalized):
        candidate_buckets[_block_key(name)].append(cand_i)

    results: list[BulkMatchResult] = []

    for block_key, cand_rows in candidate_buckets.items():
        watchlist_cols = watchlist_buckets.get(block_key)
        if not watchlist_cols or not block_key:
            continue

        block_candidate_names = [candidate_names_normalized[i] for i in cand_rows]
        block_texts = [flat_texts[j] for j in watchlist_cols]
        score_matrix = process.cdist(
            block_candidate_names, block_texts, scorer=fuzz.WRatio, dtype=np.float32
        )

        for local_i, cand_i in enumerate(cand_rows):
            row = score_matrix[local_i]
            best_per_entity: dict[int, tuple[float, int]] = {}
            for local_j, score in enumerate(row):
                col_j = watchlist_cols[local_j]
                entity_idx = flat_entity_idx[col_j]
                current_best = best_per_entity.get(entity_idx)
                if current_best is None or score > current_best[0]:
                    best_per_entity[entity_idx] = (float(score), col_j)

            candidate_name, candidate_country, candidate_dob = candidates[cand_i]
            candidate_name_normalized = candidate_names_normalized[cand_i]
            candidate_dob_norm = normalize_dob(candidate_dob)

            for entity_idx, (raw_score, col_j) in best_per_entity.items():
                if raw_score < NAME_SCORE_FLOOR:
                    continue

                entity = watchlist[entity_idx]
                matched_text_normalized = flat_texts[col_j]
                is_exact = (
                    matched_text_normalized == candidate_name_normalized
                    and matched_text_normalized != ""
                )

                country_match = bool(
                    candidate_country
                    and entity.country
                    and candidate_country.strip().upper() == entity.country.strip().upper()
                )
                entity_dob_norm = normalize_dob(entity.dob)
                dob_match = bool(
                    candidate_dob_norm
                    and entity_dob_norm
                    and candidate_dob_norm == entity_dob_norm
                )

                bonus = (COUNTRY_MATCH_BONUS if country_match else 0.0) + (
                    DOB_MATCH_BONUS if dob_match else 0.0
                )
                final_score = min(100.0, raw_score + bonus)
                band: ConfidenceBand = (
                    "exact"
                    if is_exact
                    else ("strong" if final_score >= STRONG_THRESHOLD else "weak")
                )

                results.append(
                    BulkMatchResult(
                        candidate_index=cand_i,
                        entity_id=entity.entity_id,
                        entity_name=entity.primary_name,
                        match_score=round(final_score, 2),
                        confidence_band=band,
                        raw_name_score=round(raw_score, 2),
                        matched_on_text=flat_texts[col_j],
                        matched_on_field=flat_fields[col_j],
                        country_match=country_match,
                        dob_match=dob_match,
                    )
                )

    return results
