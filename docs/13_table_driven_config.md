# Table-Driven Config (SCD2)

Two pieces of pipeline logic that used to be hard-coded now live in **SCD2 reference tables**:
the entity-type vocabulary mapping and the Gold scoring policy. Changing either is a *data*
change picked up on the next run — no code edit, PR, or redeploy — and every change keeps a
begin/end-dated history of what was in effect when.

## Why this, and why only here

Full table-driven *ingestion* (dynamic source unioning, per-field mapping tables) would be
over-engineering for this project's four fixed, well-understood public sources — recognising
that is part of the point. These two spots are different: they're **policy**, they change for
business reasons rather than schema reasons, and one of them (the scoring rules) directly
serves the project's own audit requirement in [docs/01](01_nonfunctional_requirements.md) —
"which weights were in effect at decision time." That's exactly what SCD2 begin/end dates
answer.

## The two tables

Both carry SCD2 columns `valid_from` / `valid_to` (NULL = open) / `is_current`, and the
pipeline reads `WHERE is_current`. Contracts in [docs/03](03_schema_contracts.md).

### `silver.entity_type_map`

Maps each source's native type value to the unified `entity_type` vocabulary. Replaces the
hard-coded `CASE` in `build_entities.sql`; `silver.entity` now `LEFT JOIN`s it and falls back
to `other` for any unmapped value — identical behavior to the old `ELSE 'other'`. A new source
value (say OpenSanctions adds `CryptoWallet`) becomes one table row instead of a code change.

### `gold.scoring_rule`

Key/value rows holding every weight, threshold, band cutoff, and the alert-inclusion cutoff.
`build_gold.sql` pivots the current rows into a single-row `rules` CTE and cross-joins it into
the scoring, so `composite_risk_score`, `score_band`, `top_contributing_factors`, and the alert
queue are all computed from the table. The v1 seed values equal the prior hard-coded literals
exactly (locked by `tests/test_reference_config.py`), so this refactor is behavior-preserving.

## Changing a rule (the whole point)

Use the SCD2 helper (`src/aml_lakehouse/common/scd2.py`) — it closes the current version and
opens a new one, preserving history:

```python
from collections import OrderedDict
from aml_lakehouse.common.scd2 import apply_scd2_change

apply_scd2_change(
    spark, "aml_dev.gold.scoring_rule",
    key_predicate="rule_key = 'pts_burst'",
    business_columns=OrderedDict([
        ("rule_key", "'pts_burst'"),
        ("rule_value", "25"),
        ("description", "'raised burst weight after Q3 tuning'"),
    ]),
    effective_date="2026-07-22",
)
```

After this, the next Gold build scores with `pts_burst = 25`. The prior 15-point version stays
queryable with its begin/end dates, so you can answer "why did this alert score differently
last month?" — the lineage the video calls out, and the audit trail docs/01 requires. No inline
`CREATE OR REPLACE` wipes it, because the tables are created `IF NOT EXISTS` and seeded only
when empty.

## Interview framing

- **What:** policy (scoring weights, type vocabulary) lives in SCD2 tables; transforms read
  the current version.
- **Why:** change speed (row edit vs. PR/redeploy) *and* auditability (begin/end-dated history
  = "what was in effect when"), which the compliance use case actually requires.
- **Trade-off (stated honestly):** setup + governance overhead up front, and one more join in
  the hot path; not applied to ingestion, where it would be over-engineering for fixed sources.
- **Pairs with** [resources/ops/day_over_day_variance.sql](../resources/ops/day_over_day_variance.sql):
  a rule change is a clean explanation for a day-over-day score shift.
