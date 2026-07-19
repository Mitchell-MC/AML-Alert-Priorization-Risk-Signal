# Dashboard Build Spec

Page-by-page visual spec for `AML_Dashboard.Report`'s three pages. All 19 visuals below have
already been hand-authored as PBIR JSON (see `bi/README.md` for what that does and doesn't
guarantee) — use this doc two ways depending on what you find when you open it in Desktop:

- **If the hand-authored visuals opened fine**: this is your checklist to confirm each one
  looks right and to add anything you'd still want (conditional formatting, reference lines,
  the optional QA callout).
- **If some or all visuals didn't survive contact with real Desktop**: this is the from-scratch
  rebuild spec. Every field/measure named here already exists in the semantic model — it's a
  drag-and-drop exercise, not new modeling work.

## Page 1 — Investigator Triage (`triage_page`)

The primary working page: what should an investigator look at first today.

**KPI card row (top)** — 4 card visuals, one measure each:
- `[Total Accounts Screened]`
- `[Alerts Prioritized for Review]`
- `[High-Risk Entities]`
- `[Investigator Queue Reduction %]`

**Table — Prioritized Queue** (main content, left ~65% of the page):
- Fields: `PrioritizedAlertQueue[priority_rank]`, `DimAccount[synthetic_holder_name]`,
  `DimAccount[synthetic_country]`, `EntityRiskProfile[watchlist_confidence_label]`,
  `PrioritizedAlertQueue[composite_risk_score]`, `PrioritizedAlertQueue[escalation_reason]`,
  `PrioritizedAlertQueue[status]`
- Sort by `priority_rank` ascending (rank 1 = highest priority, matches the SQL's
  `RANK() OVER (ORDER BY composite_risk_score DESC)`)
- Conditional formatting: background color on `composite_risk_score` (e.g. red >70, amber
  40-70, per `EntityRiskProfile[score_band]` if you'd rather bind color to that field
  directly)

**Bar chart — Score Band Distribution** (right side, upper):
- Axis: `EntityRiskProfile[score_band]`
- Value: count of `EntityRiskProfile[account_id]`
- This is the "how much noise did we cut" visual — pair mentally with the queue-reduction
  card above it

**Bar chart — Top Contributing Factors** (right side, lower):
- `EntityRiskProfile[top_contributing_factors]` is an `array<struct>` column in Delta and
  will need the same Power Query "Expand to New Rows" treatment as
  `WatchlistMatchSummary[jurisdictions]` (see `bi/README.md`) before it's usable as a
  flat field. Once expanded to one row per (account, factor): axis = `factor`, value = count
  of occurrences. Shows which risk factors are actually driving alerts in aggregate, not
  just per-account.

**Slicers**: `EntityRiskProfile[score_band]`, `PrioritizedAlertQueue[escalation_reason]`

## Page 2 — Sanctions & PEP Exposure (`exposure_page`)

Proves the watchlist-screening story independent of behavioral scoring.

**KPI card row**:
- `[Exact Match Alerts]`, `[Strong Match Alerts]`, `[Weak Match Alerts]`
- `Total Watchlist Entities Matched` — new quick measure or just
  `DISTINCTCOUNT(WatchlistMatchSummary[watchlist_entity_id])`

**Table — Matched Watchlist Entities**:
- Fields: `WatchlistMatchSummary[watchlist_entity_id]`,
  `WatchlistMatchSummary[matched_account_count]`,
  `WatchlistMatchSummary[highest_confidence_band]`, `WatchlistMatchSummary[risk_type]`
- Sort by `matched_account_count` descending — surfaces watchlist entities matching the most
  accounts, which in a real system is itself a signal worth a second look (a common
  boilerplate corporate name pattern matching dozens of accounts is a data-quality flag, not
  necessarily dozens of real hits — see `dataset_inspection_notes.md`'s note on OFAC's
  repeated "JOINT STOCK COMPANY..." naming pattern)

**Donut/pie — Confidence Band Mix**:
- Legend: `highest_confidence_band`
- Value: count

**Bar chart — Exposure by Risk Type**:
- Axis: `WatchlistMatchSummary[risk_type]` (person / organization / vessel / aircraft / other)
- Value: sum of `matched_account_count`

**QA validation callout** (optional, clearly labeled): a card or table filtered to
`DimAccount[is_seeded_collision] = TRUE` showing how many of the deliberately planted
near-miss test cases were actually recovered as matches — this is the number behind the
"91.5% recall" finding in the project's execution notes, good interview material, but label
it explicitly as a system-validation artifact, not real investigator data (see the comment
on `DimAccount[is_seeded_collision]`).

## Page 3 — Operational Health (`ops_health_page`)

The silent-failure/ops story — proves the pipeline is monitored, not just built.

**KPI card row**:
- `[Avg Ingestion Lag (Minutes)]` (freshness SLA target: 15 min, see
  `docs/01_nonfunctional_requirements.md`)
- `[Dead-Letter Rate]`
- `[Batches Stale or Degraded]`
- Count of `OpsControl[batch_id]` (total batches recorded)

**Line/area chart — Lag Over Time**:
- Axis: `OpsControl[recorded_at]`
- Value: `OpsControl[lag_seconds]`
- Add a constant reference line at 900 seconds (15 min) to visualize the SLA boundary

**Table — Recent Batches**:
- Fields: `pipeline_name`, `batch_id`, `processed_row_count`, `dead_letter_count`,
  `freshness_status`, `recorded_at`
- Sort by `recorded_at` descending
- Conditional formatting on `freshness_status` (green=fresh, amber=stale, red=degraded)

**Card — Dead-Letter Volume**: sum of `OpsControl[dead_letter_count]`, paired with a note
that every dead-lettered row is inspectable in `bronze.*_dead_letter` tables (e.g. the real
OFAC EOF-marker rows and the injected `NOT_A_NUMBER` transaction rows documented in
`dataset_inspection_notes.md`) — this is the "first place to look" artifact for the
silent-failure incident-response narrative.
