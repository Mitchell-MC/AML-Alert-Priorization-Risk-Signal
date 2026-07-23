# Failure Scenario Map

For every pipeline this project owns, the three questions a senior engineer must be able to
answer without tap-dancing: **what if the source schema changes, what if the dashboard gets
stale data, and what if the business asks why today's number changed.** Each answer is given
as *blast radius* — what breaks, who notices, how bad it gets, and how to recover — not "we
have alerts set up." Alerts are the smoke detector; this is the containment plan.

The mechanisms referenced here already exist in the repo; this map is the interview-ready
consolidation of them.

## Pipelines owned

| Pipeline | Layer | Trigger |
|---|---|---|
| Bronze batch ingestion (OFAC / OpenSanctions / AMLSim / Elliptic) | Bronze | Batch, on deploy/run |
| Transaction stream ingestion | Bronze | Structured Streaming (`availableNow`) |
| Silver builds (entities, accounts+matches, transactions, network-risk) | Silver | Batch, dependency-ordered |
| Gold build (profile, queue, summary, ops_control) | Gold | Batch, after Silver |
| Month-end critical path (gated publish) | Gold | Tagged/manual, reconciliation-gated |

---

## Q1 — What if the source schema changes?

**Detection.** Schema drift is caught, not discovered in a VP's dashboard: bronze ingestion
compares incoming schema against the registered contract
(`src/aml_lakehouse/common/schema_drift.py`), and breaking changes surface in
`gold.ops_schema_drift` / [resources/ops/schema_drift_breaking_changes.sql](../resources/ops/schema_drift_breaking_changes.sql).
Individual malformed rows are quarantined to `bronze.<table>_dead_letter` rather than
crashing the batch.

**Blast radius by pipeline.**

| Pipeline | What breaks | Who notices | How bad | Recovery |
|---|---|---|---|---|
| Bronze batch | New/renamed/dropped column vs contract | DE, via drift table | Contained — bad rows dead-lettered, batch completes | Fix mapping, reprocess from raw Volume (still staged) |
| Stream ingestion | Malformed event fields | DE, via dead-letter + circuit breaker | Contained until invalid ratio exceeds `MAX_INVALID_RATIO`, then micro-batch aborts | Fix parser, `replay_txn_dead_letter` job replays quarantined rows |
| Silver/Gold | Upstream contract violation (missing required column, empty source) | DE, via `DataContractError` | Job fails loud *before* writing corrupt data | Fix upstream, rerun (`CREATE OR REPLACE`, idempotent) |

**The point:** a schema change degrades to quarantine + a loud failure, never to a silently
corrupted Silver layer feeding a wrong number to an executive — the exact failure ADR-06 and
ADR-08 exist to prevent.

## Q2 — What if the dashboard gets stale data?

**Detection.** Every pipeline run reports freshness/lag/dead-letter counts to
`gold.ops_control` (`freshness_status` ∈ `fresh`/`stale`/`degraded`).
[resources/ops/business_impact_alerts.sql](../resources/ops/business_impact_alerts.sql) turns
that into actionable "what is stale right now" output.

**Containment (graceful degradation, ADR-07).** When Bronze/Silver stalls, Gold does **not**
serve nothing and does **not** serve silently-old data. `gold.month_end_reconciliation_serving`
carries `is_stale` and `last_good_batch_age_minutes`, so the dashboard shows *stale but valid*
— last known-good numbers, explicitly labelled — instead of either a broken report or a
confident lie.

**Blast radius.** Business consumers see a stale badge and the age of the data; the on-call
engineer is paged via the ops signal. The business experience is "this is a few hours old,"
not "the revenue number is wrong." Recovery is automatic on the next successful run — the
stale flag clears when `freshness_status` returns to `fresh`.

## Q3 — What if the business asks why today's number changed?

This is the question the project could not previously answer crisply, and the reason
[resources/ops/day_over_day_variance.sql](../resources/ops/day_over_day_variance.sql) now
exists. Because the Gold tables are `CREATE OR REPLACE` (one `as_of_date` per run), the query
uses **Delta time travel** to diff the current table version against the prior version and
attribute the change.

**What it answers, in plain English:**

- *"There are 240 more alerts than yesterday"* → headline deltas: total alerts and the split
  by `escalation_reason` (`sanctions_hard_override` vs `behavioral_threshold`), current
  version vs previous.
- *"Which specific entities are new / dropped off?"* → the churn set: entities that entered
  or left `gold.prioritized_alert_queue` between the two runs.
- *"Why did this account jump to the top?"* → the biggest `composite_risk_score` movers in
  `gold.entity_risk_profile`, which pair with each alert's existing `top_contributing_factors`
  to explain the move factor-by-factor.

**Blast radius framing.** The answer is a lineage sentence plus a variance table, delivered
in the meeting — not "let me go check the pipeline." Combined with the immutable ingestion
metadata (audit trail of "what data was known at decision time"), a stakeholder can trace any
number from Bronze source → Silver standardization → Gold score → the exact factors and the
day-over-day delta that moved it.

---

## Recovery primitives (shared)

- **Reprocess from raw** — staged files persist in the Bronze Volume; Bronze is rerunnable.
- **Dead-letter replay** — `replay_txn_dead_letter` job re-ingests quarantined rows after a fix.
- **Idempotent reruns** — streaming sink MERGEs on natural key; Silver/Gold are `CREATE OR REPLACE`.
- **Gated publish** — `build_gold_with_gate.py` blocks a month-end publish that fails reconciliation.
- **Runbook** — [docs/07](07_month_end_incident_runbook.md) for triage + comms cadence,
  [docs/08](08_postmortem_template.md) for the mandatory reliability upgrade after an incident.
