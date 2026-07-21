# Month-End Incident Runbook

Use this when finance-facing reconciliation outputs are stale, blank, or failed during close.

## 0-15 minutes: Triage and communication

- Answer three questions first: what failed, impact, who must know now.
- Send first stakeholder update immediately: "Issue acknowledged, impact under assessment, next update in 30 minutes."
- Identify failed step(s), upstream dependency status, and last successful run timestamp.

## 15-45 minutes: Investigation

- Confirm whether failure mode is schema drift, late-arriving data, permissions, or bad joins.
- Compare current schema to last known schema snapshot (`gold.ops_schema_drift`).
- Validate business impact:
  - Is `gold.month_end_reconciliation_serving` stale?
  - Is `as_of_batch_id` behind expected close window?

## 45+ minutes: Recovery

- Apply a temporary patch with explicit audit trail.
- Re-run month-end critical path job: `month_end_critical_path`.
- If invalid records caused failure, quarantine and replay via `replay_txn_dead_letter`.
- Validate reconciliation gate metrics and source/target tolerances before confirming recovery.

## Required status update template

- Incident: month-end reconciliation pipeline degraded/failed
- Impacted table(s): <table>
- Business impact: <close blocked / stale but serving>
- Last good batch: <batch id>
- ETA to next update: <time>
- Current owner: <team/contact>

## Recovery completion checklist

- Month-end outputs are fresh or explicitly marked stale with known reason.
- `gold.ops_control` has run metadata for the recovery batch.
- Schema drift entry captured in `gold.ops_schema_drift`.
- Postmortem opened using `docs/08_postmortem_template.md`.
