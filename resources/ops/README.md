# Ops Query Pack

These SQL files are operational triage queries built on `gold.ops_control` and
`gold.ops_schema_drift`.

Use them during month-end or incident response to quickly answer:

- What is stale right now?
- Which business process is impacted?
- Who owns the upstream dependency?
- Which schema changes are breaking?
- What do runtime/cost/freshness trends look like?

## Queries

- `business_impact_alerts.sql`: current actionable alerts (stale, degraded, dead-letter activity)
- `month_end_status.sql`: month-close readiness and latest finance-serving state
- `owner_handoff_queue.sql`: ownership/routing queue for escalations
- `cost_sla_scorecard.sql`: SLA and estimated cost scorecard over recent runs
- `schema_drift_breaking_changes.sql`: latest breaking schema-drift events
- `day_over_day_variance.sql`: attributes "why did today's number change?" by diffing the
  current Gold version against the prior one via Delta time travel (see
  [docs/11_failure_scenario_map.md](../../docs/11_failure_scenario_map.md))

## Usage

Replace `{{catalog}}` with your environment catalog (`aml_dev`, `aml_test`, `aml_prod_sim`) and
run in Databricks SQL warehouse.
