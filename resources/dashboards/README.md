# AML Dashboard — Databricks Lakeview (native, no client install needed)

A second BI deliverable alongside `bi/AML_Dashboard.pbip` (Power BI), built for a different
requirement: a dashboard "end users" can open with nothing but a link — no Power BI license,
no Desktop install, no per-user Databricks credentials to configure. This is the one that's
actually verified end-to-end against the real workspace, not just schema-valid JSON, because
the Lakeview API let it be tested the same way everything else in this project was: create,
check the real error, fix, repeat.

## What this is

`aml_dashboard.lvdash.json` — a Lakeview dashboard definition (Databricks' native BI format,
analogous to a `.pbip`/TMDL project but deployed through the same Databricks Asset Bundle as
every other resource in `resources/`). 7 datasets, 3 pages (Investigator Triage, Sanctions &
PEP Exposure, Operational Health), 16 widgets — the same page/KPI structure as the Power BI
version, independently re-derived and validated against `aml_dev`'s real Gold/Silver tables.

**Every widget's underlying SQL was executed directly against the warehouse before being
embedded in the dashboard** — not assumed correct from the JSON alone. That's how a real bug
was caught before it ever reached the dashboard: an early "average ingestion lag" KPI came
back `Infinity`, traced to two causes — batches with zero processed rows store a `lag_seconds`
sentinel of infinity by design, and *every* batch's lag looks enormous because it's measured
against the synthetic `event_time` anchor (a fixed `2026-01-01` date, not real wall-clock
time — see `docs/04_streaming_incident_response.md`'s sibling finding). Fixed by using
`recorded_at` (the real wall-clock ingestion timestamp) for the dashboard's freshness KPI
instead — "minutes since last batch reported," which is both more honest and a more standard
ops-monitoring signal anyway.

## Access model

- **Sharing**: `permissions: group_name: users, level: CAN_READ` in `dashboard.yml` — every
  workspace member can view it, no per-user setup.
- **Credentials**: published with `embed_credentials: true`, so viewers see live data using
  the *publisher's* Unity Catalog access, not their own. Nobody needs Unity Catalog grants on
  `aml_dev` just to look at the dashboard. (Trade-off to know: this means view access to the
  dashboard is effectively view access to the underlying data, scoped to whatever's on the
  pages — appropriate for this project's threat model, worth re-examining before using this
  pattern somewhere with real sensitive data.)
- **Refresh**: hourly (`0 0 * * * ?` Quartz cron, `America/New_York`), set up via
  `POST /api/2.0/lakeview/dashboards/{id}/schedules` — the CLI's own
  `lakeview create-schedule` positional-arg shortcut rejects every valid cron string tried
  (client-side bug, not a server-side rejection); calling the REST API directly with the
  proper nested `cron_schedule: {quartz_cron_expression, timezone_id}` object works.

## Deploying / updating

```
databricks bundle deploy --target dev
```

deploys the dashboard definition and permissions as part of the same bundle as every other
resource. Two things bundle deploy does **not** do automatically, so redo them by hand (or
script it) after any redeploy that changes the dashboard content:

```
databricks lakeview publish <dashboard_id> --warehouse-id 59901b31d31db40a
databricks api post /api/2.0/lakeview/dashboards/<dashboard_id>/published \
  --json '{"embed_credentials": true, "warehouse_id": "59901b31d31db40a"}'
```

Get the current dashboard ID and URL from `databricks bundle summary --target dev`.

## Known gap vs. the Power BI version

No `network_risk_exposure` / Elliptic-sourced widget yet, and the alert-queue table doesn't
yet show `top_contributing_factors` (an `array<struct>` column — same Power Query-style
"needs an explode/flatten step" caveat as `WatchlistMatchSummary.jurisdictions` in the Power
BI project). Straightforward to add as a further dataset/widget; not done here for time.
