# AML Dashboard — Power BI Project

A `.pbip` project (Power BI's text-based project format), not a `.pbix` file. Open
`AML_Dashboard.pbip` in Power BI Desktop (2024+ recommended — the TMDL/PBIR formats need a
reasonably current Desktop build).

## What's real vs. what needs your hands

**Fully built and should load as-is**: the semantic model
(`AML_Dashboard.SemanticModel/`) — 5 real tables wired to `aml_dev`'s actual Gold/Silver
schema via the Databricks connector (using this workspace's real SQL warehouse endpoint,
`dbc-08add949-9c19.cloud.databricks.com` / warehouse `59901b31d31db40a`), 2 relationships,
and a `_Measures` table with 13 DAX measures covering every KPI in
`docs/00_business_charter.md`.

**Built, but unverified in Desktop** — read this before assuming anything below "just works":
the report (`AML_Dashboard.Report/`) has 3 pages with 19 real visuals hand-authored directly
as PBIR JSON (cards, tables, a column chart, a donut, a line chart, slicers) — not just an
empty shell. Every file is valid JSON (checked directly), and every field/measure reference
matches something that actually exists in the semantic model. What's **not** verified is
whether Power BI Desktop's current schema for `visualContainer` JSON matches what was
hand-written here field-for-field — that can only be confirmed by actually opening it, which
wasn't possible in the environment this was built in.

**If it opens clean**: you have a working 3-page dashboard already laid out — check it
against `DASHBOARD_BUILD_SPEC.md` for anything you'd still want to add or restyle.

**If Desktop offers to "repair" the report, or specific visuals don't render**: the
`.SemanticModel` folder is completely independent of the report and should be unaffected
either way. Two fallback levels, cheapest first:
1. Delete the one malformed visual's folder (`pages/<page>/visuals/<visualId>/`) and rebuild
   just that visual by hand — PBIR's one-file-per-visual layout means a bad visual shouldn't
   take the rest of the page down with it.
2. If the whole report won't open: in Desktop, **File → New report** connected to
   `AML_Dashboard.SemanticModel`, then rebuild pages from `DASHBOARD_BUILD_SPEC.md` (a few
   minutes of drag-and-drop per page — the spec was written to stand on its own regardless of
   whether the hand-authored JSON survives contact with real Desktop).

## First-time setup in Desktop

1. Open `AML_Dashboard.pbip`.
2. On first refresh, Desktop will prompt for the Databricks connection — sign in with
   whatever auth method you use for `meridian-dev` (Azure AD / personal access token).
3. `WatchlistMatchSummary`'s `jurisdictions` column (an `ARRAY<STRING>` in Delta) was
   deliberately left out of the model query — Power Query surfaces array columns as `list`
   values that need a one-time **Extract Values** or **Expand to New Rows** step in the Power
   Query Editor. The M code in `WatchlistMatchSummary.tmdl` has the exact spot to add it back
   if you want it.

## The ROI number is a documented assumption, not a measurement

`Estimated Analyst Hours Saved` (in `_Measures.tmdl`) uses a hardcoded
`MinutesPerLowPriorityReview = 8` constant. There's no real investigator feedback loop in
this portfolio project to calibrate that number from — it's a placeholder assumption, labeled
as such in the DAX comment. Say so out loud if this comes up in an interview; presenting an
assumption as a measured result is the opposite of the explainability this project is built
around.
