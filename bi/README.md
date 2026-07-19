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

**Shell only, needs visual layout**: the report (`AML_Dashboard.Report/`) has 3 correctly
named, empty pages already wired to the semantic model. None of this project's JSON could be
validated by actually opening Power BI Desktop, so treat the report shell as
higher-risk than the semantic model — if Desktop offers to "repair" the report on first
open, or the pages don't show up, the fallback is: keep the `.SemanticModel` folder (it's
independent and should be unaffected), and in Desktop use **File → New report** connected to
this semantic model instead of opening the existing report shell. You lose nothing but the
empty page shells, which take a minute to recreate.

Every visual below is something you build in a few minutes with drag-and-drop from the
Fields pane — that's the "your visual polish" part. See `DASHBOARD_BUILD_SPEC.md` for the
exact page-by-page spec (fields, measures, visual types, filters).

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
