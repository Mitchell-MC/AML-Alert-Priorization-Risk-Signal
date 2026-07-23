# Architecture Decision Record

Every load-bearing decision in this project, stated the way it should be defended out loud —
not "that's how it was set up," but *the business needed X, so we chose Y because Z; the
trade-off was W, and we mitigated it by V*. The decisions themselves already live in the
detailed docs; this file consolidates the **why** in one place so the architecture can be
told as a story, not recited as a parts list.

Each entry links to where the mechanism actually lives.

---

## ADR-01 — Streaming for transaction events, batch for everything else

**The business needed** near-real-time visibility into transaction risk (a 15-minute
freshness SLA), **so we chose** Structured Streaming *only* for the transaction-event
pipeline **because** batch on a nightly/hourly cadence can't reliably hit a 15-minute window.
**The trade-off was** that streaming adds checkpointing, watermark, and idempotency
complexity plus standing operational cost, **and we mitigated it by** keeping OFAC,
OpenSanctions, AMLSim, and Elliptic on batch — those reference/graph datasets publish daily
at most, so streaming them would buy nothing and cost real money.

- SLA: [docs/01](01_nonfunctional_requirements.md) (15-min streaming freshness). Split:
  [docs/05](05_architecture.md) ("txn events are the one streaming source").

## ADR-02 — Catalog-per-environment, not catalog-per-layer

**The business needed** dev/test/prod isolation with change-management gates, **so we chose**
one Unity Catalog catalog per environment (`aml_dev` / `aml_test` / `aml_prod_sim`) with
layers as schemas inside, **because** Databricks Free Edition is a single workspace with no
multi-workspace support. **The trade-off was** that this inverts the "layer + environment as
the catalog" pattern some teams prefer, **and we mitigated it by** documenting it as a
legitimate real-world single-metastore pattern and being ready to defend the axis choice —
environment isolation matters more here than layer-name-as-catalog cosmetics.

- [docs/02](02_environment_and_branching.md); layer/ownership split in
  [docs/09](09_ownership_and_access_model.md).

## ADR-03 — AMLSim as the primary transaction source, Elliptic as a secondary signal

**The business needed** raw account/value/time event data to practice feature engineering on,
**so we chose** AMLSim as the primary pipeline input **because** Elliptic's transactions are
165 already-anonymized PCA features with no raw field left to engineer. **The trade-off was**
losing Elliptic's real illicit labels as a primary input, **and we mitigated it by**
integrating Elliptic narrowly as a network-risk signal (illicit-neighbor ratio) feeding one
input of the Gold composite score.

- README "Note on Elliptic vs. AMLSim"; [docs/03](03_schema_contracts.md) Elliptic caveat.

## ADR-04 — Disclosed synthetic bridges instead of a fake real join

**The business needed** an entity-matching target and a network-risk input, **so we chose**
to attach fabricated identities to AMLSim accounts and to sample network-risk scores from
Elliptic's real distribution, **because** AMLSim carries no PII and shares no real join key
with Elliptic. **The trade-off was** that these are synthetic linkages, not real KYC, **and
we mitigated it by** disclosing both bridges plainly in the charter, README, and schema
contracts — never presented as real cross-dataset linkage.

- [docs/00](00_business_charter.md) assumptions; [docs/03](03_schema_contracts.md) caveats.

## ADR-05 — Synthetic identity as columns on `silver.account`, not a unified entity table

**The business needed** account-to-watchlist matching to work on every rerun, **so we chose**
to keep each account's synthetic identity as plain columns on `silver.account` **because**
`silver.entity` is rebuilt with `CREATE OR REPLACE` from Bronze every run — injecting
synthetic rows there would be wiped or force a circular rebuild. **The trade-off was** no
single unified entity vocabulary across watchlist and accounts, **and we mitigated it by**
carrying the relationship in `silver.match_candidate`, so nothing was actually lost.

- [docs/03](03_schema_contracts.md) "Design correction (2026-07-18)".

## ADR-06 — Fail loud, not broad exception handling

**The business needed** to never ship a "green but wrong" pipeline, **so we chose** to block
broad/silent exception handlers **because** a swallowed error becomes a silent data-quality
failure that corrupts trust downstream. **The trade-off was** that jobs abort on unexpected
conditions instead of limping along, **and we mitigated it by** enforcing it in CI
(`scripts/check_ai_risk_patterns.py`) and handling only genuinely recoverable cases (e.g.
malformed rows → dead-letter, not crash).

- [docs/06](06_pipeline_survival_framework.md) §1; [docs/01](01_nonfunctional_requirements.md)
  enforced controls.

## ADR-07 — Graceful degradation over hard-fail on stale upstream

**The business needed** a dashboard that never lies, **so we chose** to serve the last
known-good Gold tables with an explicit `is_stale` flag when Bronze/Silver stalls **because**
a stale-but-valid number an executive can trust beats both a broken report and a silently
wrong one. **The trade-off was** continuity over completeness (consumers may see slightly old
data), **and we mitigated it by** making staleness visible — `gold.ops_control` freshness
status and `gold.month_end_reconciliation_serving.is_stale` surface it rather than hiding it.

- [docs/01](01_nonfunctional_requirements.md) degraded-mode; [docs/11](11_failure_scenario_map.md).

## ADR-08 — Idempotent MERGE sink for streaming

**The business needed** exactly-once transaction facts across restarts, **so we chose** a
deterministic natural-key `MERGE` sink **because** a plain append double-counted rows on
replay — a real duplicate-amplification incident (2× rows) caught in a resilience drill.
**The trade-off was** MERGE costs more than append, **and we mitigated it by** scoping it to
the streaming sink where correctness on restart is non-negotiable; batch builds stay
`CREATE OR REPLACE` and rerunnable.

- [docs/04](04_streaming_incident_response.md); [docs/06](06_pipeline_survival_framework.md) §5.

## ADR-09 — Explainable additive scoring, not a black-box model

**The business needed** every alert to be auditable and reproducible (a regulatory
expectation), **so we chose** additive rule-based risk scoring **because** each point of the
composite score must trace to a named factor. **The trade-off was** less predictive
sophistication than an ML model, **and we mitigated it by** persisting
`top_contributing_factors` and `escalation_reason` so an investigator sees exactly why an
alert ranked where it did — explainability was the higher-value property here than raw AUC.

- [docs/01](01_nonfunctional_requirements.md) audit; [docs/03](03_schema_contracts.md) Gold.

## ADR-10 — Governance and access as code

**The business needed** governance to be enforced, not aspirational, **so we chose** to
encode it — CI policy checks (governance/contract/AI-risk), classification tags, and the
ownership/access/masking DDL **because** governance that only exists in a slide deck isn't
governance. **The trade-off was** that Free Edition can't actually enforce the group-based
grants and masks (single identity, no groups), **and we mitigated it by** shipping them as
reviewed, runnable design-intent DDL that applies as-is on an enterprise account.

- [docs/09](09_ownership_and_access_model.md); `resources/ddl/01_ownership_and_access.sql`.

## ADR-11 — PAT auth for CI/CD instead of a service principal

**The business needed** automated GitHub Actions → Databricks deploys, **so we chose** a
scoped personal access token in a repository secret **because** Free Edition has no service
principals or SCIM. **The trade-off was** CI/CD runs as a human identity rather than a
least-privilege dedicated principal, **and we mitigated it by** scoping the token and calling
out plainly what an enterprise account would do differently — a stated deviation, not a
gap glossed over.

- [docs/02](02_environment_and_branching.md) auth/access.
