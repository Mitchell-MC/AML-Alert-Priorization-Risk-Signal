# AML Alert Prioritization & Entity Risk Signal Lakehouse

A lakehouse-based AML/fraud signal platform that ingests sanctions, PEP, and
transaction-style event data to prioritize high-risk entities for investigation. Portfolio
project, built and verified live on a real Databricks workspace — see
[Verified results](#verified-results) for what's actually running, not just designed.

## Business problem

A digital payments and remittance provider is overwhelmed by transaction alerts, sanctions
screening hits, and fragmented customer risk signals. Investigators spend too much time on
low-value alerts because customer, counterparty, and watchlist data aren't unified into a
timely, risk-ranked queue. Full framing, stakeholders, and KPI definitions in
[docs/00_business_charter.md](docs/00_business_charter.md).

## Architecture

Medallion architecture on Databricks Free Edition / Unity Catalog / Delta Lake — diagram and
per-layer responsibilities in [docs/05_architecture.md](docs/05_architecture.md).

- **Bronze** — raw OFAC SDN, OpenSanctions (sanctions + PEP), AMLSim, and Elliptic, landed
  with ingestion metadata, schema versioning, and malformed-row quarantine (not just
  designed — a real trailing-EOF-marker quirk in OFAC's export and a real malformed
  transaction value were both caught and correctly dead-lettered on live runs).
- **Silver** — unified entity resolution (OFAC + OpenSanctions, 838K entities), Python
  fuzzy matching with explainable confidence bands (rapidfuzz + first-character blocking,
  cross-validated and benchmarked against real data), synthetic account identities, and
  transaction modeling.
- **Gold** — explainable, additive rule-based risk scoring and a prioritized alert queue,
  plus an operational-health control layer for streaming freshness and silent-failure
  detection.
- **Serving** — a Power BI project (`bi/AML_Dashboard.pbip`) for investigator triage,
  sanctions/PEP exposure, and pipeline ops health.

Full schema contracts in [docs/03_schema_contracts.md](docs/03_schema_contracts.md).

## Verified results

Everything below happened on a real `aml_dev` Unity Catalog run, not a design estimate —
details and the bugs found/fixed along the way in
[dataset_inspection_notes.md](dataset_inspection_notes.md) and
[docs/04_streaming_incident_response.md](docs/04_streaming_incident_response.md).

| | |
|---|---|
| Unified watchlist entities (`silver.entity`) | 838,309 |
| Match candidates produced (`silver.match_candidate`) | 116,476 |
| Seeded near-miss collisions recovered | **345 / 377 (91.5%)**, avg score 96.9 |
| Streaming: valid txns / dead-lettered | 117,703 / 1,194 |
| Streaming: duplicate rows after idempotent-MERGE fix + restart | **0** (was 2x before the fix) |
| Gold alerts (`gold.prioritized_alert_queue`) | 5,142 of 20,000 accounts |
| Top 10 ranked alerts that are deliberately-seeded test cases | **10 / 10** |

That last row is the one that matters most: every account in this project's top 10 highest
scores is a planted near-miss sanctions match, correctly bubbling to the top with the right
escalation reason — concrete, end-to-end proof the whole pipeline behaves as intended, not
just that each stage runs without error.

## Data sources

| Source | Use | License |
|---|---|---|
| [OFAC SDN List](https://ofac.treasury.gov/specially-designated-nationals-and-blocked-persons-list-sdn-human-readable-lists) | Primary US sanctions watchlist | US public domain |
| [OpenSanctions](https://www.opensanctions.org/datasets/default/) — `sanctions` + `peps` collections | Broader sanctions and PEP exposure | CC 4.0 BY-NC (free for this non-commercial use) |
| [IBM AMLSim](https://github.com/IBM/AMLSim) — pre-generated sample graph | Primary transaction-style event data with labeled laundering typologies (fan-in, cycles) | Apache 2.0 |
| [Elliptic Bitcoin Dataset](https://www.kaggle.com/datasets/ellipticco/elliptic-data-set) (Kaggle) | Secondary network-risk signal — graph-topology features (illicit-neighbor ratio) feed one input into the Gold composite score | Per Kaggle listing, research use |

Dataset schemas, real row counts, and format quirks discovered during ingestion are in
[dataset_inspection_notes.md](dataset_inspection_notes.md) — including a real correction to
an earlier naive row-count (embedded newlines in quoted CSV fields inflate a plain `wc -l`
count) and the actual OFAC dead-letter cause (a legacy DOS EOF marker byte).

**Note on Elliptic vs. AMLSim**: the original project brief's narrative names Elliptic as the
primary transaction dataset, but its actual Datasets list only links AMLSim (with Elliptic
mentioned only in passing). Elliptic's transactions are individual Bitcoin transactions with
165 already-anonymized features — there's no raw field left to feature-engineer, which works
against this project's specific goal of practicing feature engineering from raw event data.
AMLSim (raw account/value/time fields) is used as the primary pipeline input instead, with
Elliptic integrated narrowly as a secondary network-risk signal.

## Simulated data disclosure

All account-holder identity data (names, addresses) is synthetic — AMLSim's transaction
graph carries no PII, so a synthetic identity-linkage layer attaches fabricated identities to
a subset of accounts to give the entity-matching module real candidates to score against.
This is never derived from real customer or breach data.

Similarly, the "network-risk exposure" signal is a synthetic bridge: Elliptic's Bitcoin
transaction graph and AMLSim's simulated remittance accounts describe unrelated activity and
share no real join key, so a subset of accounts is assigned a network-risk score sampled
from Elliptic's real illicit-neighbor-ratio distribution — not a claim that the two datasets
describe the same entities. See [docs/00_business_charter.md](docs/00_business_charter.md)
for both assumptions in full.

## Running it

1. **Stage raw data** (runs outside Databricks — serverless compute has no internet egress,
   see [docs/02_environment_and_branching.md](docs/02_environment_and_branching.md)):
   `python scripts/stage_raw_files.py --catalog aml_dev --profile <your-cli-profile>`
2. **Deploy the Databricks Asset Bundle**: `databricks bundle deploy --target dev`
3. **Run Bronze**: `databricks bundle run bronze_ingestion --target dev`
4. **Build Silver**: run `silver/build_entities.sql`, then the
   `silver_accounts_and_matches` and `silver_network_risk_reference` bundle jobs, then
   `silver/build_transactions.sql` and `silver/build_elliptic_risk.sql`
5. **Build Gold**: run `gold/build_gold.sql`
6. **Streaming**: `databricks bundle run streaming_validation_run --target dev` (drips
   AMLSim transactions through a paced producer into the Structured Streaming consumer)
7. **Month-end critical path**: `databricks bundle run month_end_critical_path --target dev`
  (runs guarded publish flow with reconciliation gate checks)
8. **Dead-letter replay (as needed)**: `databricks bundle run replay_txn_dead_letter --target dev`
9. **Dashboard**: open `bi/AML_Dashboard.pbip` in Power BI Desktop — see
   [bi/README.md](bi/README.md)

## Project status

- [x] Business charter, KPIs, and explicit assumptions
- [x] Non-functional requirements (SLA/SLO, retention, recovery, PII handling, audit)
- [x] Environment and branching conventions
- [x] Bronze/Silver/Gold schema contracts
- [x] Bronze ingestion pipelines — deployed and verified on real data
- [x] Streaming ingestion with silent-failure handling — deployed, verified, and one real
      duplicate-amplification bug found and fixed via an actual resilience test
- [x] Python entity normalization and fuzzy matching — tested and benchmarked against real data
- [x] Silver feature engineering
- [x] Gold alert prioritization scoring — verified against real data
- [x] CI/CD workflow written, lint/test/schema-validation steps verified locally
- [x] Streaming resilience drill and incident-response narrative
- [x] Architecture diagram
- [x] CI/CD deploy jobs live-tested — pushed to GitHub, secrets wired up, `deploy-dev`
      confirmed passing on a real Actions run (`deploy-prod-sim` gated behind a required
      reviewer + tag-only policy, correctly skipped on this push)
- [x] Power BI semantic model — 5 tables wired to real `aml_dev` schema, 13 DAX measures,
      confirmed loading correctly in real Power BI Desktop (all tables visible, structure
      intact) after fixing a missing `version.json` found via an actual open attempt
- [ ] Power BI report visuals — 19 visuals hand-authored as PBIR JSON across 3 pages; report
      shell opens in Desktop, individual visual rendering not yet fully confirmed — see
      [bi/README.md](bi/README.md) for the fallback path if any visual doesn't survive contact
      with real Desktop
- [x] Databricks Lakeview dashboard — a second, fully-verified BI deliverable: 7 datasets, 3
      pages, 16 widgets, every underlying query executed directly against the warehouse
      before being embedded, deployed via the same Asset Bundle as everything else, hourly
      auto-refresh, shared read-only with all workspace users via a single link (see
      [resources/dashboards/README.md](resources/dashboards/README.md)) — this one doesn't
      have the Power BI report's "can't verify without Desktop" caveat

## Docs

- [docs/00_business_charter.md](docs/00_business_charter.md) — business problem,
  stakeholders, KPIs, assumptions, scope
- [docs/01_nonfunctional_requirements.md](docs/01_nonfunctional_requirements.md) — SLAs,
  retention, recovery objectives, PII handling, audit requirements
- [docs/02_environment_and_branching.md](docs/02_environment_and_branching.md) — dev/test/prod
  conventions, secrets, CI/CD, branching model, and the real serverless-egress constraint
  that shaped the ingestion architecture
- [docs/03_schema_contracts.md](docs/03_schema_contracts.md) — Bronze/Silver/Gold table
  contracts
- [docs/04_streaming_incident_response.md](docs/04_streaming_incident_response.md) — the real
  duplicate-amplification incident: detection, root cause, fix, verification
- [docs/05_architecture.md](docs/05_architecture.md) — architecture diagram and layer
  responsibilities
- [docs/06_pipeline_survival_framework.md](docs/06_pipeline_survival_framework.md) —
  fail-loud coding standards, CI gatekeepers, and governance-as-code checklist
- [docs/07_month_end_incident_runbook.md](docs/07_month_end_incident_runbook.md) —
  month-end triage, stakeholder communication cadence, and recovery checklist
- [docs/08_postmortem_template.md](docs/08_postmortem_template.md) —
  incident postmortem template with mandatory reliability-upgrade action
- [dataset_inspection_notes.md](dataset_inspection_notes.md) — real dataset schemas, row
  counts, and every bug found during ingestion
- [resources/ops/README.md](resources/ops/README.md) — operational SQL query pack for
  stale-impact triage, owner handoffs, schema drift, and SLA/cost scorecards
