# Environment Foundation & Branching Model

> **Resolved**: confirmed on Databricks **Free Edition** (the current serverless/Unity
> Catalog-enabled free tier, not the older single-node classic Community Edition). That
> unlocks Unity Catalog governance, serverless SQL warehouses, serverless job compute, and
> Lakeflow Declarative Pipelines. It's still a **single personal workspace** — no account
> console, no multiple workspaces, no service principals, no SSO/SCIM. Everything below is
> written around that reality rather than an idealized multi-workspace enterprise setup.

## Environments

Single-developer portfolio project on a single workspace, so environment separation happens
through **Unity Catalog catalogs**, not separate workspaces or deployment targets. "Prod"
doesn't mean a live customer-facing environment — it means the highest-fidelity synthetic
catalog used for the final interview-ready demo and the Phase 10 resilience/performance
drills.

| Environment | Unity Catalog catalog | Purpose | Data |
|---|---|---|---|
| `dev` | `aml_dev` | Day-to-day development, fast iteration | Sampled subsets (small slice of OFAC/OpenSanctions, small AMLSim run) |
| `test` | `aml_test` | CI-triggered validation | Synthetic fixtures sized for deterministic, fast test runs |
| `prod-sim` | `aml_prod_sim` | Full-scale demo target | Full reference datasets + full AMLSim transaction volume |

Each catalog holds the same `bronze` / `silver` / `gold` schemas from
`docs/03_schema_contracts.md` (e.g. `aml_dev.bronze.ofac_sdn`, `aml_prod_sim.gold.entity_risk_profile`)
— identical structure per environment, different data volume and freshness cadence. This is
a legitimate real-world Unity Catalog pattern (catalog-per-environment in a single metastore),
not just a free-tier workaround.

## Branching model

Trunk-based development with short-lived feature branches per phase or component (e.g.
`feat/bronze-ofac-ingestion`, `feat/entity-matching-confidence-bands`). PRs merge into
`main`; `main` is always deployable to the `dev` catalog. Promotion to `prod-sim` is a
tagged-release or manually-triggered step, not automatic on every merge — mirrors a real
change-management gate even at single-workspace scale.

## Secrets strategy

- Databricks secret scopes for any tokens used inside notebooks/jobs.
- No credentials committed to the repo, ever.
- Local-only configuration (e.g. a personal access token for ad hoc local runs) goes in a
  gitignored `.env`, never in code or notebooks.

## Auth / access (Free Edition constraint)

Free Edition has no service principals or SCIM — there's one personal identity. CI/CD
deployment (GitHub Actions → Databricks) authenticates with a **scoped personal access
token** stored as a GitHub Actions repository secret, not a service principal. This is
called out explicitly as a deliberate deviation from enterprise practice (where a
CI/CD-dedicated service principal would be least-privilege scoped separately from any
human's identity) — worth stating plainly in interview packaging rather than glossing over,
since it's a real constraint of the free tier and a legitimate "here's what I'd do
differently with an enterprise account" talking point.

## Compute policy

No manual cluster sizing/autoscaling policy needed — Free Edition compute is **serverless**
(serverless SQL warehouses for BI/ad hoc queries, serverless compute for jobs and Lakeflow
Declarative Pipelines). This actually strengthens the cost-optimization story rather than
weakening it: serverless scales to zero between runs instead of relying on idle-termination
timers on a standing cluster. Document any Free Edition usage-credit ceilings encountered
during the Phase 10 load/resilience drills, since that's the real capacity constraint here
in place of a cluster node cap.

## Pipeline orchestration

Medallion transformations (Bronze→Silver→Gold) are implemented as **Lakeflow Declarative
Pipelines** where the declarative model fits (continuous/incremental Bronze and Silver
transforms), with Databricks Workflows (Jobs) for anything needing custom control flow
(e.g. the Python entity-matching step, ops-control table updates). This leans into the
orchestration strength already called out as a foundation skill rather than a centerpiece.

## CI/CD (GitHub Actions)

- On every PR: lint, type-check, unit tests, and schema-contract validation against the
  contracts in `docs/03_schema_contracts.md`.
- On merge to `main`: `databricks bundle deploy` to the `dev` target (→ `aml_dev` catalog),
  authenticated via the PAT-based secret above.
- On tagged release: promotion gate to the `prod-sim` target (→ `aml_prod_sim` catalog),
  including the resilience/rollback rehearsal called for in the execution plan's Phase 9.
