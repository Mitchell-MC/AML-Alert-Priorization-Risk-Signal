# Ownership & Access Model

Per-layer ownership, least-privilege access, PII masking, and row filtering for the medallion
layers. This closes the two governance gaps the structure otherwise only implied: *who owns
each layer* and *who can read what*. The video framing is deliberate here — a layer with no
clear owner "decays on a schedule," and broad access to raw layers "creates confusion and
chaos at scale."

## Free Edition caveat (read first)

Databricks Free Edition is a single personal workspace with one identity — no account
console, no groups, no service principals, no SCIM (see
[docs/02_environment_and_branching.md](02_environment_and_branching.md)). So the access model
below **cannot actually be enforced on this tier**: there are no groups to grant to. It is
implemented as *design-intent DDL* in
[resources/ddl/01_ownership_and_access.sql](../../resources/ddl/01_ownership_and_access.sql)
— real, valid Unity Catalog syntax (`ALTER SCHEMA ... OWNER TO`, `GRANT`, `SET MASK`,
`SET ROW FILTER`, `SET TAGS`) that runs as-is on an enterprise account once the groups exist.
This is the same "here's exactly what I'd apply with a real account" posture used for the
CI/CD service-principal deviation in docs/02 — documented as a constraint, not glossed over.

## Principal groups

Provisioned out-of-band (account console / SCIM / Terraform), not in SQL.

| Group | Role |
|---|---|
| `aml_platform_admins` | Platform / Unity Catalog administration, break-glass access |
| `aml_data_engineers` | Own Bronze, build Silver; the only humans who see raw (unmasked) PII |
| `aml_analytics_engineers` | Own Gold, co-build Silver business logic |
| `aml_data_scientists` | Silver access for modeling, PII masked |
| `aml_bi_analysts` | Gold consumption through approved BI tools |
| `aml_business_users` | Gold consumption; suppressed alerts and ops detail filtered out |

## Ownership by layer

Ownership follows the layer's job: source-facing layers to engineering, business-facing
layers to analytics engineering. On this single-owner portfolio project all of these are one
person — the point is that the *responsibility per layer is explicit*, not diffuse.

| Layer | Owner | Contributors | Owner's responsibilities |
|---|---|---|---|
| **Bronze** | `aml_data_engineers` | `aml_platform_admins` | Ingestion, source connections, landing patterns, schema-drift handling, replay, ingestion metadata |
| **Silver** | `aml_data_engineers` | `aml_analytics_engineers`, data stewards | Conformance, dedup, key alignment, reusable business entities, PII masking |
| **Gold** | `aml_analytics_engineers` | `aml_data_engineers` | KPI/scoring definitions, certification, business-facing correctness, reconciliation |
| **Governance UDFs** | `aml_platform_admins` | — | Masking / row-filter functions, classification tags, access policy |

Upstream *source* ownership (who to page when an incoming feed breaks) is a separate axis,
tracked per pipeline in [resources/ops/upstream_dependencies.json](../../resources/ops/upstream_dependencies.json)
and surfaced through `gold.ops_control` (`upstream_owner` / `upstream_contact`) and
[resources/ops/owner_handoff_queue.sql](../../resources/ops/owner_handoff_queue.sql). That
answers "who owns the *feed*"; the table above answers "who owns the *layer*."

## Access model by layer

The rule from the video: the farther raw data is from the average consumer, the fewer
accidental problems get created. Bronze is tight, access widens as data becomes trusted, and
narrows again for anything operational.

| Layer | Read access | Write access | Notes |
|---|---|---|---|
| **Bronze** | DE, platform admins | DE | Tight. No analyst / data-scientist / business access — raw quirks and pre-masking PII live here |
| **Silver** | DE, analytics eng, data scientists, admins | DE (tables), analytics eng (business logic) | Controlled. SIMULATED-PII masked for everyone except DE / admins |
| **Gold** | analytics eng, BI analysts, business users, DE, data scientists, admins | analytics eng | Broad but governed — the default safe path. `ops_control` excluded from business users |
| **Governance** | admins | admins | Policy objects only |

## Data classification → treatment

Classification tags come from [docs/01_nonfunctional_requirements.md](01_nonfunctional_requirements.md).
The DDL applies them as Unity Catalog tags so they're queryable and discoverable, not just
prose.

| Classification | Example columns | Treatment |
|---|---|---|
| `PUBLIC` | OFAC / OpenSanctions list fields | No masking; public by nature |
| `SIMULATED-PII` | `silver.account.synthetic_holder_name`, `synthetic_dob`, `synthetic_country`; `silver.entity_address.address_line` | Column mask — cleartext only for DE / admins, redacted otherwise |
| `INTERNAL` | `gold.entity_risk_profile`, `gold.prioritized_alert_queue` scores/rationale | Gold-layer governed access; suppressed alerts row-filtered from business consumers |

## Column masking

Three masking UDFs in the `governance` schema, attached via `ALTER COLUMN ... SET MASK`:

- `mask_holder_name` → cleartext for DE/admins, otherwise a stable `MASKED#<sha2 prefix>`
  token (preserves join/group-by behaviour without revealing the name).
- `mask_dob` → cleartext for DE/admins, otherwise `NULL`.
- `mask_address` → cleartext for DE/admins, otherwise `*** REDACTED ***`.

Masking is enforced at the Silver boundary, which is exactly where the video says
masking/hashing should be applied before a broader technical audience gets access.

## Row filtering

`gold.prioritized_alert_queue` carries a row filter (`governance.alert_visibility`):
reviewers and engineers (`aml_analytics_engineers` / `aml_data_engineers` /
`aml_platform_admins`) see every alert including `suppressed` ones; BI and business consumers
never see suppressed alerts, so a dismissed false-positive can't resurface in a general
dashboard.

## Applying it

On an enterprise account, after the groups exist:

```sql
-- run once per environment catalog, same substitution pattern as the transform SQL
-- (aml_dev / aml_test / aml_prod_sim)
```

Run [resources/ddl/00_setup_catalogs.sql](../../resources/ddl/00_setup_catalogs.sql) first
(catalogs/schemas/volumes), then
[resources/ddl/01_ownership_and_access.sql](../../resources/ddl/01_ownership_and_access.sql)
with `{catalog}` substituted, after the Silver and Gold tables exist (masks and row filters
attach to live tables). On Free Edition it stays as reviewed, un-applied specification.
