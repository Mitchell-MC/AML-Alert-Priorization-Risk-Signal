# Non-Functional Requirements

These are written as realistic targets for a production AML platform, then validated in this
project through synthetic drills rather than real production load (see Phase 10 in the
execution plan — load/replay/failover testing). Treating them as real design constraints,
not decoration, is itself part of the interview story: the resilience work in Phase 6/8/10
only makes sense if there's a documented target to fail against and recover from.

## Freshness SLAs

| Data | Target |
|---|---|
| Reference data (OFAC / OpenSanctions) | No more than 24h stale, matching upstream publish cadence |
| Transaction streaming | No more than 15 minutes stale under normal operation (micro-batch trigger every 1-5 min) |
| Gold alert queue | Reflects Silver data no more than one scheduled refresh cycle behind |

## Latency SLOs

- **Time-to-risk-signal** (transaction ingested → entity appears in the prioritized alert
  queue): target p95 < 30 minutes.
- **Watchlist re-screening turnaround** (new/updated sanctions or PEP entity → existing
  accounts re-screened against it): target < 24h.

## Retention

| Layer | Retention |
|---|---|
| Bronze (raw) | Indefinite / configurable — modeled on BSA's 5-year AML recordkeeping requirement, even though the project won't actually accumulate 5 years of synthetic data |
| Silver / Gold | Rolling 13 months active, archive beyond that |
| Dead-letter / quarantine | 90 days for investigation, then purge or archive |

## Recovery objectives

- **RPO** for streaming ingestion: ≤ 1 micro-batch — checkpointing ensures at most one
  in-flight batch is at risk on failure.
- **RTO**: pipeline resumes from checkpoint within minutes of restart; no manual replay for
  the common-case failure.
- **Degraded-mode behavior**: when Bronze ingestion stalls, downstream Silver/Gold jobs keep
  serving the last-known-good Gold tables with an explicit `is_stale` flag rather than
  failing outright — continuity over completeness.

## PII handling

All "customer" data in this project is synthetic (AMLSim's account graph plus fabricated
identities layered on top — see the business charter's assumptions). No real PII exists.
That said, the synthetic identity fields (name, address) are treated *as if* real for design
purposes: access patterns, column-level tagging, and BI exposure are designed the way they
would be for a real system, even though there's no real IAM backing the enforcement at
portfolio scale. This is a deliberate design exercise, not a claim of real compliance
controls.

**Data classification tags**:
- `PUBLIC` — sanctions/PEP list data (OFAC and OpenSanctions are both public by nature)
- `SIMULATED-PII` — synthetic customer identity fields (name, address, DOB)
- `INTERNAL` — risk scores, alert rationale, match explanations

The per-layer ownership matrix, least-privilege access model, and the column masks / row
filters / classification tags that enforce these treatments are specified as runnable Unity
Catalog DDL in [docs/09_ownership_and_access_model.md](09_ownership_and_access_model.md) and
`resources/ddl/01_ownership_and_access.sql` (design-intent on Free Edition, which has no
groups to grant to).

## Audit requirements

- Every alert's score must be explainable: persist which specific factors contributed
  (watchlist match confidence, which behavioral thresholds tripped, network-risk indicators)
  and their individual values/weights — not just the final composite score. This is both a
  stated project deliverable and a real regulatory expectation (screening decisions must be
  explainable and reproducible).
- Ingestion metadata (source, timestamp, schema version, row/file counts) is immutable and
  queryable per batch, forming the audit trail for "what data was known at decision time."

## Enforced controls

- **Fail-loud coding standard**: broad or silent exception handlers are blocked in CI via
  `scripts/check_ai_risk_patterns.py`.
- **Data contract guardrails**: required-column/non-empty/invalid-ratio checks are enforced
  with `src/aml_lakehouse/common/risk_guardrails.py`.
- **Structured incident telemetry**: batch and streaming jobs emit JSON logs with row counts,
  lag, and quality metrics.
- **Governance-as-code checks**: CI runs `scripts/check_governance_policies.py` to catch
  secret patterns and blocked PII field names in SQL assets before merge.
