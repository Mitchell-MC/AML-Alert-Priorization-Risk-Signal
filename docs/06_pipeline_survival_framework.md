# Pipeline Survival Framework (2026)

This checklist codifies the production-safety standards used in this repository. It exists
to prevent "green but wrong" pipelines: jobs that run successfully while silently damaging
trust, cost, or compliance.

## 1) Fail Loud by Default

- Broad exception handlers (`except Exception`, bare `except`) are blocked in CI by
  `scripts/check_ai_risk_patterns.py`.
- Only recoverable cases should be handled (for example, rate-limit retries); everything
  else must fail immediately with traceable context.

## 2) Enforce Data Contracts at Boundaries

- Critical jobs validate required input columns and non-empty source datasets.
- Contract violations raise `DataContractError` from
  `src/aml_lakehouse/common/risk_guardrails.py`.

## 3) Use Circuit Breakers for Data Quality

- Streaming ingestion enforces a hard invalid-row ratio cap (`MAX_INVALID_RATIO`) and aborts
  the micro-batch when exceeded.
- This prevents bad data from propagating silently downstream.

## 4) Require Structured Incident Logging

- Batch and streaming jobs emit JSON structured events (timestamp, pipeline, batch,
  row counts, quality/circuit-breaker details) via
  `src/aml_lakehouse/common/structured_logging.py`.

## 5) Make Idempotency Non-Negotiable

- Streaming writes use deterministic natural-key `MERGE` semantics (`_merge_append`) so
  restarts/replays do not duplicate facts.
- Silver and Gold table builds are deterministic and rerunnable.

## 6) Enforce CI/CD Gatekeepers

- CI blocks merges when lint, tests, SQL statement validation, AI safety audit, governance
  checks, or coverage threshold fail.
- Coverage floor is enforced in `.github/workflows/ci.yml`.

## 7) Test Failure Paths Explicitly

- Unit tests must include fail-on-bad-data behavior, not just happy-path outcomes.
- `tests/test_risk_guardrails.py` validates contract and circuit-breaker failures.

## 8) Tie Engineering to Business Outcomes

- Every pipeline run records operational metrics to `gold.ops_control` for before/after
  benchmarking and trend monitoring.
- Optimization and reliability changes should be justified in terms of cost, freshness,
  and incident reduction.

## 9) Audit AI-Generated Code Before Merge

- Automated static checks review risky code patterns commonly emitted by LLMs.
- Human review still verifies architecture, scalability, and cost-aware behavior.

## 10) Treat Governance as Code

- CI policy checks detect secret leaks and blocked PII field patterns before merge.
- Data classification and auditability requirements remain defined in
  `docs/01_nonfunctional_requirements.md`.