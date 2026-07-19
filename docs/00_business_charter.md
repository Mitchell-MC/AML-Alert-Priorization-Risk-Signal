# Business Charter — AML Alert Prioritization & Entity Risk Signal Lakehouse

## Business problem

A digital payments and remittance provider is overwhelmed by transaction alerts, sanctions
screening hits, and fragmented customer risk signals. Investigators triage alerts in
volume/arrival order because customer, counterparty, and watchlist data are not unified into
a timely risk-prioritization layer. Low-value alerts consume the same investigator time as
genuinely high-risk ones, which increases regulatory exposure (delayed SAR filings, missed
escalations) and slows response to potentially suspicious activity.

## Fictitious client

**Meridian Remit** — a mid-size digital remittance and cross-border payments platform.
High-volume, low-value international transfers (a classic structuring/money-laundering
vector) at a scale where transaction volume has outpaced investigator headcount. Has an
existing AML/compliance function but no unified, risk-ranked investigation queue.

## Stakeholders

| Stakeholder | Stake in this system |
|---|---|
| AML Investigations | Primary consumer of the prioritized alert queue; needs explainable, ranked cases, not raw alert firehose |
| Fraud Operations | Secondary consumer; behavioral/velocity signals overlap with fraud detection |
| Compliance Leadership | Owns regulatory exposure; needs KPI/ROI reporting and auditable, reproducible scoring methodology |
| Data Platform Engineering | Owns pipeline reliability, freshness SLAs, and silent-failure response |

## Business KPIs (drive the Gold mart and BI dashboard)

1. High-risk entities identified per period
2. Alerts prioritized for investigator review (top-N queue) vs. total raw alert volume
3. Investigator queue reduction % — alerts deprioritized/suppressed vs. total raw volume, as
   an hours-saved proxy
4. Average time-to-risk-signal — elapsed time from transaction ingestion to the entity
   appearing in the prioritized queue
5. Watchlist match quality mix — % of matches at exact vs. strong vs. weak confidence (a
   precision proxy, since there's no real investigator feedback loop to measure true
   precision)
6. Risk concentration — % of total flagged risk concentrated in the top X% of entities
   (supports the "focus investigator effort where it matters" narrative)

## Explicit assumptions

There is no real investigator feedback loop or labeled ground truth in this portfolio
setting, so these have to be declared rather than derived:

- **Risk event**: any transaction that trips one or more behavioral thresholds (velocity,
  burst activity, round-dollar patterns, counterparty diversity, exposure to a risky
  counterparty) OR involves an entity with a sanctions/PEP match at strong or exact
  confidence.
- **Watchlist match**: an entity-level fuzzy or exact match against OFAC SDN or
  OpenSanctions (sanctions + PEP) at or above the "weak" confidence band defined by the
  Phase 3 matching module. Matches below that threshold are not surfaced to investigators.
- **Alert escalation threshold**: an entity enters the prioritized queue if its composite
  risk score exceeds a documented cutoff, OR if it has any exact-confidence sanctions match
  — sanctions hits are a hard override regardless of behavioral score, since sanctions
  exposure is a strict-liability regulatory concern, not a probabilistic one.
- AMLSim's `isFraud` node flag (see `dataset_inspection_notes.md`) is used only as a labeled
  signal to sanity-check feature/ranking behavior, never as a training label — scoring stays
  rule-based and explainable, not ML, per scope decision below.
- AMLSim's account graph carries no PII. A synthetic identity-linkage step will attach
  fabricated names/addresses to a subset of accounts so the matching module has real
  candidates to score, including deliberately seeded near-miss collisions against real
  SDN/OpenSanctions aliases. This is simulated KYC data, not derived from any real breach or
  customer records — the README must state this plainly so the artifact is never mistaken
  for a real PII-handling system.
- **Network-risk exposure** (one input to the Gold composite score) is sourced from the
  Elliptic Bitcoin dataset's graph topology (illicit-neighbor ratio), attached to a subset of
  AMLSim accounts via the same kind of synthetic bridge as the identity linkage above —
  Elliptic's transactions and AMLSim's accounts describe unrelated underlying activity and
  share no real join key. This must be disclosed with the same plainness as the identity
  linkage, not presented as if the two datasets describe the same entities.

## Explicit scope

**In scope** (per the governing 6-12 week execution plan — see project memory): explainable
SQL-based scoring, Python entity matching, structured streaming with silent-failure triage,
dashboarded ROI, CI/CD promotion gates, security/audit validation, and resilience/failover
drills.

**Out of scope regardless of plan depth**: ML/statistical risk modeling (scoring stays
rule-based and explainable — this is a deliberate choice, not a time constraint, since
explainability is itself a stated interview talking point) and advanced governance
automation (e.g. automated PII discovery/DLP tooling, enterprise data-catalog integrations)
— basic governance (schema contracts, lineage expectations, quality ownership, classification
tags) is in scope, but it's hand-authored, not tool-automated.
