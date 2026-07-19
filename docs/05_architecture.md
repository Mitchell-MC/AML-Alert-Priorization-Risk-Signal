# Architecture

```mermaid
flowchart TB
    subgraph EXT["External sources (public internet)"]
        OFAC["OFAC SDN\n(SDN/ALT/ADD/COMMENTS.CSV)"]
        OS["OpenSanctions\n(sanctions + PEPs)"]
        AMLSIM["IBM AMLSim\n(accounts + transactions)"]
        ELLIPTIC["Elliptic Bitcoin dataset\n(Kaggle, via kagglehub)"]
    end

    subgraph STAGE["scripts/stage_raw_files.py\n(runs OUTSIDE Databricks -- serverless has no egress)"]
        FETCH["download + upload to Volume"]
    end

    subgraph VOL["Unity Catalog Volume\naml_dev.bronze.raw_files"]
        RAW["staged raw files"]
    end

    subgraph BRONZE["Bronze (bronze schema)"]
        B_OFAC["bronze.ofac_*"]
        B_OS["bronze.opensanctions_*"]
        B_ACCT["bronze.txn_accounts"]
        B_EVT["bronze.txn_events"]
        B_ELL["bronze.elliptic_txs_*"]
        B_DL["*_dead_letter tables"]
    end

    subgraph STREAM["Structured Streaming"]
        PROD["simulate_txn_feed.py\n(demo producer: paced file drip\n+ ~1% injected malformed rows)"]
        CONS["txn_stream_ingest.py\navailableNow trigger, watermark dedup,\nidempotent MERGE sink, try_cast validation"]
    end

    subgraph SILVER["Silver (silver schema)"]
        S_ENT["silver.entity / entity_alias / entity_address\n(OFAC + OpenSanctions, unified vocabulary)"]
        S_ACCT["silver.account\n(synthetic identities, ~2% seeded collisions)"]
        S_MATCH["silver.match_candidate\n(rapidfuzz + blocking, vs OFAC watchlist)"]
        S_TXN["silver.transaction"]
        S_ELLRISK["silver.elliptic_txn_risk\n(graph-topology analytics)"]
        S_BRIDGE["silver.network_risk_reference\n(disclosed synthetic bridge, ~5% of accounts)"]
    end

    subgraph GOLD["Gold (gold schema)"]
        G_PROFILE["gold.entity_risk_profile\n(explainable additive scoring)"]
        G_QUEUE["gold.prioritized_alert_queue\n(ranked, escalation_reason)"]
        G_SUMMARY["gold.watchlist_match_summary"]
        G_OPS["gold.ops_control\n(freshness/lag/dead-letter per batch)"]
    end

    subgraph BI["Power BI (bi/AML_Dashboard.pbip)"]
        P1["Investigator Triage"]
        P2["Sanctions & PEP Exposure"]
        P3["Operational Health"]
    end

    OFAC --> FETCH
    OS --> FETCH
    AMLSIM --> FETCH
    ELLIPTIC --> FETCH
    FETCH --> RAW

    RAW --> B_OFAC
    RAW --> B_OS
    RAW --> B_ACCT
    RAW --> B_ELL
    RAW --> PROD
    PROD --> CONS
    CONS --> B_EVT
    CONS -.malformed rows.-> B_DL
    B_OFAC -.malformed rows.-> B_DL
    CONS ==reports every batch==> G_OPS

    B_OFAC --> S_ENT
    B_OS --> S_ENT
    B_ACCT --> S_ACCT
    S_ENT --> S_ACCT
    S_ENT --> S_MATCH
    S_ACCT --> S_MATCH
    B_EVT --> S_TXN
    B_ELL --> S_ELLRISK
    S_ELLRISK --> S_BRIDGE
    S_ACCT --> S_BRIDGE

    S_ACCT --> G_PROFILE
    S_MATCH --> G_PROFILE
    S_TXN --> G_PROFILE
    S_BRIDGE --> G_PROFILE
    G_PROFILE --> G_QUEUE
    S_MATCH --> G_SUMMARY
    S_ENT --> G_SUMMARY

    G_PROFILE --> P1
    G_QUEUE --> P1
    G_SUMMARY --> P2
    G_OPS --> P3
```

## Layer responsibilities

- **Staging** (`scripts/stage_raw_files.py`): the only layer that talks to the public
  internet — a real, load-bearing constraint discovered mid-build, not a design preference
  (see `docs/02_environment_and_branching.md`). Runs locally or in CI, never inside a
  Databricks job.
- **Bronze**: raw fidelity, source schema preserved, ingestion metadata attached, malformed
  rows quarantined rather than dropped or crashing the job. OFAC/AMLSim/Elliptic are batch;
  transaction events are the one streaming source.
- **Silver**: normalization, entity resolution, fuzzy matching, transaction modeling. This is
  where the two disclosed synthetic bridges live (account identities, Elliptic network-risk
  linkage) — both documented plainly in `docs/00_business_charter.md`, never presented as
  real KYC or real cross-dataset linkage.
- **Gold**: explainable, rule-based scoring. Every point on `composite_risk_score` traces to
  a named factor in `top_contributing_factors` — no black-box weighting.
- **Ops control**: not a separate layer so much as a cross-cutting concern — every ingestion
  job (currently the streaming consumer; Bronze batch jobs are the natural next addition)
  reports into `gold.ops_control` so a stalled or degraded pipeline is visible rather than
  silent.

## Environments

One Databricks Free Edition workspace, three Unity Catalog catalogs
(`aml_dev`/`aml_test`/`aml_prod_sim`) as the environment boundary — see
`docs/02_environment_and_branching.md` for why (no multi-workspace support on this tier) and
`docs/04_streaming_incident_response.md` for the real incident that shaped the streaming
consumer's reliability design.
