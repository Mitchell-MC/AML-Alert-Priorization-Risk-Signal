# Schema Contracts — Bronze / Silver / Gold

Column-level contracts, not full DDL — DDL gets written alongside the actual ingestion code
in each phase. Grounded in the real source schemas recorded in `dataset_inspection_notes.md`,
not assumed ones. Update that file first if a source schema turns out to differ once real
ingestion code touches it, then reconcile here.

Table names below are schema-qualified (`bronze.*`, `silver.*`, `gold.*`); the full Unity
Catalog three-level name adds the environment catalog per `docs/02_environment_and_branching.md`
(e.g. `aml_dev.bronze.ofac_sdn`, `aml_prod_sim.gold.entity_risk_profile`).

## Bronze — raw, append-only, source schema preserved + ingestion metadata

Every Bronze table carries these ingestion-metadata columns in addition to its source
columns: `_ingested_at` (timestamp), `_source_file` or `_source_offset`, `_schema_version`,
`_batch_id`.

| Table | Source | Grain | Columns (source-native) |
|---|---|---|---|
| `bronze.ofac_sdn` | OFAC `SDN.CSV` | one row per SDN entity | `ent_num, sdn_name, sdn_type, program, title, call_sign, vess_type, tonnage, grt, vess_flag, vess_owner, remarks` |
| `bronze.ofac_alt` | OFAC `ALT.CSV` | one row per alias | `ent_num, alt_num, alt_type, alt_name, alt_remarks` |
| `bronze.ofac_add` | OFAC `ADD.CSV` | one row per address | `ent_num, add_num, address, city_state_zip_etc, country, add_remarks` |
| `bronze.opensanctions_sanctions` | OpenSanctions `sanctions` collection | one row per target entity | `id, schema, name, aliases, birth_date, countries, addresses, identifiers, sanctions, phones, emails, program_ids, dataset, first_seen, last_seen, last_change` |
| `bronze.opensanctions_peps` | OpenSanctions `peps` collection | one row per target entity | same shape as `opensanctions_sanctions` |
| `bronze.txn_accounts` | AMLSim `nodes.csv` | one row per account | `nodeid, isFraud, init_balance, fraudStep` |
| `bronze.txn_events` | AMLSim `transactions.csv`, streamed in micro-batches | one row per transaction | `sourceNodeId, targetNodeId, value, time` |
| `bronze.elliptic_txs_classes` | Elliptic `elliptic_txs_classes.csv` | one row per Bitcoin tx | `txId, class` (`1`=illicit, `2`=licit, `unknown`) |
| `bronze.elliptic_txs_edgelist` | Elliptic `elliptic_txs_edgelist.csv` | one row per tx-to-tx edge | `txId1, txId2` |
| `bronze.elliptic_txs_features` | Elliptic `elliptic_txs_features.csv` | one row per Bitcoin tx | `txId, time_step, features` (165 anonymized values collapsed into one `array<double>` — they're PCA-derived and not individually interpretable, so 165 named columns would be contract noise) |

Null-sentinel policy: OFAC's `-0-` sentinel is preserved as-is in Bronze (raw fidelity) and
converted to real `NULL` only at Bronze→Silver.

Malformed/schema-breaking records go to `bronze.<table>_dead_letter` (same raw columns as
text + `_error_reason`, `_ingested_at`) rather than failing the batch — see Phase 6.

## Silver — normalized, deduped, typed, business keys

| Table | Grain | Key columns | Notes |
|---|---|---|---|
| `silver.entity` | one row per **watchlist** entity | `entity_id` (surrogate), `source_system`, `source_entity_id` | **Watchlist-only** (OFAC + OpenSanctions), always `is_watchlist = true` — deliberately *not* unified with synthetic AMLSim identities, see the design-correction note below. `entity_type` reconciles OFAC's `sdn_type` (blank/individual/vessel/aircraft) and OpenSanctions' `schema` (Person/Organization/...) into one vocabulary: `person`, `organization`, `vessel`, `aircraft`, `other`. `watchlist_program`, `birth_date`, `countries` (array) |
| `silver.entity_alias` | one row per alias | `entity_id`, `alias_name`, `alias_normalized` | Normalized = uppercased, punctuation-stripped, whitespace-collapsed — the actual normalization function is a Phase 3 deliverable |
| `silver.entity_address` | one row per address | `entity_id`, `address_line`, `city`, `country` | Parsed out of OFAC's combined `city_state_zip_etc` and OpenSanctions' free-text `addresses` |
| `silver.account` | one row per account | `account_id` (= AMLSim `nodeid`) | `synthetic_holder_name`, `synthetic_country`, `synthetic_dob` (the account's own fabricated identity attributes from the synthetic identity-linkage step — **not** a FK to `silver.entity`, see below), `init_balance`, `opened_date` (synthetic, derived from the date anchor) |
| `silver.transaction` | one row per transaction | `transaction_id` (surrogate) | `source_account_id`, `target_account_id`, `amount`, `currency` (assumed USD), `event_time` (derived from synthetic date anchor + AMLSim `time` step) |
| `silver.match_candidate` | one row per candidate match | `match_id` | `account_id`, `entity_id` (the watchlist entity), `match_confidence_band` (`exact` / `strong` / `weak`), `match_score`, `matched_on` (name/alias/dob/country), `explanation` (structured — which fields matched and how) |
| `silver.elliptic_txn_risk` | one row per Elliptic tx | `txId` | `class`, `time_step`, `in_degree`, `out_degree` (from `bronze.elliptic_txs_edgelist`), `illicit_neighbor_ratio` (share of 1-hop neighbors labeled illicit — the interpretable signal this dataset can actually offer, since the 165 raw features can't be feature-engineered further). Real SQL self-join/window-function work, standalone graph-topology analytics — not natively joinable to `silver.account`, see the bridge table below. |
| `silver.network_risk_reference` | one row per `account_id` that received a synthetic linkage | `account_id` | `network_risk_exposure` (0-1, sampled from the `illicit_neighbor_ratio` distribution in `silver.elliptic_txn_risk`), `linkage_note` (fixed string documenting this is a simulated bridge, not a real Bitcoin-to-remittance-account linkage) — see caveat below |

Silver-layer responsibilities per the execution plan: timestamp standardization,
deduplication, entity-centric transaction histories, and the fuzzy-matching / confidence
scoring logic (Phase 3/4).

**Design correction (found while implementing, 2026-07-18):** an earlier draft of this
contract put synthetic AMLSim identities directly into `silver.entity` alongside watchlist
entities, with `silver.account.linked_entity_id` as an FK to that unified table. Dropped in
favor of the design above once actually building it, for a concrete reason: `silver.entity`
is rebuilt with `CREATE OR REPLACE TABLE` from OFAC/OpenSanctions Bronze on every run (see
`build_entities.sql`) — injecting synthetic rows into that same table would mean every rerun
either wipes them out or requires a circular self-referencing rebuild. Keeping the account's
synthetic identity as plain columns on `silver.account` avoids that entirely, and is no less
correct: `silver.match_candidate` already carries the account-to-watchlist-entity
relationship, so nothing was actually lost by not routing it through a shared entity table.

**Elliptic integration caveat:** Elliptic's transaction graph (Bitcoin) and AMLSim's account
graph (simulated remittance) share no real join key — there is no actual account that both
datasets describe. `silver.network_risk_reference` is a deliberately-labeled synthetic
bridge (same category of assumption as the entity-identity linkage in
`docs/00_business_charter.md`) that assigns a subset of AMLSim accounts a network-risk score
sampled from Elliptic's real illicit-neighbor-ratio distribution, so the Gold score has a
"network-risk indicator" input to demonstrate as called for in the original brief — but it
is not claiming the Bitcoin graph and the remittance accounts are the same underlying
activity. State this plainly in the README/interview narrative, the same way the identity
linkage is disclosed.

## Gold — business-facing, explainable

| Table | Grain | Key columns | Notes |
|---|---|---|---|
| `gold.entity_risk_profile` | one row per entity/account, per `as_of_date` | `entity_id`, `account_id`, `as_of_date` | `watchlist_match_summary` (struct), behavioral features: `velocity_1d/7d/30d`, `counterparty_diversity_30d`, `burst_flag`, `round_dollar_ratio`, `risky_counterparty_exposure`, `network_risk_exposure` (from `silver.network_risk_reference`, nullable — only populated for accounts that received the synthetic Elliptic linkage); `composite_risk_score`, `score_band`, `top_contributing_factors` (array of struct: factor name, value, weight/contribution) |
| `gold.prioritized_alert_queue` | one row per active alert | `alert_id` | `entity_id`, `account_id`, `priority_rank`, `composite_risk_score`, `escalation_reason` (`sanctions_hard_override` vs. `behavioral_threshold`), `status` (`new`/`reviewed`/`suppressed`), `generated_at` |
| `gold.watchlist_match_summary` | one row per watchlist entity | `watchlist_entity_id` | `matched_account_count`, `highest_confidence_band`, `jurisdictions`, `risk_type` |
| `gold.ops_control` | one row per pipeline batch | `pipeline_name`, `batch_id` | `last_successful_batch_id`, `processed_row_count`, `expected_row_count`, `lag_seconds`, `freshness_status` (`fresh`/`stale`/`degraded`), `dead_letter_count`, `checkpoint_offset` — the silent-failure/ops layer from Phase 6/8 |

`top_contributing_factors` and `escalation_reason` exist specifically to satisfy the audit
requirement in `docs/01_nonfunctional_requirements.md` — every ranked alert must show its
work, not just its score.

## Open items to resolve before Bronze DDL is written for real

- Exact normalization rules for `entity_type` reconciliation (OFAC vocabulary → OpenSanctions
  vocabulary → Silver vocabulary) — draft in Phase 3.
- Confidence-band thresholds for `match_confidence_band` (what score range is `weak` vs.
  `strong` vs. `exact`) — draft in Phase 3, RapidFuzz-based.
- The composite risk-scoring formula and its weights — draft in Phase 5, must stay
  transparent/documented per the explainability requirement.
- Synthetic date anchor for converting AMLSim's `time` step index into real `event_time`
  values.
- Sampling methodology for `silver.network_risk_reference` (which accounts get a synthetic
  Elliptic linkage, and how `illicit_neighbor_ratio` values get assigned to them) — draft
  alongside the identity-linkage step since they're the same category of design problem.
