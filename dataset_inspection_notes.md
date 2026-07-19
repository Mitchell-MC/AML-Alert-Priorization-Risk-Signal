# Dataset Inspection Notes (Phase 0 input)

Raw files landed under `raw_data/` (not yet in a git repo, not yet Bronze tables — this is
pre-scaffolding reconnaissance to inform the schema contracts in Phase 0).

Downloaded 2026-07-18. All three sources are point-in-time snapshots — OFAC and
OpenSanctions update continuously, so ingestion timestamps matter for the freshness
metadata called for in Phase 1/2 of the plan.

## 1. OFAC SDN List — `raw_data/ofac/`

Source: `https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/*.CSV`
(the public page redirects here with a signed, time-limited S3 URL — don't hardcode the
signed URL, re-resolve it each ingestion run). License: US government public domain, no
restriction.

**Format gotcha:** headerless CSV, `-0-` is the null sentinel (not empty string), and fields
are comma-quoted with embedded commas/quotes — naive `split(',')` breaks on ~40% of rows
(saw 12–20 raw comma-separated tokens per row against a 12-column schema). Must parse with
a real CSV reader.

| File | Rows (confirmed via real Bronze ingestion) | Columns (undocumented in-file, inferred from known OFAC layout) |
|---|---|---|
| `SDN.CSV` | 19,169 valid + 2 dead-lettered | `ent_num, sdn_name, sdn_type, program, title, call_sign, vess_type, tonnage, grt, vess_flag, vess_owner, remarks` |
| `ALT.CSV` | 20,302 valid + 2 dead-lettered | `ent_num, alt_num, alt_type, alt_name, alt_remarks` — joins to SDN via `ent_num`, one-to-many |
| `ADD.CSV` | 24,955 valid + 2 dead-lettered | `ent_num, add_num, address, city_state_zip_etc, country, add_remarks` — one-to-many |
| `SDN_COMMENTS.CSV` | 33 valid + 2 dead-lettered | `ent_num, remarks_text` — free-text, often contains embedded aliases/tax IDs/directive dates that could be mined but are messy |

**Real dead-letter finding:** every OFAC file's 2 rejected rows are the exact same thing —
`ent_num = ""` (the ASCII SUB/EOF control character, a legacy DOS end-of-file marker).
OFAC's export apparently trails each CSV with this byte, which Spark's CSV reader parses as
one final malformed row. The `ent_num` numeric-validation rule in
`bronze/ingest_ofac.py` correctly quarantines it rather than crashing the job or polluting
the real table — confirmed by actually running the ingestion twice (2 rows = 2 runs, 1 per
run, append-only dead-letter accumulating correctly across runs).

`sdn_type` distribution: 9,838 blank (= organizations/entities), 7,471 `individual`,
1,517 `vessel`, 344 `aircraft`. This maps cleanly onto an entity-type dimension —
individuals and organizations are the two types relevant to customer/watchlist matching;
vessels/aircraft are a separate screening concern we can deprioritize or exclude from the
MVP entity model.

## 2. OpenSanctions — `raw_data/opensanctions/`

Used the `targets.simple.csv` export for two specific collections instead of the full
`default` bundle (which is 2.5–9GB depending on format and includes datasets irrelevant to
this project). License: **CC 4.0 BY-NC — free for non-commercial/portfolio use**, requires
attribution in the README; a paid license would be needed for any commercial reuse.

| Dataset | File | Rows | Size |
|---|---|---|---|
| Consolidated Sanctions | `sanctions_targets_simple.csv` | 71,809 (corrected, see below) | 66.8 MB |
| Politically Exposed Persons | `peps_targets_simple.csv` | 747,331 | 192 MB |

**Row-count correction (found via real Bronze ingestion, 2026-07-18):** the 79,806 figure
above came from a naive `wc -l` count and is wrong. The sanctions CSV has multi-line quoted
fields (embedded literal newlines inside quoted alias/address values), which `wc -l` counts
as extra lines that aren't real records. Spark's CSV reader with `multiLine=true` parses it
correctly and lands exactly 71,809 rows in `bronze.opensanctions_sanctions` -- which matches
OpenSanctions' own reported "71,809 distinct targets" for this collection exactly, and zero
rows were rejected to dead-letter, confirming this is the true count, not a data-loss bug.
Same wc -l trap applies to `SDN_COMMENTS.CSV` below (true count is 33, not the much larger
number naive line-counting suggested).

Shared schema (both files):
`id, schema, name, aliases, birth_date, countries, addresses, identifiers, sanctions, phones, emails, program_ids, dataset, first_seen, last_seen, last_change`

Notes:
- `aliases` is `;`-delimited within the field, and names containing literal quotes/semicolons
  are double-quote-escaped in a way that trips up some CSV readers — validate the parser
  handles RFC 4180 doubled-quote escaping (pandas/Spark `csv` reader with `quote='"'` and
  `escape='"'` handles it; naive regex splitting does not).
- `schema` distinguishes `Organization` vs `Person` (and a few other FollowTheMoney types) —
  same role as OFAC's `sdn_type`, different vocabulary. Reconciling these two type
  vocabularies into one entity-type dimension is a Silver-layer normalization decision worth
  writing down explicitly in the schema contract.
- `first_seen`/`last_seen`/`last_change` give genuine provenance timestamps, better than
  OFAC's SDN files which have none at the row level (only the publication's own
  `Last-Modified` header).
- The PEP file at 747K rows is the largest input by far — worth sampling/filtering
  (e.g. by country or `last_change` recency) rather than loading whole for iterative dev
  work, even though Delta/Spark can handle it fine at full size.

## 3. Transaction event data — `raw_data/transactions/` and `raw_data/elliptic/`

**Correction to an earlier note in this file:** Elliptic was initially assumed to require
authenticated Kaggle API access (a plain `curl` against
`kaggle.com/api/v1/datasets/download/...` 404s without a session) and was skipped in favor
of AMLSim. That assumption was wrong — `kagglehub.dataset_download("ellipticco/elliptic-data-set")`
fetches it anonymously with no credentials needed (installed via `pip install kagglehub`).
Both datasets are now downloaded and inspected; see the comparison/recommendation below for
why AMLSim is still the better primary fit despite Elliptic being available.

### 3a. IBM AMLSim (`raw_data/transactions/`)

License: Apache 2.0. Pulled `20K_fanin200cycle200.tgz` (one of three pre-built samples in
the AMLSim repo, chosen because it mixes two laundering typologies rather than one):

| File | Rows | Schema |
|---|---|---|
| `nodes.csv` | 20,000 accounts (1,803 flagged fraud) | `nodeid, isFraud, init_balance, fraudStep` |
| `transactions.csv` | 120,558 | `sourceNodeId, targetNodeId, value, time` |

`metadata.txt` confirms 200 injected cyclic-laundering patterns and 200 fan-in patterns
among the fraud nodes. `time` is a step index (simulation tick), not a real timestamp — will
need a synthetic date anchor to build the rolling 1d/7d/30d windows the plan calls for.

**Important gap:** pure graph data — no customer names, addresses, or identifiers, so
there's nothing to fuzzy-match against OFAC/OpenSanctions aliases out of the box (see the
synthetic identity-linkage note already in the business charter / schema contracts docs).

Two other sample tarballs exist in the same repo location if more/different typology mix is
wanted later: `20K_cycle200.tgz`, `20K_fanin200.tgz` (same node/edge schema, single typology
each).

### 3b. Elliptic Bitcoin dataset (`raw_data/elliptic/`)

License: per Kaggle listing (`ellipticco/elliptic-data-set`), released by Elliptic for
research use — check the Kaggle page's terms before any non-portfolio reuse. 666MB total,
dominated by the features file.

| File | Rows | Schema |
|---|---|---|
| `elliptic_txs_classes.csv` | 203,769 | `txId, class` — `class` is `1`=illicit (4,545), `2`=licit (42,019), `unknown` (157,205 — 77% of all rows) |
| `elliptic_txs_edgelist.csv` | 234,355 | `txId1, txId2` — directed edge, money flow between transactions |
| `elliptic_txs_features.csv` | 203,769 | headerless: col 1 = `txId`, col 2 = `time_step` (integer 1-49), cols 3-167 = 165 anonymized features (94 "local" tx features + 71 aggregated 1-hop neighborhood features per the original Elliptic paper) |

**Structural difference that matters for this project:** Elliptic's nodes are individual
Bitcoin **transactions**, not accounts — edges represent value flow between transactions
(UTXO graph), not sender→receiver account relationships. There is no dollar amount, no
account/entity concept, and no raw field left to feature-engineer — the 165 features are
already PCA-derived and anonymized by the dataset publisher. AMLSim, by contrast, gives raw
`sourceNodeId, targetNodeId, value, time` on an account graph, which is exactly the raw
material the plan's Phase 4 SQL feature engineering (velocity, counterparty diversity, burst
activity, round-dollar patterns) needs to operate on.

**Recommendation:** keep AMLSim as the primary Bronze/Silver/Gold transaction pipeline
input — it structurally supports the raw feature-engineering work this project is
specifically meant to build (see the growth-area framing in the source PDF: practice
building features from raw event data, not consuming pre-engineered ones). Elliptic is
better suited as a secondary, narrowly-scoped addition — e.g. a graph/network-risk reference
table feeding the "network-risk indicators" component of the Gold composite score, or a
standalone talking point on graph-based AML detection — rather than a swap-in replacement.

**Decision (2026-07-18):** integrate Elliptic as a secondary network-risk signal. Design is
in `docs/03_schema_contracts.md`: `silver.elliptic_txn_risk` computes real graph-topology
features (in/out degree, illicit-neighbor ratio) from the edgelist + classes, and
`silver.network_risk_reference` is an explicitly-labeled synthetic bridge that assigns a
subset of AMLSim accounts a `network_risk_exposure` score sampled from that distribution —
since Elliptic (Bitcoin transactions) and AMLSim (simulated remittance accounts) share no
real join key. `gold.entity_risk_profile.network_risk_exposure` carries it into the
composite score, nullable for accounts that didn't receive the synthetic linkage.

## Implications for Phase 0 contracts

- Entity model needs a **type-reconciliation mapping** between OFAC's `sdn_type` vocabulary
  and OpenSanctions' `schema` vocabulary (Person/Organization as the common denominator for
  MVP; vessels/aircraft out of scope).
- Need a documented **null-sentinel policy** (`-0-` → NULL) applied at Bronze→Silver, not
  left raw.
- Need a **synthetic identity-linkage step** to bridge AMLSim's anonymous account graph to
  named entities before matching logic has anything real to chew on — this is net-new scope
  versus the original plan and should be called out explicitly in the assumptions doc as a
  simulated-data limitation (real screening systems match against actual KYC/customer data).
- Freshness/provenance fields differ per source (OFAC: publication-level only; OpenSanctions:
  row-level `first_seen`/`last_seen`/`last_change`; AMLSim: none, it's static simulation
  output) — the Bronze ingestion metadata contract needs to tolerate sources that don't
  provide row-level timestamps rather than assuming all sources do.
- `raw_data/` is now ~930MB (mostly Elliptic's features file + the PEP CSV) — `.gitignore`d,
  not committed; ingestion code should re-fetch from source, not depend on these files being
  checked in.

## Live execution findings (2026-07-18, once Databricks auth was working)

- **Databricks Free Edition serverless compute has no outbound internet egress.** A first
  version of the Bronze ingestion jobs downloaded source files from inside the job itself
  (`urllib.request.urlopen`) and failed on a real run with `URLError: <urlopen error [Errno
  -3] Temporary failure in name resolution>`. Fixed by splitting the pipeline:
  `scripts/stage_raw_files.py` runs *outside* Databricks (locally or in CI) to fetch and
  upload raw files into the Unity Catalog Volume via `databricks fs cp`; the Bronze jobs only
  read already-staged files. This is now the permanent architecture, not a workaround --
  documented in `docs/02_environment_and_branching.md`.
- All 4 Bronze sources ran successfully end-to-end on the real `aml_dev` catalog after that
  fix: OFAC (19,169/20,302/24,955/33 rows across the 4 files), OpenSanctions (71,809
  sanctions + 747,331 PEPs), AMLSim (20,000 accounts), Elliptic (203,769 classes / 234,355
  edges / 203,769 feature rows -- exact match to local inspection, no surprises there since
  it already had a real header row and no embedded-newline issue).
- Git Bash mangles `databricks api post /api/2.0/...` paths into Windows filesystem paths
  (MSYS auto path-conversion) -- need `MSYS_NO_PATHCONV=1` in the environment for any
  `databricks api` / `databricks fs` invocation from this shell.

## Not yet done

- Git repo is initialized and `docs/00-03` are locked (business charter, NFRs,
  environment/branching, schema contracts). Bronze is live and verified on `aml_dev` (see
  above); Silver/Gold are not built yet.
- Elliptic is downloaded, inspected, and Bronze-ingested. Its Silver integration
  (`silver.elliptic_txn_risk` / `silver.network_risk_reference`) is designed in
  `docs/03_schema_contracts.md` but not yet built.
