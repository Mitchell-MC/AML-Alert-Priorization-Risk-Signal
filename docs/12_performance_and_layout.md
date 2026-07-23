# Performance & Physical Layout

How this project keeps reads and writes fast and cheap: table layout (partitioning /
liquid clustering), file compaction, engine-tier discipline, and history retention. The
governing constraint is that the project runs on **Databricks Free Edition serverless**, which
*auto-manages* several classic tuning knobs and *forbids* others — so half the standard advice
is either automatic or unavailable here. Each lever below states **what the project does**,
**how it behaves on serverless**, and **how it would behave / what you'd tune on a classic
(dedicated/provisioned) cluster**, since that difference is exactly the judgment an interview
probes.

## Serverless vs. classic: the knob-by-knob verdict

| Lever | Project stance | Serverless (this project) | Classic / dedicated cluster |
|---|---|---|---|
| **Shuffle partition count** (`spark.sql.shuffle.partitions`) | Not set | AQE auto-coalesces post-shuffle partitions; `spark.conf` largely restricted | You'd size it ~2–4× cores or ~128–250MB/partition, and re-check for spill vs. tiny tasks |
| **AQE** (adaptive query execution) | Relied on, not toggled | On by default; handles skew-join splitting + partition coalescing | On by default in modern DBR too; you *can* disable/tune it, and pair with salting if AQE can't split a hot key cleanly |
| **Skew handling / salting** | Not implemented | AQE skew-join handles the common case automatically | You'd salt hot keys manually when AQE can't, then un-salt after the wide op |
| **Broadcast joins** | **Explicit `BROADCAST(...)` hints** in `build_gold.sql` and `build_elliptic_risk.sql` | Hints honored; AQE also auto-broadcasts small sides | Same hints apply; more important on classic where you may also tune `autoBroadcastJoinThreshold` |
| **DataFrame `cache`/`persist`** | Not used (documented) | **Forbidden** — `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE`; SQL-layer result cache still applies | Supported — you'd `persist → materialize → reuse → unpersist` a slice reused across multiple downstream steps |
| **Repartition before wide op / coalesce after** | Not used | AQE reshapes; the Python matching sidesteps Spark shuffles by working on driver-collected, bounded inputs | You'd `repartition(key)` once before a big join and `coalesce()` after a big filter to control file counts |
| **Partitioning / liquid clustering** | **Applied** (below) | Fully supported and load-bearing | Same |
| **OPTIMIZE / auto-compaction / file sizing** | **Applied** (maintenance job + table properties) | Predictive optimization *may* auto-compact managed tables; job is belt-and-suspenders | **Load-bearing** — auto-compaction not guaranteed; the scheduled `OPTIMIZE` is the primary defense against small files |
| **VACUUM / time-travel retention** | **Applied** (default 7-day retain) | Supported | Same |
| **UDF tier** (built-in > SQL > pandas UDF > Python UDF) | **Built-in only** on the hot path (below) | Python UDFs pay a per-row serialization round-trip either way | Same hierarchy; UDF cost is if anything higher on a busy shared cluster |
| **Cluster sizing / autoscaling** | N/A | Serverless scales to zero between runs — no node cap to tune | You'd right-size the cluster and set autoscale min/max and idle-termination |

**Bottom line:** the levers that don't apply here (shuffle partitions, salting, DataFrame
caching, cluster sizing) are auto-managed or forbidden on serverless — not oversights. The
levers that *do* apply on serverless are physical layout, compaction, vacuum, and engine-tier
discipline, and those are implemented below.

## Table layout

The rule (per the "data shape" guidance): **partition low-cardinality** keys where range
pruning dominates; use **liquid clustering** on the high-cardinality columns you actually
filter/join by; **don't over-partition** into directory explosion.

| Table | Layout | Rationale |
|---|---|---|
| `silver.transaction` | `PARTITIONED BY (event_date)` | Classic time-series fact; date-range pruning is the dominant access pattern. Bounded, low-cardinality key — a safe partition, not directory confetti |
| `gold.entity_risk_profile` | `CLUSTER BY (score_band, account_id)` | Dashboards filter by `score_band` then drill to `account_id` |
| `gold.prioritized_alert_queue` | `CLUSTER BY (status, escalation_reason)` | `status` drives the row filter (`resources/ddl/01_ownership_and_access.sql`); both are triage filters |
| `bronze.txn_events` | `CLUSTER BY (sourceNodeId, targetNodeId)` (via maintenance job) | Highest-churn table — streamed MERGE appends; the natural-key columns are the join/dedup keys |
| `silver.account` | `CLUSTER BY (account_id)` (via maintenance job) | Joined by `account_id` everywhere |
| `silver.match_candidate` | `CLUSTER BY (account_id)` (via maintenance job) | Joined/filtered by `account_id` |
| `silver.network_risk_reference` | `CLUSTER BY (account_id)` (via maintenance job) | Joined by `account_id` |

**Why some inline and some via the maintenance job:** tables built with `CREATE OR REPLACE`
(the Gold tables, `silver.transaction`) must declare clustering/partitioning **inline in the
CTAS**, because a rebuild drops any spec set by a later `ALTER`. Tables created by DataFrame
`mode("overwrite")` or the streaming MERGE keep their table metadata across data writes, so
the maintenance job sets their clustering **once** via `ALTER TABLE ... CLUSTER BY` and it
persists.

## File compaction & sizing

Every high-volume table sets `delta.targetFileSize = 128MB` plus `optimizeWrite` /
`autoCompact` properties, and the **`aml_table_maintenance` job** (daily,
`resources/jobs/table_maintenance_job.yml` → `src/aml_lakehouse/maintenance/run_table_maintenance.py`)
runs `OPTIMIZE` (which also reclusters liquid-clustered tables) and `VACUUM`. This is the
direct defense against the "file confetti" failure mode — most acute on `bronze.txn_events`,
which the streaming consumer appends to every micro-batch.

## History retention & the time-travel dependency

`resources/ops/day_over_day_variance.sql` answers "why did today's number change?" by reading
`VERSION AS OF` a prior Delta version of the Gold tables (time travel). The maintenance
`VACUUM` therefore uses the **default 7-day (168h) retention** — deliberately larger than the
day-over-day window it must preserve. **Do not lower it** below ~48h or the variance query
silently loses its comparison point. This is the one place layout maintenance and the ops
tooling are coupled, and it's intentional.

## Engine-tier discipline (built-in > SQL > pandas UDF > Python UDF)

The streaming consumer previously mapped AMLSim's step index to `event_time` with a per-row
Python `@F.udf` — the slowest tier, paying a Python serialization round-trip for every
streamed transaction. It is now a **built-in expression** (`timestamp_seconds(...)`,
`common/time_anchor.py::step_to_event_time_expr`) that stays in Catalyst/Photon. The Python
scalar `step_to_event_time()` is kept only as the tested single-source-of-truth for the
mapping; `tests/test_time_anchor.py` proves the built-in expression is row-for-row identical.
This applies equally on serverless and classic — a Python UDF is a per-row cost on any engine.

## Deliberate non-optimizations

- **Driver-collected fuzzy matching** (`silver/build_accounts_and_matches.py`): the rapidfuzz
  matching runs on bounded (tens-of-thousands) driver-collected inputs rather than distributed
  Spark, documented as fine at this volume with `mapInPandas` as the scale-out path. A
  deliberate call, not a missed optimization.
- **No manual shuffle/skew tuning:** left to AQE, per the table above.
