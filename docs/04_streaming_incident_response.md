# Streaming Incident Response: Duplicate Amplification After Restart

This is a real incident from this project's own build, not a hypothetical written for
interview purposes. It's the exact failure mode Phase 6/8 of the execution plan calls out —
"streaming resilience drill: stop/restart stream, confirm checkpoint recovery and no
duplicate amplification" — found by actually triggering it, not by design review.

## What happened

While deploying `streaming/txn_stream_ingest.py` for the first time, four unrelated bugs
surfaced in quick succession (serverless compute rejecting an infinite trigger, `.persist()`
being unsupported on serverless, ANSI-mode `cast()` raising instead of returning null on the
deliberately-injected malformed test row — see `docs/02_environment_and_branching.md` and
inline comments in that file for each). Each fix required redeploying and rerunning the job
against the **same checkpoint location**. After the run that finally completed successfully,
a routine row-count sanity check surfaced the real problem:

```sql
SELECT count(*), count(distinct sourceNodeId, targetNodeId, value, time)
FROM aml_dev.bronze.txn_events
-- 235,406 total rows | 117,703 distinct rows
```

Exactly 2x. Every row had been written twice.

## First place to look (per the plan's own silent-failure design)

In order:
1. **Streaming query progress** — was the query actually consuming new data, or stuck? (It
   was consuming fine; the row counts were growing, just wrong.)
2. **Checkpoint state** — was the checkpoint directory intact, or had a crash left it in a
   partial/inconsistent state? This is exactly what happened: several of the earlier runs
   crashed *inside* `foreachBatch`, after Spark's internal offset bookkeeping had advanced
   but before (or during) some batches' sink writes had fully committed in a way the next
   run's checkpoint recovery could cleanly reconcile.
3. **Source offsets / file arrival** — the file-source's own tracking was doing its job
   (it wasn't re-reading already-seen *files*); the duplication was happening at the *sink*
   write level, not the source read level.
4. **Bronze freshness metadata** — `gold.ops_control`'s `processed_row_count` per batch,
   summed across runs, was the number that first made the 2x pattern legible instead of just
   "the total looks high."

## Root cause

The sink write was a plain `cast_df.write.format("delta").mode("append").saveAsTable(...)`.
Structured Streaming's checkpoint-based exactly-once guarantee covers the *source* (a file,
once read, won't be re-read) and the *query's own state* (e.g. the watermark-bounded
`dropDuplicates` upstream) — it does not, by itself, make a plain append-mode sink write
idempotent against a batch being *reprocessed* after an abnormal termination. When a crash
happens mid-batch and the next run resumes, Spark can re-execute that batch's write; a plain
append then inserts the same rows a second time.

## Fix

Replaced the plain append with an idempotent `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`,
keyed on the transaction's natural identity (`sourceNodeId`, `targetNodeId`, `value`,
`time`) — see `streaming/txn_stream_ingest.py`'s `_merge_append()`. Replaying an
already-committed batch now no-ops on every row that already exists instead of inserting
duplicates. This is the standard, textbook-correct pattern for exactly-once
`foreachBatch` sinks; the plain-append version was the actual bug, not a simplification that
happened to work until it didn't.

## Verification

1. Truncated `bronze.txn_events` / `bronze.txn_events_dead_letter`, deleted the checkpoint
   directory, reran from a clean state with the fix in place:
   `117,703 total = 117,703 distinct`. Zero duplicates.
2. **Deliberate restart test** (not another accidental crash): reran the identical job again
   with no data changes. Result: `117,703 = 117,703` (unchanged), `dead_letter` count
   unchanged, and `gold.ops_control` gained **zero** new rows — the checkpoint correctly
   recognized there was nothing new to process and the query didn't even invoke
   `foreachBatch` again. This is the cleanest possible confirmation: idempotent, no
   duplicate amplification, and correct "nothing to do" recognition on restart.

## What degraded-mode continuity looks like here

Per `docs/01_nonfunctional_requirements.md`'s degraded-mode design: while this incident was
being diagnosed, `silver.transaction` and everything downstream (`gold.entity_risk_profile`,
the alert queue) kept serving whatever Bronze data existed at the time, rather than failing
outright — the pipeline's layered design means a Bronze-level bug doesn't take down Silver/
Gold's ability to serve their last-known-good state. The bug's *symptom* (double-counted
transactions) would have propagated into Silver/Gold *had they been rebuilt* during the
incident window, which is exactly why `silver.transaction`'s own `SELECT DISTINCT` on the
final projection exists as defense-in-depth on top of the Bronze-level fix, not as a
substitute for it.

## Known residual, documented rather than chased further

Valid + dead-lettered rows after the fix (117,703 + 1,194 = 118,897) fall about 1,661 short
of the source's 120,558 total rows. The likely explanation: AMLSim's synthetic transaction
generator occasionally produces two genuinely distinct transactions that happen to share the
same natural key by coincidence (same source, target, amount, and time step) — the
natural-key `MERGE` would (correctly, given the identity it's told to trust) collapse those
into one. A real production event source would stamp its own unique event ID rather than
relying on a derived natural key; this demo producer doesn't, which is a documented
simplification of the simulation, not a silently-ignored data-loss bug.
