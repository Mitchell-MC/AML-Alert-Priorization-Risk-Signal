-- Silver transaction history: standardizes bronze.txn_events into silver.transaction.
-- See docs/03_schema_contracts.md. transaction_id is a stable hash of the natural key
-- (source/target/value/time) -- the same key bronze/streaming/txn_stream_ingest.py's
-- idempotent MERGE uses, so a transaction's identity is consistent end to end.
--
-- SELECT DISTINCT on the final projection is a deliberate defense-in-depth dedup, not
-- decoration: it collapses any residual duplicate rows down to one, on top of (not instead
-- of) the Bronze-layer idempotent MERGE fix documented in txn_stream_ingest.py after a real
-- duplicate-amplification finding.
--
-- {catalog} is substituted by the calling job.

CREATE OR REPLACE TABLE {catalog}.silver.transaction
PARTITIONED BY (event_date)
AS
SELECT DISTINCT
  sha2(
    concat_ws('-', CAST(sourceNodeId AS STRING), CAST(targetNodeId AS STRING), CAST(value AS STRING), CAST(time AS STRING)),
    256
  ) AS transaction_id,
  CAST(sourceNodeId AS STRING) AS source_account_id,
  CAST(targetNodeId AS STRING) AS target_account_id,
  value AS amount,
  'USD' AS currency,
  event_time,
  DATE(event_time) AS event_date
FROM {catalog}.bronze.txn_events;
