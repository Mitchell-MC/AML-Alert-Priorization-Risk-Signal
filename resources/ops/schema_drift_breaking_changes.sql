-- Latest breaking schema drift events
WITH latest_per_table AS (
  SELECT
    pipeline_name,
    table_name,
    schema_hash,
    drift_json,
    is_breaking,
    observed_at,
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY observed_at DESC) AS rn
  FROM {{catalog}}.gold.ops_schema_drift
)
SELECT
  pipeline_name,
  table_name,
  schema_hash,
  drift_json,
  observed_at
FROM latest_per_table
WHERE rn = 1
  AND is_breaking = true
ORDER BY observed_at DESC;
