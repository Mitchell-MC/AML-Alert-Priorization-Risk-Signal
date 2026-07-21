-- Month-end readiness snapshot
WITH latest_publish AS (
  SELECT *
  FROM {{catalog}}.gold.ops_control
  WHERE pipeline_name = 'gold_month_end_publish'
  ORDER BY recorded_at DESC
  LIMIT 1
),
latest_serving AS (
  SELECT
    MAX(serving_metadata_recorded_at) AS serving_metadata_recorded_at,
    MAX(as_of_batch_id) AS as_of_batch_id,
    MAX(CASE WHEN is_stale THEN 1 ELSE 0 END) AS is_stale_int,
    MAX(last_good_batch_age_minutes) AS last_good_batch_age_minutes
  FROM {{catalog}}.gold.month_end_reconciliation_serving
)
SELECT
  lp.pipeline_name,
  lp.business_process,
  lp.impacted_table,
  lp.freshness_status,
  lp.last_successful_batch_id,
  lp.last_good_batch_age_minutes,
  lp.recorded_at AS publish_recorded_at,
  ls.serving_metadata_recorded_at,
  ls.as_of_batch_id,
  CAST(ls.is_stale_int AS BOOLEAN) AS serving_is_stale,
  ls.last_good_batch_age_minutes AS serving_last_good_batch_age_minutes,
  lp.upstream_owner,
  lp.upstream_contact
FROM latest_publish lp
CROSS JOIN latest_serving ls;
