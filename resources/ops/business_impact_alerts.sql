-- Actionable business-impact alerts from gold.ops_control
SELECT
  pipeline_name,
  business_process,
  impacted_table,
  freshness_status,
  last_good_batch_age_minutes,
  last_successful_batch_id,
  upstream_owner,
  upstream_contact,
  recorded_at
FROM {{catalog}}.gold.ops_control
WHERE freshness_status != 'fresh'
   OR dead_letter_count > 0
ORDER BY recorded_at DESC;
