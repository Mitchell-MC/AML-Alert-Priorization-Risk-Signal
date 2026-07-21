-- Escalation queue by ownership and business impact
SELECT
  business_process,
  impacted_table,
  upstream_owner,
  upstream_contact,
  pipeline_name,
  freshness_status,
  dead_letter_count,
  last_successful_batch_id,
  last_good_batch_age_minutes,
  recorded_at
FROM {{catalog}}.gold.ops_control
WHERE freshness_status != 'fresh'
   OR dead_letter_count > 0
ORDER BY
  CASE freshness_status
    WHEN 'degraded' THEN 1
    WHEN 'stale' THEN 2
    ELSE 3
  END,
  dead_letter_count DESC,
  recorded_at DESC;
