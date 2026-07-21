-- Cost and SLA scorecard (last 7 days)
SELECT
  pipeline_name,
  business_process,
  COUNT(*) AS run_count,
  AVG(runtime_seconds) AS avg_runtime_seconds,
  MAX(runtime_seconds) AS max_runtime_seconds,
  AVG(estimated_cloud_cost_usd) AS avg_estimated_cost_usd,
  SUM(estimated_cloud_cost_usd) AS total_estimated_cost_usd,
  AVG(last_good_batch_age_minutes) AS avg_last_good_batch_age_minutes,
  MAX(last_good_batch_age_minutes) AS max_last_good_batch_age_minutes,
  SUM(dead_letter_count) AS total_dead_letter_count,
  SUM(CASE WHEN freshness_status = 'fresh' THEN 1 ELSE 0 END) AS fresh_runs,
  SUM(CASE WHEN freshness_status = 'stale' THEN 1 ELSE 0 END) AS stale_runs,
  SUM(CASE WHEN freshness_status = 'degraded' THEN 1 ELSE 0 END) AS degraded_runs
FROM {{catalog}}.gold.ops_control
WHERE recorded_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY pipeline_name, business_process
ORDER BY total_estimated_cost_usd DESC, degraded_runs DESC;
