-- Gold layer: explainable, rule-based risk scoring and the prioritized alert queue. See
-- docs/03_schema_contracts.md and docs/00_business_charter.md's escalation-threshold
-- assumption (exact sanctions match = hard override regardless of behavioral score).
--
-- Scoring policy v1 -- simple, additive, and fully traceable to a named reason (not a
-- trained/weighted model), per the explainability requirement in
-- docs/01_nonfunctional_requirements.md. Every point on composite_risk_score corresponds to
-- exactly one row in top_contributing_factors. Thresholds below are deliberately simple
-- round numbers, documented as v1 defaults to tune later against real investigator feedback
-- (which this portfolio project has no way to actually collect) -- not claimed to be
-- statistically optimized.
--
-- {catalog} is substituted by the calling job.

CREATE OR REPLACE TABLE {catalog}.gold.entity_risk_profile AS
WITH as_of AS (
  SELECT MAX(event_date) AS as_of_date FROM {catalog}.silver.transaction
),
params AS (
  SELECT as_of_date FROM as_of
),
account_txns AS (
  -- one row per (account, transaction) regardless of whether the account sent or received --
  -- both directions count toward an account's own activity-volume features.
  SELECT source_account_id AS account_id, target_account_id AS counterparty_id, amount, event_time, event_date
  FROM {catalog}.silver.transaction
  UNION ALL
  SELECT target_account_id AS account_id, source_account_id AS counterparty_id, amount, event_time, event_date
  FROM {catalog}.silver.transaction
),
features AS (
  SELECT
    account_id,
    COUNT(CASE WHEN event_time >= p.as_of_date - INTERVAL 1 DAY THEN 1 END) AS velocity_1d,
    COUNT(CASE WHEN event_time >= p.as_of_date - INTERVAL 7 DAY THEN 1 END) AS velocity_7d,
    COUNT(CASE WHEN event_time >= p.as_of_date - INTERVAL 30 DAY THEN 1 END) AS velocity_30d,
    COUNT(DISTINCT CASE WHEN event_time >= p.as_of_date - INTERVAL 30 DAY THEN counterparty_id END) AS counterparty_diversity_30d,
    SUM(CASE WHEN amount = ROUND(amount, 0) AND event_time >= p.as_of_date - INTERVAL 30 DAY THEN 1 ELSE 0 END) AS round_dollar_count_30d,
    COUNT(CASE WHEN event_time >= p.as_of_date - INTERVAL 30 DAY THEN 1 END) AS total_30d
  FROM account_txns
  CROSS JOIN params p
  GROUP BY account_id
),
daily_counts AS (
  SELECT account_id, event_date AS txn_date, COUNT(*) AS daily_count
  FROM account_txns
  CROSS JOIN params p
  WHERE event_date >= p.as_of_date - INTERVAL 30 DAY
  GROUP BY account_id, event_date
),
burst AS (
  -- burst_flag: any single day in the last 30 has more than 3x the account's own 30-day
  -- daily average -- a self-relative threshold, not a fixed count, so it works the same way
  -- for a quiet account and a naturally high-volume one.
  SELECT account_id, MAX(daily_count) AS max_daily_count, AVG(daily_count) AS avg_daily_count
  FROM daily_counts
  GROUP BY account_id
),
risky_accounts AS (
  -- accounts that are themselves a strong/exact watchlist match -- used below to flag *other*
  -- accounts that transact with them (risky_counterparty_exposure), not to flag themselves.
  SELECT DISTINCT account_id
  FROM {catalog}.silver.match_candidate
  WHERE match_confidence_band IN ('exact', 'strong')
),
counterparty_exposure AS (
  SELECT /*+ BROADCAST(r) */ DISTINCT t.account_id
  FROM account_txns t
  JOIN risky_accounts r ON t.counterparty_id = r.account_id
),
watchlist_summary AS (
  SELECT
    account_id,
    MAX(CASE
      WHEN match_confidence_band = 'exact' THEN 3
      WHEN match_confidence_band = 'strong' THEN 2
      WHEN match_confidence_band = 'weak' THEN 1
      ELSE 0
    END) AS best_band_rank,
    COUNT(*) AS match_count
  FROM {catalog}.silver.match_candidate
  GROUP BY account_id
),
joined AS (
  SELECT /*+ BROADCAST(n), BROADCAST(ws) */
    a.account_id AS entity_id,
    a.account_id,
    p.as_of_date,
    COALESCE(f.velocity_1d, 0) AS velocity_1d,
    COALESCE(f.velocity_7d, 0) AS velocity_7d,
    COALESCE(f.velocity_30d, 0) AS velocity_30d,
    COALESCE(f.counterparty_diversity_30d, 0) AS counterparty_diversity_30d,
    CASE WHEN COALESCE(f.total_30d, 0) > 0 THEN f.round_dollar_count_30d / f.total_30d ELSE 0.0 END AS round_dollar_ratio,
    COALESCE(b.max_daily_count > 3 * b.avg_daily_count, false) AS burst_flag,
    (ce.account_id IS NOT NULL) AS risky_counterparty_exposure,
    n.network_risk_exposure,
    COALESCE(ws.best_band_rank, 0) AS best_band_rank,
    COALESCE(ws.match_count, 0) AS watchlist_match_count
  FROM {catalog}.silver.account a
  CROSS JOIN params p
  LEFT JOIN features f ON a.account_id = f.account_id
  LEFT JOIN burst b ON a.account_id = b.account_id
  LEFT JOIN counterparty_exposure ce ON a.account_id = ce.account_id
  LEFT JOIN {catalog}.silver.network_risk_reference n ON a.account_id = n.account_id
  LEFT JOIN watchlist_summary ws ON a.account_id = ws.account_id
),
scored AS (
  SELECT
    *,
    -- points, one CASE per named factor -- kept individually so top_contributing_factors
    -- below can report the exact same numbers back, not a recomputation.
    CASE WHEN best_band_rank = 3 THEN 50 WHEN best_band_rank = 2 THEN 30 WHEN best_band_rank = 1 THEN 10 ELSE 0 END AS pts_watchlist,
    CASE WHEN velocity_7d > 20 THEN 10 ELSE 0 END AS pts_velocity,
    CASE WHEN counterparty_diversity_30d > 15 THEN 10 ELSE 0 END AS pts_diversity,
    CASE WHEN round_dollar_ratio > 0.5 THEN 10 ELSE 0 END AS pts_round_dollar,
    CASE WHEN burst_flag THEN 15 ELSE 0 END AS pts_burst,
    CASE WHEN risky_counterparty_exposure THEN 20 ELSE 0 END AS pts_risky_counterparty,
    CASE WHEN network_risk_exposure IS NOT NULL AND network_risk_exposure > 0.5 THEN 15 ELSE 0 END AS pts_network_risk
  FROM joined
),
final_scored AS (
  SELECT
    *,
    LEAST(
      100,
      pts_watchlist + pts_velocity + pts_diversity + pts_round_dollar + pts_burst + pts_risky_counterparty + pts_network_risk
    ) AS composite_risk_score
  FROM scored
)
SELECT
  entity_id,
  account_id,
  as_of_date,
  velocity_1d,
  velocity_7d,
  velocity_30d,
  counterparty_diversity_30d,
  round_dollar_ratio,
  burst_flag,
  risky_counterparty_exposure,
  network_risk_exposure,
  best_band_rank,
  watchlist_match_count,
  composite_risk_score,
  CASE
    WHEN composite_risk_score >= 70 THEN 'high'
    WHEN composite_risk_score >= 40 THEN 'medium'
    ELSE 'low'
  END AS score_band,
  filter(
    array(
      CASE WHEN pts_watchlist > 0 THEN named_struct('factor', 'watchlist_match', 'band', CASE WHEN best_band_rank = 3 THEN 'exact' WHEN best_band_rank = 2 THEN 'strong' ELSE 'weak' END, 'points', pts_watchlist) END,
      CASE WHEN pts_velocity > 0 THEN named_struct('factor', 'high_velocity_7d', 'band', CAST(velocity_7d AS STRING), 'points', pts_velocity) END,
      CASE WHEN pts_diversity > 0 THEN named_struct('factor', 'high_counterparty_diversity_30d', 'band', CAST(counterparty_diversity_30d AS STRING), 'points', pts_diversity) END,
      CASE WHEN pts_round_dollar > 0 THEN named_struct('factor', 'high_round_dollar_ratio_30d', 'band', CAST(round(round_dollar_ratio, 2) AS STRING), 'points', pts_round_dollar) END,
      CASE WHEN pts_burst > 0 THEN named_struct('factor', 'burst_activity', 'band', 'flagged', 'points', pts_burst) END,
      CASE WHEN pts_risky_counterparty > 0 THEN named_struct('factor', 'risky_counterparty_exposure', 'band', 'flagged', 'points', pts_risky_counterparty) END,
      CASE WHEN pts_network_risk > 0 THEN named_struct('factor', 'elliptic_network_risk_exposure', 'band', CAST(round(network_risk_exposure, 2) AS STRING), 'points', pts_network_risk) END
    ),
    x -> x IS NOT NULL
  ) AS top_contributing_factors
FROM final_scored;


CREATE OR REPLACE TABLE {catalog}.gold.prioritized_alert_queue AS
SELECT
  sha2(concat(entity_id, '-', CAST(as_of_date AS STRING)), 256) AS alert_id,
  entity_id,
  account_id,
  RANK() OVER (ORDER BY composite_risk_score DESC) AS priority_rank,
  composite_risk_score,
  -- sanctions_hard_override: an exact watchlist match escalates regardless of behavioral
  -- score, per the escalation-threshold assumption in docs/00_business_charter.md.
  CASE WHEN best_band_rank = 3 THEN 'sanctions_hard_override' ELSE 'behavioral_threshold' END AS escalation_reason,
  'new' AS status,
  current_timestamp() AS generated_at
FROM {catalog}.gold.entity_risk_profile
WHERE best_band_rank = 3 OR composite_risk_score >= 40;


CREATE OR REPLACE TABLE {catalog}.gold.watchlist_match_summary AS
WITH ranked AS (
  SELECT
    m.entity_id AS watchlist_entity_id,
    m.account_id,
    CASE
      WHEN m.match_confidence_band = 'exact' THEN 3
      WHEN m.match_confidence_band = 'strong' THEN 2
      ELSE 1
    END AS band_rank
  FROM {catalog}.silver.match_candidate m
),
agg AS (
  SELECT
    watchlist_entity_id,
    COUNT(DISTINCT account_id) AS matched_account_count,
    MAX(band_rank) AS highest_band_rank
  FROM ranked
  GROUP BY watchlist_entity_id
)
SELECT
  a.watchlist_entity_id,
  a.matched_account_count,
  CASE WHEN a.highest_band_rank = 3 THEN 'exact' WHEN a.highest_band_rank = 2 THEN 'strong' ELSE 'weak' END AS highest_confidence_band,
  e.countries AS jurisdictions,
  e.entity_type AS risk_type
FROM agg a
JOIN {catalog}.silver.entity e ON a.watchlist_entity_id = e.entity_id;


CREATE OR REPLACE VIEW {catalog}.gold.month_end_reconciliation_serving AS
WITH latest_publish AS (
  SELECT
    batch_id AS as_of_batch_id,
    freshness_status,
    COALESCE(last_good_batch_age_minutes, lag_seconds / 60.0) AS last_good_batch_age_minutes,
    recorded_at
  FROM {catalog}.gold.ops_control
  WHERE pipeline_name = 'gold_month_end_publish'
  ORDER BY recorded_at DESC
  LIMIT 1
)
SELECT
  p.*,
  lp.as_of_batch_id,
  COALESCE(lp.freshness_status, 'degraded') != 'fresh' AS is_stale,
  lp.last_good_batch_age_minutes,
  lp.recorded_at AS serving_metadata_recorded_at
FROM {catalog}.gold.entity_risk_profile p
CROSS JOIN latest_publish lp;
