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
--
-- Table-driven scoring policy (docs/13_table_driven_config.md): every weight, threshold, and
-- band cutoff lives in gold.scoring_rule (an SCD2 table) instead of being hard-coded below,
-- so tuning the policy is a table change with begin/end-dated lineage -- directly serving the
-- audit requirement in docs/01 ("which weights were in effect at decision time"). The seed
-- values are the exact v1 literals the CASE expressions used before, so results are unchanged.
-- Create + seed is idempotent (IF NOT EXISTS + insert-only-when-empty) so it never wipes SCD2
-- history on a rebuild.

CREATE TABLE IF NOT EXISTS {catalog}.gold.scoring_rule (
  rule_key STRING,
  rule_value DOUBLE,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN,
  description STRING
);

INSERT INTO {catalog}.gold.scoring_rule
SELECT * FROM (VALUES
  ('pts_watchlist_exact', 50, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: exact watchlist match'),
  ('pts_watchlist_strong', 30, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: strong watchlist match'),
  ('pts_watchlist_weak', 10, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: weak watchlist match'),
  ('pts_velocity', 10, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: high 7d velocity'),
  ('pts_diversity', 10, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: high 30d counterparty diversity'),
  ('pts_round_dollar', 10, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: high round-dollar ratio'),
  ('pts_burst', 15, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: burst activity'),
  ('pts_risky_counterparty', 20, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: risky counterparty exposure'),
  ('pts_network_risk', 15, DATE'2026-01-01', CAST(NULL AS DATE), true, 'points: elliptic network-risk exposure'),
  ('thr_velocity_7d', 20, DATE'2026-01-01', CAST(NULL AS DATE), true, 'threshold: velocity_7d strictly above'),
  ('thr_diversity_30d', 15, DATE'2026-01-01', CAST(NULL AS DATE), true, 'threshold: counterparty_diversity_30d strictly above'),
  ('thr_round_dollar_ratio', 0.5, DATE'2026-01-01', CAST(NULL AS DATE), true, 'threshold: round_dollar_ratio strictly above'),
  ('thr_network_risk', 0.5, DATE'2026-01-01', CAST(NULL AS DATE), true, 'threshold: network_risk_exposure strictly above'),
  ('thr_burst_multiplier', 3, DATE'2026-01-01', CAST(NULL AS DATE), true, 'threshold: max daily count > N x avg daily count'),
  ('score_cap', 100, DATE'2026-01-01', CAST(NULL AS DATE), true, 'composite score ceiling'),
  ('band_high_min', 70, DATE'2026-01-01', CAST(NULL AS DATE), true, 'score_band high: composite >= this'),
  ('band_medium_min', 40, DATE'2026-01-01', CAST(NULL AS DATE), true, 'score_band medium: composite >= this'),
  ('alert_min_score', 40, DATE'2026-01-01', CAST(NULL AS DATE), true, 'alert queue inclusion: composite >= this')
) AS v(rule_key, rule_value, valid_from, valid_to, is_current, description)
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.gold.scoring_rule);

-- Liquid clustering on the columns dashboards actually filter/drill by (score_band, then
-- account_id), plus explicit file sizing so OPTIMIZE targets ~128MB files. CLUSTER BY must be
-- inline in the CTAS because CREATE OR REPLACE rebuilds the table every run and would drop a
-- clustering spec set via a later ALTER. See docs/12_performance_and_layout.md.
CREATE OR REPLACE TABLE {catalog}.gold.entity_risk_profile
CLUSTER BY (score_band, account_id)
TBLPROPERTIES (
  'delta.targetFileSize' = '134217728',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
WITH as_of AS (
  SELECT MAX(event_date) AS as_of_date FROM {catalog}.silver.transaction
),
params AS (
  SELECT as_of_date FROM as_of
),
-- current scoring policy, pivoted from the SCD2 gold.scoring_rule into a single row of scalar
-- values that the scoring CTEs cross-join against. Aggregation with no GROUP BY always yields
-- exactly one row, so the cross-join can never multiply the fact rows.
rules AS (
  SELECT
    MAX(CASE WHEN rule_key = 'pts_watchlist_exact' THEN rule_value END) AS pts_watchlist_exact,
    MAX(CASE WHEN rule_key = 'pts_watchlist_strong' THEN rule_value END) AS pts_watchlist_strong,
    MAX(CASE WHEN rule_key = 'pts_watchlist_weak' THEN rule_value END) AS pts_watchlist_weak,
    MAX(CASE WHEN rule_key = 'pts_velocity' THEN rule_value END) AS pts_velocity,
    MAX(CASE WHEN rule_key = 'pts_diversity' THEN rule_value END) AS pts_diversity,
    MAX(CASE WHEN rule_key = 'pts_round_dollar' THEN rule_value END) AS pts_round_dollar,
    MAX(CASE WHEN rule_key = 'pts_burst' THEN rule_value END) AS pts_burst,
    MAX(CASE WHEN rule_key = 'pts_risky_counterparty' THEN rule_value END) AS pts_risky_counterparty,
    MAX(CASE WHEN rule_key = 'pts_network_risk' THEN rule_value END) AS pts_network_risk,
    MAX(CASE WHEN rule_key = 'thr_velocity_7d' THEN rule_value END) AS thr_velocity_7d,
    MAX(CASE WHEN rule_key = 'thr_diversity_30d' THEN rule_value END) AS thr_diversity_30d,
    MAX(CASE WHEN rule_key = 'thr_round_dollar_ratio' THEN rule_value END) AS thr_round_dollar_ratio,
    MAX(CASE WHEN rule_key = 'thr_network_risk' THEN rule_value END) AS thr_network_risk,
    MAX(CASE WHEN rule_key = 'thr_burst_multiplier' THEN rule_value END) AS thr_burst_multiplier,
    MAX(CASE WHEN rule_key = 'score_cap' THEN rule_value END) AS score_cap,
    MAX(CASE WHEN rule_key = 'band_high_min' THEN rule_value END) AS band_high_min,
    MAX(CASE WHEN rule_key = 'band_medium_min' THEN rule_value END) AS band_medium_min
  FROM {catalog}.gold.scoring_rule
  WHERE is_current
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
    -- raw burst inputs carried forward; burst_flag is derived in `scored` using the
    -- table-driven thr_burst_multiplier rather than a hard-coded 3.
    b.max_daily_count AS max_daily_count,
    b.avg_daily_count AS avg_daily_count,
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
  -- one CROSS JOIN against the single-row `rules` supplies every weight/threshold. points are
  -- CAST back to INT so the column types (and top_contributing_factors' `points`) are byte-for-
  -- byte what the hard-coded integer literals produced. burst_flag and pts_burst both use the
  -- raw inequality (not the burst_flag alias) because SQL can't reference a sibling select alias.
  SELECT
    j.*,
    r.score_cap,
    r.band_high_min,
    r.band_medium_min,
    COALESCE(j.max_daily_count > r.thr_burst_multiplier * j.avg_daily_count, false) AS burst_flag,
    CAST(CASE WHEN j.best_band_rank = 3 THEN r.pts_watchlist_exact WHEN j.best_band_rank = 2 THEN r.pts_watchlist_strong WHEN j.best_band_rank = 1 THEN r.pts_watchlist_weak ELSE 0 END AS INT) AS pts_watchlist,
    CAST(CASE WHEN j.velocity_7d > r.thr_velocity_7d THEN r.pts_velocity ELSE 0 END AS INT) AS pts_velocity,
    CAST(CASE WHEN j.counterparty_diversity_30d > r.thr_diversity_30d THEN r.pts_diversity ELSE 0 END AS INT) AS pts_diversity,
    CAST(CASE WHEN j.round_dollar_ratio > r.thr_round_dollar_ratio THEN r.pts_round_dollar ELSE 0 END AS INT) AS pts_round_dollar,
    CAST(CASE WHEN COALESCE(j.max_daily_count > r.thr_burst_multiplier * j.avg_daily_count, false) THEN r.pts_burst ELSE 0 END AS INT) AS pts_burst,
    CAST(CASE WHEN j.risky_counterparty_exposure THEN r.pts_risky_counterparty ELSE 0 END AS INT) AS pts_risky_counterparty,
    CAST(CASE WHEN j.network_risk_exposure IS NOT NULL AND j.network_risk_exposure > r.thr_network_risk THEN r.pts_network_risk ELSE 0 END AS INT) AS pts_network_risk
  FROM joined j
  CROSS JOIN rules r
),
final_scored AS (
  SELECT
    *,
    CAST(LEAST(
      score_cap,
      pts_watchlist + pts_velocity + pts_diversity + pts_round_dollar + pts_burst + pts_risky_counterparty + pts_network_risk
    ) AS INT) AS composite_risk_score
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
    WHEN composite_risk_score >= band_high_min THEN 'high'
    WHEN composite_risk_score >= band_medium_min THEN 'medium'
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


-- Clustered by the columns the queue is filtered by (status drives the row filter in
-- resources/ddl/01_ownership_and_access.sql; escalation_reason drives triage views).
CREATE OR REPLACE TABLE {catalog}.gold.prioritized_alert_queue
CLUSTER BY (status, escalation_reason)
TBLPROPERTIES (
  'delta.targetFileSize' = '134217728',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
WITH alert_rules AS (
  SELECT MAX(CASE WHEN rule_key = 'alert_min_score' THEN rule_value END) AS alert_min_score
  FROM {catalog}.gold.scoring_rule
  WHERE is_current
)
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
CROSS JOIN alert_rules
WHERE best_band_rank = 3 OR composite_risk_score >= alert_min_score;


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
