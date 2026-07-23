-- Day-over-day variance: "why did today's number change?"
--
-- Answers the stakeholder question in plain English by attributing the change in the Gold
-- outputs between two runs. Because gold.prioritized_alert_queue and gold.entity_risk_profile
-- are CREATE OR REPLACE (one as_of_date per run), there is no in-table history to diff, so
-- this uses Delta time travel to compare the current table version against the prior version.
--
-- Usage:
--   1. Replace {{catalog}} with your environment catalog (aml_dev / aml_test / aml_prod_sim).
--   2. Find the previous version number:
--        DESCRIBE HISTORY {{catalog}}.gold.prioritized_alert_queue;
--      Take the version *before* the latest write and substitute it for {{prev_version}}
--      below (and the matching one for entity_risk_profile in query C).
--   3. Run in a Databricks SQL warehouse.

-- ---------------------------------------------------------------------------
-- A. Headline deltas: total alerts and the split by escalation reason,
--    current version vs previous. "There are N more alerts than the last run,
--    driven by sanctions overrides vs behavioral thresholds."
-- ---------------------------------------------------------------------------
WITH curr AS (
  SELECT escalation_reason, COUNT(*) AS alert_count
  FROM {{catalog}}.gold.prioritized_alert_queue
  GROUP BY escalation_reason
),
prev AS (
  SELECT escalation_reason, COUNT(*) AS alert_count
  FROM {{catalog}}.gold.prioritized_alert_queue VERSION AS OF {{prev_version}}
  GROUP BY escalation_reason
)
SELECT
  COALESCE(curr.escalation_reason, prev.escalation_reason) AS escalation_reason,
  COALESCE(prev.alert_count, 0) AS previous_alert_count,
  COALESCE(curr.alert_count, 0) AS current_alert_count,
  COALESCE(curr.alert_count, 0) - COALESCE(prev.alert_count, 0) AS delta
FROM curr
FULL OUTER JOIN prev ON curr.escalation_reason = prev.escalation_reason
ORDER BY ABS(COALESCE(curr.alert_count, 0) - COALESCE(prev.alert_count, 0)) DESC;

-- ---------------------------------------------------------------------------
-- B. Alert churn: which specific entities entered or dropped off the queue
--    between the two runs. "These 12 accounts are newly alerting; these 4 fell off."
-- ---------------------------------------------------------------------------
WITH curr AS (
  SELECT entity_id, account_id, composite_risk_score
  FROM {{catalog}}.gold.prioritized_alert_queue
),
prev AS (
  SELECT entity_id, account_id
  FROM {{catalog}}.gold.prioritized_alert_queue VERSION AS OF {{prev_version}}
)
SELECT
  CASE WHEN prev.entity_id IS NULL THEN 'ENTERED' ELSE 'DROPPED' END AS change_type,
  COALESCE(curr.entity_id, prev.entity_id) AS entity_id,
  COALESCE(curr.account_id, prev.account_id) AS account_id,
  curr.composite_risk_score AS current_score
FROM curr
FULL OUTER JOIN prev
  ON curr.entity_id = prev.entity_id AND curr.account_id = prev.account_id
WHERE curr.entity_id IS NULL OR prev.entity_id IS NULL
ORDER BY change_type, current_score DESC;

-- ---------------------------------------------------------------------------
-- C. Biggest score movers: entities whose composite_risk_score shifted most
--    between runs. Pair each mover with its top_contributing_factors (already
--    persisted on the profile) to explain the move factor-by-factor.
-- ---------------------------------------------------------------------------
WITH curr AS (
  SELECT entity_id, account_id, composite_risk_score, score_band
  FROM {{catalog}}.gold.entity_risk_profile
),
prev AS (
  SELECT entity_id, account_id, composite_risk_score AS prev_score, score_band AS prev_band
  FROM {{catalog}}.gold.entity_risk_profile VERSION AS OF {{prev_version}}
)
SELECT
  curr.entity_id,
  curr.account_id,
  prev.prev_score,
  curr.composite_risk_score AS current_score,
  curr.composite_risk_score - prev.prev_score AS score_delta,
  prev.prev_band,
  curr.score_band AS current_band
FROM curr
JOIN prev ON curr.entity_id = prev.entity_id AND curr.account_id = prev.account_id
WHERE curr.composite_risk_score <> prev.prev_score
ORDER BY ABS(curr.composite_risk_score - prev.prev_score) DESC
LIMIT 50;
