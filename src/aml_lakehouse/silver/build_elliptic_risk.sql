-- Silver Elliptic graph-topology risk: silver.elliptic_txn_risk. See
-- docs/03_schema_contracts.md's "Elliptic integration caveat" -- this is standalone
-- graph-topology analytics on the Elliptic Bitcoin dataset (self-join/aggregation over its
-- edgelist), not natively joinable to any AMLSim account. silver/build_network_risk_reference.py
-- is the separate, explicitly-labeled synthetic bridge that connects a sample of this output
-- to a subset of AMLSim accounts.
--
-- {catalog} is substituted by the calling job.

CREATE OR REPLACE TABLE {catalog}.silver.elliptic_txn_risk AS
WITH latest_classes AS (
  SELECT txId, class FROM {catalog}.bronze.elliptic_txs_classes
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.elliptic_txs_classes)
),
latest_features AS (
  SELECT txId, time_step FROM {catalog}.bronze.elliptic_txs_features
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.elliptic_txs_features)
),
latest_edges AS (
  SELECT txId1, txId2 FROM {catalog}.bronze.elliptic_txs_edgelist
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.elliptic_txs_edgelist)
),
-- Elliptic's edges are directed (money flow between transactions); neighbor risk should
-- consider both directions, so each edge contributes a neighbor relationship both ways.
neighbors AS (
  SELECT txId1 AS txId, txId2 AS neighbor_txId FROM latest_edges
  UNION ALL
  SELECT txId2 AS txId, txId1 AS neighbor_txId FROM latest_edges
),
neighbor_classes AS (
  SELECT n.txId, c.class AS neighbor_class
  FROM neighbors n
  LEFT JOIN latest_classes c ON n.neighbor_txId = c.txId
),
neighbor_agg AS (
  SELECT
    txId,
    sum(CASE WHEN neighbor_class = '1' THEN 1 ELSE 0 END) AS illicit_neighbors,
    sum(CASE WHEN neighbor_class IN ('1', '2') THEN 1 ELSE 0 END) AS known_class_neighbors
  FROM neighbor_classes
  GROUP BY txId
),
out_degree_agg AS (
  SELECT txId1 AS txId, count(*) AS out_degree FROM latest_edges GROUP BY txId1
),
in_degree_agg AS (
  SELECT txId2 AS txId, count(*) AS in_degree FROM latest_edges GROUP BY txId2
)
SELECT
  c.txId,
  c.class,
  f.time_step,
  COALESCE(od.out_degree, 0) AS out_degree,
  COALESCE(ind.in_degree, 0) AS in_degree,
  -- NULL (not 0) when there are no known-class neighbors to compute a ratio from -- an
  -- unknown-risk node is not the same claim as a zero-risk node.
  CASE
    WHEN na.known_class_neighbors > 0 THEN na.illicit_neighbors / na.known_class_neighbors
    ELSE NULL
  END AS illicit_neighbor_ratio
FROM latest_classes c
LEFT JOIN latest_features f ON c.txId = f.txId
LEFT JOIN out_degree_agg od ON c.txId = od.txId
LEFT JOIN in_degree_agg ind ON c.txId = ind.txId
LEFT JOIN neighbor_agg na ON c.txId = na.txId;
