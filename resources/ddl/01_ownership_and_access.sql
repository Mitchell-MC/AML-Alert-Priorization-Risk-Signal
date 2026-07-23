-- Ownership, access control, column masking, and row filtering for the medallion layers.
--
-- DESIGN-INTENT DDL. This is real, valid Unity Catalog syntax that expresses the ownership
-- and least-privilege access model in docs/09_ownership_and_access_model.md. It is NOT run
-- by any job and is NOT wired into CI, because Databricks Free Edition has a single personal
-- identity with no account console, no groups, and no SCIM (docs/02_environment_and_branching.md).
-- The GRANT / ALTER OWNER / SET MASK / SET ROW FILTER statements below therefore cannot be
-- enforced on this tier -- they document exactly what would be applied on an enterprise
-- account where the groups referenced here actually exist. Treat this file as the executable
-- specification of the access model, runnable as-is once the groups below are provisioned.
--
-- {catalog} is substituted per environment (aml_dev / aml_test / aml_prod_sim) the same way
-- the transform SQL is, so the model is identical across environments.
--
-- Principal groups (provisioned out-of-band via account console / SCIM / Terraform -- there
-- is no CREATE GROUP in SQL, groups are an account-level identity concern):
--   aml_platform_admins     -- platform/UC administration, break-glass access
--   aml_data_engineers      -- own Bronze, build Silver; the only humans who see raw PII
--   aml_analytics_engineers -- own Gold, co-build Silver business logic
--   aml_data_scientists     -- Silver access for modeling, PII masked
--   aml_bi_analysts         -- Gold consumption through approved BI tools
--   aml_business_users      -- Gold consumption, suppressed/ops detail filtered out

-- ---------------------------------------------------------------------------
-- 0. Governance schema: home for masking and row-filter UDFs (kept out of the
--    data layers so a layer rebuild never drops a governance function).
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {catalog}.governance
  COMMENT 'Masking / row-filter UDFs and access-control objects. Owned by platform admins.';

ALTER SCHEMA {catalog}.governance OWNER TO `aml_platform_admins`;

-- ---------------------------------------------------------------------------
-- 1. Per-layer ownership (see the matrix in docs/09_ownership_and_access_model.md).
--    Ownership is the load-bearing part the video calls out: a layer with no
--    owner decays on a schedule.
-- ---------------------------------------------------------------------------

ALTER SCHEMA {catalog}.bronze OWNER TO `aml_data_engineers`;

ALTER SCHEMA {catalog}.silver OWNER TO `aml_data_engineers`;

ALTER SCHEMA {catalog}.gold OWNER TO `aml_analytics_engineers`;

-- ---------------------------------------------------------------------------
-- 2. Catalog reachability. USE CATALOG is a prerequisite for reaching any
--    schema; it grants no data access on its own, so every consuming group gets it.
-- ---------------------------------------------------------------------------

GRANT USE CATALOG ON CATALOG {catalog} TO `aml_platform_admins`;
GRANT USE CATALOG ON CATALOG {catalog} TO `aml_data_engineers`;
GRANT USE CATALOG ON CATALOG {catalog} TO `aml_analytics_engineers`;
GRANT USE CATALOG ON CATALOG {catalog} TO `aml_data_scientists`;
GRANT USE CATALOG ON CATALOG {catalog} TO `aml_bi_analysts`;
GRANT USE CATALOG ON CATALOG {catalog} TO `aml_business_users`;

-- ---------------------------------------------------------------------------
-- 3. BRONZE -- kept tight. Raw intake, source quirks, un-standardized values,
--    pre-masking PII. Data engineering + platform admins only. No analyst,
--    data-scientist, or business access by design (the "no analysts on raw
--    layers" rule).
-- ---------------------------------------------------------------------------

GRANT USE SCHEMA ON SCHEMA {catalog}.bronze TO `aml_data_engineers`;
GRANT USE SCHEMA ON SCHEMA {catalog}.bronze TO `aml_platform_admins`;

GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA {catalog}.bronze TO `aml_data_engineers`;
GRANT SELECT ON SCHEMA {catalog}.bronze TO `aml_platform_admins`;

-- ---------------------------------------------------------------------------
-- 4. SILVER -- controlled. Standardized, reusable building blocks. Technically
--    mature users only (DE, analytics engineers, data scientists). SIMULATED-PII
--    columns are masked below for everyone except data engineers / admins.
-- ---------------------------------------------------------------------------

GRANT USE SCHEMA ON SCHEMA {catalog}.silver TO `aml_data_engineers`;
GRANT USE SCHEMA ON SCHEMA {catalog}.silver TO `aml_analytics_engineers`;
GRANT USE SCHEMA ON SCHEMA {catalog}.silver TO `aml_data_scientists`;
GRANT USE SCHEMA ON SCHEMA {catalog}.silver TO `aml_platform_admins`;

GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA {catalog}.silver TO `aml_data_engineers`;
GRANT SELECT, MODIFY ON SCHEMA {catalog}.silver TO `aml_analytics_engineers`;
GRANT SELECT ON SCHEMA {catalog}.silver TO `aml_data_scientists`;
GRANT SELECT ON SCHEMA {catalog}.silver TO `aml_platform_admins`;

-- ---------------------------------------------------------------------------
-- 5. GOLD -- broad but governed. The default safe path for trusted consumption.
--    Analysts, BI, and business users read here through approved tools; ops_control
--    detail and suppressed alerts are narrowed by the ops grant + row filter below.
-- ---------------------------------------------------------------------------

GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `aml_analytics_engineers`;
GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `aml_bi_analysts`;
GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `aml_business_users`;
GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `aml_data_engineers`;
GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `aml_data_scientists`;
GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `aml_platform_admins`;

GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA {catalog}.gold TO `aml_analytics_engineers`;
GRANT SELECT ON SCHEMA {catalog}.gold TO `aml_bi_analysts`;
GRANT SELECT ON SCHEMA {catalog}.gold TO `aml_business_users`;
GRANT SELECT ON SCHEMA {catalog}.gold TO `aml_data_scientists`;
GRANT SELECT ON SCHEMA {catalog}.gold TO `aml_platform_admins`;

-- ops_control is an operations/engineering artifact, not business-facing. Revoke the
-- broad schema-level SELECT for business users on this one table so it is not exposed
-- through general Gold access.
REVOKE SELECT ON TABLE {catalog}.gold.ops_control FROM `aml_business_users`;

-- ---------------------------------------------------------------------------
-- 6. Column masks for SIMULATED-PII (docs/01 classification). Data engineers and
--    platform admins see cleartext; everyone else sees a redacted value. Masks are
--    defined as UDFs in the governance schema and attached with ALTER COLUMN SET MASK.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {catalog}.governance.mask_holder_name(name STRING)
  RETURN CASE
    WHEN is_account_group_member('aml_data_engineers')
      OR is_account_group_member('aml_platform_admins')
    THEN name
    ELSE concat('MASKED#', substr(sha2(coalesce(name, ''), 256), 1, 8))
  END;

CREATE OR REPLACE FUNCTION {catalog}.governance.mask_dob(dob DATE)
  RETURN CASE
    WHEN is_account_group_member('aml_data_engineers')
      OR is_account_group_member('aml_platform_admins')
    THEN dob
    ELSE NULL
  END;

CREATE OR REPLACE FUNCTION {catalog}.governance.mask_address(line STRING)
  RETURN CASE
    WHEN is_account_group_member('aml_data_engineers')
      OR is_account_group_member('aml_platform_admins')
    THEN line
    ELSE '*** REDACTED ***'
  END;

ALTER TABLE {catalog}.silver.account
  ALTER COLUMN synthetic_holder_name SET MASK {catalog}.governance.mask_holder_name;

ALTER TABLE {catalog}.silver.account
  ALTER COLUMN synthetic_dob SET MASK {catalog}.governance.mask_dob;

ALTER TABLE {catalog}.silver.entity_address
  ALTER COLUMN address_line SET MASK {catalog}.governance.mask_address;

-- ---------------------------------------------------------------------------
-- 7. Row filter on the prioritized alert queue. Reviewers/engineers see every
--    alert including suppressed ones; downstream BI and business consumers do not
--    see suppressed alerts, so a suppressed (false-positive) alert never resurfaces
--    in a general dashboard.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {catalog}.governance.alert_visibility(status STRING)
  RETURN
    is_account_group_member('aml_analytics_engineers')
    OR is_account_group_member('aml_data_engineers')
    OR is_account_group_member('aml_platform_admins')
    OR status <> 'suppressed';

ALTER TABLE {catalog}.gold.prioritized_alert_queue
  SET ROW FILTER {catalog}.governance.alert_visibility ON (status);

-- ---------------------------------------------------------------------------
-- 8. Classification tags (docs/01). Unity Catalog tags make the classification
--    queryable/discoverable rather than living only in a doc. Applied at table and
--    column level so lineage and discovery tools can surface them.
-- ---------------------------------------------------------------------------

ALTER TABLE {catalog}.silver.account
  ALTER COLUMN synthetic_holder_name SET TAGS ('data_classification' = 'SIMULATED-PII');

ALTER TABLE {catalog}.silver.account
  ALTER COLUMN synthetic_dob SET TAGS ('data_classification' = 'SIMULATED-PII');

ALTER TABLE {catalog}.silver.account
  ALTER COLUMN synthetic_country SET TAGS ('data_classification' = 'SIMULATED-PII');

ALTER TABLE {catalog}.silver.entity_address
  ALTER COLUMN address_line SET TAGS ('data_classification' = 'SIMULATED-PII');

ALTER TABLE {catalog}.gold.entity_risk_profile
  SET TAGS ('data_classification' = 'INTERNAL');

ALTER TABLE {catalog}.gold.prioritized_alert_queue
  SET TAGS ('data_classification' = 'INTERNAL');
