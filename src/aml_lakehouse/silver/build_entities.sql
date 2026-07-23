-- Silver entity resolution: unifies OFAC + OpenSanctions (sanctions + PEP) into
-- silver.entity / silver.entity_alias / silver.entity_address. See
-- docs/03_schema_contracts.md for the full contract and docs/00_business_charter.md for the
-- entity_type reconciliation rationale.
--
-- CREATE OR REPLACE (full rebuild each run), not MERGE: OFAC/OpenSanctions Bronze tables are
-- themselves periodic full-refresh snapshots (see dataset_inspection_notes.md), so there is
-- no meaningful incremental delta to merge -- a full rebuild from each source's latest batch
-- is simpler and avoids merge-key drift if OFAC ever reassigns/retires an ent_num.
--
-- entity_id is a stable hash of (source_system, source_entity_id), not a row_number --
-- silver.match_candidate and Gold tables reference it, so it must not change across reruns.
--
-- {catalog} is substituted by the calling job per docs/02_environment_and_branching.md
-- (aml_dev / aml_test / aml_prod_sim).
--
-- Table-driven entity_type reconciliation (docs/13_table_driven_config.md): the source ->
-- unified vocabulary mapping lives in silver.entity_type_map (an SCD2 reference table) rather
-- than hard-coded CASE logic, so a new source value is a table row, not a code change. The
-- create + seed below is idempotent (IF NOT EXISTS + insert-only-when-empty), so it never
-- wipes SCD2 history on a rebuild -- unlike the CREATE OR REPLACE data tables that follow.

CREATE TABLE IF NOT EXISTS {catalog}.silver.entity_type_map (
  source_system STRING,
  source_value STRING,
  unified_value STRING,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN,
  description STRING
);

INSERT INTO {catalog}.silver.entity_type_map
SELECT * FROM (VALUES
  ('ofac', 'individual', 'person', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OFAC sdn_type individual -> person'),
  ('ofac', 'vessel', 'vessel', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OFAC sdn_type vessel'),
  ('ofac', 'aircraft', 'aircraft', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OFAC sdn_type aircraft'),
  ('ofac', '', 'organization', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OFAC null/blank/-0- sdn_type -> organization (default)'),
  ('opensanctions', 'Person', 'person', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OpenSanctions schema Person -> person'),
  ('opensanctions', 'Organization', 'organization', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OpenSanctions schema Organization -> organization'),
  ('opensanctions', 'Vessel', 'vessel', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OpenSanctions schema Vessel -> vessel'),
  ('opensanctions', 'Airplane', 'aircraft', DATE'2026-01-01', CAST(NULL AS DATE), true, 'OpenSanctions schema Airplane -> aircraft')
) AS v(source_system, source_value, unified_value, valid_from, valid_to, is_current, description)
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.silver.entity_type_map);

CREATE OR REPLACE TABLE {catalog}.silver.entity AS
WITH latest_ofac_sdn AS (
  -- OFAC's unquoted "-0-" null sentinel is written with a trailing space (e.g. "-0- ") --
  -- confirmed by an actual query against the real Bronze table (length(sdn_type) = 4, not
  -- 3). Every raw OFAC column is trimmed once here so every downstream comparison against
  -- '-0-' or a fixed value (e.g. 'individual') works without repeating trim() everywhere.
  SELECT
    trim(ent_num) AS ent_num, trim(sdn_name) AS sdn_name, trim(sdn_type) AS sdn_type,
    trim(program) AS program, trim(title) AS title, trim(remarks) AS remarks
  FROM {catalog}.bronze.ofac_sdn
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.ofac_sdn)
),
ofac_entities AS (
  SELECT
    sha2(concat('ofac:', ent_num), 256) AS entity_id,
    'ofac' AS source_system,
    ent_num AS source_entity_id,
    NULLIF(sdn_name, '-0-') AS primary_name,
    -- entity_type via silver.entity_type_map (was a hard-coded CASE). The key normalizes
    -- NULL / '' / '-0-' to '' so all three hit the 'organization' default row; any value with
    -- no mapping row falls through to 'other', exactly as the original CASE's ELSE did.
    COALESCE(m.unified_value, 'other') AS entity_type,
    NULLIF(program, '-0-') AS watchlist_program,
    -- OFAC's SDN.CSV has no dedicated DOB column -- individuals' DOBs are embedded as free
    -- text inside `remarks` (e.g. "DOB 01 Jan 1970"). Not regex-extracted in v1; documented
    -- limitation, not an oversight -- see docs/03_schema_contracts.md open items.
    CAST(NULL AS STRING) AS birth_date,
    CAST(NULL AS ARRAY<STRING>) AS countries,
    true AS is_watchlist
  FROM latest_ofac_sdn s
  LEFT JOIN {catalog}.silver.entity_type_map m
    ON m.source_system = 'ofac'
   AND m.source_value = COALESCE(NULLIF(s.sdn_type, '-0-'), '')
   AND m.is_current
),
latest_sanctions AS (
  SELECT *, 'opensanctions_sanctions' AS source_system FROM {catalog}.bronze.opensanctions_sanctions
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.opensanctions_sanctions)
),
latest_peps AS (
  SELECT *, 'opensanctions_peps' AS source_system FROM {catalog}.bronze.opensanctions_peps
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.opensanctions_peps)
),
opensanctions_entities AS (
  SELECT
    sha2(concat(source_system, ':', id), 256) AS entity_id,
    source_system,
    id AS source_entity_id,
    name AS primary_name,
    -- entity_type via silver.entity_type_map (was a hard-coded CASE), keyed on OpenSanctions'
    -- `schema` value; unmapped schemas fall through to 'other' as the original CASE's ELSE did.
    COALESCE(m.unified_value, 'other') AS entity_type,
    program_ids AS watchlist_program,
    NULLIF(trim(birth_date), '') AS birth_date,
    CASE WHEN countries IS NULL OR trim(countries) = '' THEN NULL ELSE split(countries, ';') END AS countries,
    true AS is_watchlist
  FROM (SELECT * FROM latest_sanctions UNION ALL SELECT * FROM latest_peps) x
  LEFT JOIN {catalog}.silver.entity_type_map m
    ON m.source_system = 'opensanctions' AND m.source_value = x.schema AND m.is_current
)
SELECT * FROM ofac_entities
UNION ALL
SELECT * FROM opensanctions_entities;


CREATE OR REPLACE TABLE {catalog}.silver.entity_alias AS
WITH latest_ofac_alt AS (
  -- see silver.entity's latest_ofac_sdn CTE for why every raw column is trimmed here
  SELECT trim(ent_num) AS ent_num, trim(alt_name) AS alt_name
  FROM {catalog}.bronze.ofac_alt
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.ofac_alt)
),
ofac_aliases AS (
  SELECT
    sha2(concat('ofac:', ent_num), 256) AS entity_id,
    NULLIF(alt_name, '-0-') AS alias_name
  FROM latest_ofac_alt
  WHERE NULLIF(alt_name, '-0-') IS NOT NULL
),
latest_sanctions AS (
  SELECT id, aliases, 'opensanctions_sanctions' AS source_system FROM {catalog}.bronze.opensanctions_sanctions
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.opensanctions_sanctions)
),
latest_peps AS (
  SELECT id, aliases, 'opensanctions_peps' AS source_system FROM {catalog}.bronze.opensanctions_peps
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.opensanctions_peps)
),
opensanctions_combined AS (
  SELECT * FROM latest_sanctions UNION ALL SELECT * FROM latest_peps
),
opensanctions_aliases AS (
  SELECT
    sha2(concat(source_system, ':', id), 256) AS entity_id,
    trim(alias_name) AS alias_name
  FROM opensanctions_combined
  LATERAL VIEW explode(split(aliases, ';')) AS alias_name
  WHERE aliases IS NOT NULL AND trim(aliases) != '' AND trim(alias_name) != ''
),
unioned AS (
  SELECT * FROM ofac_aliases
  UNION ALL
  SELECT * FROM opensanctions_aliases
)
-- alias_normalized here is a simple SQL-native approximation (uppercase, strip
-- non-alphanumerics, collapse whitespace) for readability/indexing only -- it is NOT what
-- fuzzy_match.py actually matches against. The matcher always re-normalizes alias_name
-- through the tested normalize_name() at match time, so any drift between this SQL
-- approximation and the Python normalizer (e.g. diacritics) never affects match correctness.
SELECT
  entity_id,
  alias_name,
  upper(trim(regexp_replace(regexp_replace(alias_name, '[^a-zA-Z0-9]', ' '), ' +', ' '))) AS alias_normalized
FROM unioned;


CREATE OR REPLACE TABLE {catalog}.silver.entity_address AS
WITH latest_ofac_add AS (
  -- see silver.entity's latest_ofac_sdn CTE for why every raw column is trimmed here
  SELECT
    trim(ent_num) AS ent_num, trim(address) AS address,
    trim(city_state_zip_etc) AS city_state_zip_etc, trim(country) AS country
  FROM {catalog}.bronze.ofac_add
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.ofac_add)
),
ofac_addresses AS (
  SELECT
    sha2(concat('ofac:', ent_num), 256) AS entity_id,
    NULLIF(address, '-0-') AS address_line,
    -- OFAC's city/state/zip arrive pre-combined in one field with no consistent delimiter
    -- across countries; stored as-is rather than mis-parsed. Documented v1 simplification.
    NULLIF(city_state_zip_etc, '-0-') AS city,
    NULLIF(country, '-0-') AS country
  FROM latest_ofac_add
  WHERE NULLIF(address, '-0-') IS NOT NULL OR NULLIF(city_state_zip_etc, '-0-') IS NOT NULL
),
latest_sanctions AS (
  SELECT id, addresses, 'opensanctions_sanctions' AS source_system FROM {catalog}.bronze.opensanctions_sanctions
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.opensanctions_sanctions)
),
latest_peps AS (
  SELECT id, addresses, 'opensanctions_peps' AS source_system FROM {catalog}.bronze.opensanctions_peps
  WHERE _batch_id = (SELECT MAX(_batch_id) FROM {catalog}.bronze.opensanctions_peps)
),
opensanctions_combined AS (
  SELECT * FROM latest_sanctions UNION ALL SELECT * FROM latest_peps
),
opensanctions_addresses AS (
  -- targets.simple.csv gives one free-text address blob per entity, not structured
  -- line/city/country -- city/country are left NULL here (country is already available at
  -- the entity level via silver.entity.countries) rather than guessed from free text.
  SELECT
    sha2(concat(source_system, ':', id), 256) AS entity_id,
    trim(address_line) AS address_line,
    CAST(NULL AS STRING) AS city,
    CAST(NULL AS STRING) AS country
  FROM opensanctions_combined
  LATERAL VIEW explode(split(addresses, ';')) AS address_line
  WHERE addresses IS NOT NULL AND trim(addresses) != '' AND trim(address_line) != ''
)
SELECT * FROM ofac_addresses
UNION ALL
SELECT * FROM opensanctions_addresses;
