-- One-time environment setup: Unity Catalog catalogs/schemas/volumes for all three
-- environments (docs/02_environment_and_branching.md). Run once per workspace, idempotent
-- (IF NOT EXISTS throughout) so it's safe to re-run in CI as a pre-deploy check.

CREATE CATALOG IF NOT EXISTS aml_dev;
CREATE CATALOG IF NOT EXISTS aml_test;
CREATE CATALOG IF NOT EXISTS aml_prod_sim;

CREATE SCHEMA IF NOT EXISTS aml_dev.bronze;
CREATE SCHEMA IF NOT EXISTS aml_dev.silver;
CREATE SCHEMA IF NOT EXISTS aml_dev.gold;

CREATE SCHEMA IF NOT EXISTS aml_test.bronze;
CREATE SCHEMA IF NOT EXISTS aml_test.silver;
CREATE SCHEMA IF NOT EXISTS aml_test.gold;

CREATE SCHEMA IF NOT EXISTS aml_prod_sim.bronze;
CREATE SCHEMA IF NOT EXISTS aml_prod_sim.silver;
CREATE SCHEMA IF NOT EXISTS aml_prod_sim.gold;

-- Landing zone for downloaded raw files before Spark parses them into Bronze Delta tables.
-- One volume per environment so a dev run never touches prod-sim's raw files.
CREATE VOLUME IF NOT EXISTS aml_dev.bronze.raw_files;
CREATE VOLUME IF NOT EXISTS aml_test.bronze.raw_files;
CREATE VOLUME IF NOT EXISTS aml_prod_sim.bronze.raw_files;

-- Streaming checkpoints live in their own volume, separate from raw_files, so a checkpoint
-- directory listing is never accidentally polluted by unrelated downloaded source files.
CREATE VOLUME IF NOT EXISTS aml_dev.bronze.checkpoints;
CREATE VOLUME IF NOT EXISTS aml_test.bronze.checkpoints;
CREATE VOLUME IF NOT EXISTS aml_prod_sim.bronze.checkpoints;
