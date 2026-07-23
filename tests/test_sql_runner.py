from pathlib import Path

from aml_lakehouse.common.sql_runner import load_statements

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_load_statements_basic_splitting(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text(
        "-- comment\nCREATE TABLE a AS SELECT 1;\n\nCREATE TABLE b AS SELECT 2;\n"
    )
    statements = load_statements(str(sql_file))
    assert len(statements) == 2
    assert "CREATE TABLE a" in statements[0]
    assert "CREATE TABLE b" in statements[1]
    assert not statements[0].endswith(";")


def test_load_statements_applies_substitutions(tmp_path):
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("SELECT * FROM {catalog}.bronze.ofac_sdn;\n\n")
    statements = load_statements(str(sql_file), catalog="aml_dev")
    assert statements == ["SELECT * FROM aml_dev.bronze.ofac_sdn"]
    assert "{catalog}" not in statements[0]


def test_real_build_entities_sql_splits_into_five_statements():
    # Now 5: the SCD2 entity_type_map is created + seeded (2 statements) before the three
    # entity tables are built. Statement 0 legitimately carries the file's leading comment
    # block as a prefix -- harmless for spark.sql() -- so this checks containment, not startswith.
    sql_path = REPO_ROOT / "src" / "aml_lakehouse" / "silver" / "build_entities.sql"
    statements = load_statements(str(sql_path), catalog="aml_dev")
    assert len(statements) == 5
    assert "CREATE TABLE IF NOT EXISTS aml_dev.silver.entity_type_map" in statements[0]
    assert statements[1].startswith("INSERT INTO aml_dev.silver.entity_type_map")
    assert "CREATE OR REPLACE TABLE aml_dev.silver.entity AS" in statements[2]
    assert statements[3].startswith("CREATE OR REPLACE TABLE aml_dev.silver.entity_alias AS")
    assert statements[4].startswith("CREATE OR REPLACE TABLE aml_dev.silver.entity_address AS")
    assert "{catalog}" not in "".join(statements)


def test_gold_and_elliptic_sql_include_plan_hints():
    gold_sql = (REPO_ROOT / "src" / "aml_lakehouse" / "gold" / "build_gold.sql").read_text()
    elliptic_sql = (
        REPO_ROOT / "src" / "aml_lakehouse" / "silver" / "build_elliptic_risk.sql"
    ).read_text()

    assert "params AS (" in gold_sql
    assert "BROADCAST(n)" in gold_sql
    assert "BROADCAST(ws)" in gold_sql
    assert "composite_risk_score" in gold_sql

    assert "BROADCAST(c)" in elliptic_sql
    assert "BROADCAST(f), BROADCAST(od), BROADCAST(ind), BROADCAST(na)" in elliptic_sql


def test_gold_tables_declare_liquid_clustering_and_file_sizing():
    gold_sql = (REPO_ROOT / "src" / "aml_lakehouse" / "gold" / "build_gold.sql").read_text()

    # clustering keys match the columns the tables are filtered/drilled by
    assert "CLUSTER BY (score_band, account_id)" in gold_sql
    assert "CLUSTER BY (status, escalation_reason)" in gold_sql
    # explicit file sizing + auto-optimize so OPTIMIZE targets ~128MB and small files compact
    assert gold_sql.count("'delta.targetFileSize' = '134217728'") >= 2
    assert "'delta.autoOptimize.autoCompact' = 'true'" in gold_sql


def test_transaction_sql_partitions_by_event_date():
    txn_sql = (
        REPO_ROOT / "src" / "aml_lakehouse" / "silver" / "build_transactions.sql"
    ).read_text()

    assert "PARTITIONED BY (event_date)" in txn_sql
    assert "DATE(event_time) AS event_date" in txn_sql
    # partitioned time-series fact still gets file-size control to avoid small-file confetti
    assert "'delta.targetFileSize' = '134217728'" in txn_sql
