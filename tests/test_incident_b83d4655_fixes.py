# tests/test_incident_b83d4655_fixes.py
"""
Regression test suite for Incident INC-20261026-b83d4655 fixes:
1. Multi-target Lakehouse table subscription validation in MetadataValidator.
2. Context isolation: external MySQL target JDBC host is not overwritten by PGDBBI.
3. Transformer business key sanitization preventing NULL primary key insertion in Gold.
"""
import pytest
from unittest.mock import MagicMock
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.connections.connection_manager import ConnectionManager, ExecutionContext
from src.validation.metadata_validator import MetadataValidator
from src.transform.transformer import Transformer


def test_metadata_validator_multi_target_duplicate_check():
    """Verify MetadataValidator scopes duplicate active table checks by target_tag."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cm = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    cm.has_connection.return_value = True

    # 8 Check queries executed in order
    cur.fetchall.side_effect = [
        [("opf_oracle_housing", "land_housing_prod")],  # Check 1
        [],  # Check 2
        [],  # Check 3
        [],  # Check 4
        [],  # Check 5
        [],  # Check 6
        [],  # Check 7
        [],  # Check 8
    ]

    validator = MetadataValidator(cm)
    is_valid, errors, _ = validator.validate(
        source_tag="opf_oracle_housing",
        target_tag="land_housing_prod",
        quiet=True
    )
    assert is_valid is True
    assert len(errors) == 0

    # Verify query included target_tag in GROUP BY and WHERE clause
    executed_queries = [call.args[0] for call in cur.execute.call_args_list]
    dup_query = next(q for q in executed_queries if "GROUP BY" in q)
    assert "target_tag" in dup_query
    assert "HAVING COUNT(*) > 1" in dup_query


def test_connection_manager_mysql_target_spark_host_isolation(monkeypatch):
    """Verify external MySQL target host is not overridden by SPARK_CONTAINER_DB_HOST."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = [
        (
            "land_housing_prod", "DATABASE", False, True, "PROD", "mysql",
            "192.0.2.1", 3306, "land_housing_prod", "land_housing_prod", "hiveworx",
            "${MYSQL_PASSWORD}", "", {}, "Admin", "Data", "Target", 10,
        ),
        (
            "land_housing", "DATABASE", False, True, "DEV", "postgres",
            "192.0.2.1", 5432, "opf", "land_housing", "mg",
            "${PG_PASSWORD}", "", {}, "Admin", "Data", "Target", 10,
        ),
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cm = ConnectionManager.__new__(ConnectionManager)
    cm._raw_config = {"pipeline": {}}
    cm._configs = {}
    cm.get_metadata_db_connection = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    cm._resolve_env_vars = lambda value: "secret" if "PASSWORD" in value else value
    monkeypatch.setenv("SPARK_CONTAINER_DB_HOST", "PGDBBI")

    cm._build_connections()

    # MySQL target: must retain authoritative external host 192.0.2.1
    mysql_cfg = cm.get("land_housing_prod", expected_role="target")
    assert mysql_cfg.spark_host == ""
    assert "192.0.2.1:3306" in cm.resolve_jdbc(mysql_cfg, context=ExecutionContext.SPARK).url
    assert "PGDBBI" not in cm.resolve_jdbc(mysql_cfg, context=ExecutionContext.SPARK).url

    # PostgreSQL target: uses PGDBBI on internal Docker bridge network
    pg_cfg = cm.get("land_housing", expected_role="target")
    assert pg_cfg.spark_host == "PGDBBI"
    assert "PGDBBI:5432" in cm.resolve_jdbc(pg_cfg, context=ExecutionContext.SPARK).url


@pytest.mark.live
def test_transformer_sanitize_business_keys(spark):
    """Verify Transformer.sanitize_business_keys imputes NULL business keys with 'N/A'."""
    transformer = Transformer(run_id="test-incident-run")
    meta = MagicMock()
    meta.landing_mode = "METADATA"
    meta.canonical_source_business_key = ("schno", "plotno", "phase", "blockno", "street")
    meta.canonical_business_key_columns = ("schno", "plotno", "phase", "blockno", "street")

    schema = StructType([
        StructField("schno", StringType(), True),
        StructField("plotno", StringType(), True),
        StructField("phase", StringType(), True),
        StructField("blockno", StringType(), True),
        StructField("street", StringType(), True),
        StructField("plotsize", DoubleType(), True),
    ])

    data = [
        ("1", "49", None, "G", "STREET NO. 4", 250.0),
        ("1", "50", "Phase 1", None, None, 500.0),
    ]
    raw_df = spark.createDataFrame(data, schema)

    # Before sanitization: NULLs exist
    assert raw_df.filter("phase IS NULL").count() == 1
    assert raw_df.filter("blockno IS NULL").count() == 1
    assert raw_df.filter("street IS NULL").count() == 1

    clean_df = transformer.sanitize_business_keys(raw_df, meta)

    # After sanitization: NULLs are coalesced to 'N/A'
    assert clean_df.filter("phase IS NULL").count() == 0
    assert clean_df.filter("blockno IS NULL").count() == 0
    assert clean_df.filter("street IS NULL").count() == 0

    rows = clean_df.collect()
    assert rows[0]["phase"] == "N/A"
    assert rows[0]["blockno"] == "G"
    assert rows[1]["blockno"] == "N/A"
    assert rows[1]["street"] == "N/A"
