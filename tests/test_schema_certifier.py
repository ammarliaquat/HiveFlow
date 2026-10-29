import os
import pytest
from unittest.mock import MagicMock, patch
from src.connections.schema_certifier import SchemaCertifier
from src.exceptions import MetadataSchemaIncompatibleError
from src.orchestrator.etl import ETL

def test_schema_certifier_success():
    mock_conn_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn_mgr.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    certifier = SchemaCertifier(mock_conn_mgr)
    
    # Get actual ledger migrations
    local_migrations = certifier._get_expected_ledger_migrations()
    
    # Mock DB ledger returning exact match
    mock_cur.fetchall.return_value = local_migrations
    
    # Certify should succeed without exception
    certifier.certify()

def test_schema_certifier_missing_migration_raises_error():
    mock_conn_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn_mgr.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    certifier = SchemaCertifier(mock_conn_mgr)
    local_migrations = certifier._get_expected_ledger_migrations()
    
    # DB ledger is missing the last migration
    mock_cur.fetchall.return_value = local_migrations[:-1]
    
    with pytest.raises(MetadataSchemaIncompatibleError, match="Database is missing required migration"):
        certifier.certify()

def test_schema_certifier_checksum_mismatch_raises_error():
    mock_conn_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn_mgr.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    certifier = SchemaCertifier(mock_conn_mgr)
    local_migrations = certifier._get_expected_ledger_migrations()
    
    # Alter checksum of first migration in DB ledger
    db_migrations = list(local_migrations)
    ver, name, _ = db_migrations[0]
    db_migrations[0] = (ver, name, "bad_checksum_12345")
    mock_cur.fetchall.return_value = db_migrations
    
    with pytest.raises(MetadataSchemaIncompatibleError, match="Checksum mismatch"):
        certifier.certify()


def test_schema_certifier_apply_migration_is_ordered_and_ledgers_real_checksum():
    mock_conn_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn_mgr.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    certifier = SchemaCertifier(mock_conn_mgr)
    local_migrations = certifier._get_expected_ledger_migrations()
    target_version, target_name, target_checksum = local_migrations[-1]
    mock_cur.fetchall.return_value = local_migrations[:-1]

    certifier.apply_migration(target_name)

    executed_sql = [call.args[0] for call in mock_cur.execute.call_args_list]
    ledger_calls = [
        call for call in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.schema_migrations" in call.args[0]
    ]

    assert ledger_calls, "apply_migration must insert a schema_migrations ledger row"
    assert ledger_calls[-1].args[1] == (target_version, target_name, target_checksum)
    assert all(statement.strip().upper() not in ("BEGIN;", "COMMIT;") for statement in executed_sql)
    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()


def test_schema_certifier_apply_migration_refuses_out_of_order_apply():
    mock_conn_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn_mgr.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    certifier = SchemaCertifier(mock_conn_mgr)
    local_migrations = certifier._get_expected_ledger_migrations()
    mock_cur.fetchall.return_value = local_migrations[:-2]

    with pytest.raises(MetadataSchemaIncompatibleError, match="next missing migration"):
        certifier.apply_migration(local_migrations[-1][1])

    mock_conn.commit.assert_not_called()


def test_schema_certifier_ledger_uses_first_file_for_duplicate_versions():
    certifier = SchemaCertifier(MagicMock())

    ledger_migrations = certifier._get_expected_ledger_migrations()
    ledger_by_version = {version: name for version, name, _ in ledger_migrations}

    assert ledger_by_version["010"] == "010_notifications_schema.sql"
    assert ledger_by_version["011"] == "011_add_source_tag_to_db_columns_etl.sql"
    assert ledger_by_version["012"] == "012_add_connection_limits.sql"
    assert ledger_by_version["013"] == "013_add_data_governance.sql"
    assert ledger_by_version["021"] == "021_add_stage_metrics_column.sql"
    assert ledger_by_version["022"] == "022_benchmark_history_schema.sql"
    assert len(ledger_migrations) < len(certifier._get_local_migrations())


def test_etl_accepts_injected_schema_certifier():
    mock_certifier = MagicMock()
    with patch("src.orchestrator.etl.ConnectionManager"), \
         patch("src.spark_session.SparkSessionFactory.get_session"), \
         patch("src.metadata.metadata_reader.MetadataReader"):
        etl = ETL(schema_certifier=mock_certifier)
        mock_certifier.certify.assert_called_once()
