# tests/test_metadata_reader.py
from unittest.mock import MagicMock, patch
import pytest
from src.metadata.metadata_reader import MetadataReader
from src.metadata.metadata_models import MetadataRow

def test_metadata_reader_get_tables():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    # Mock description to match selected columns (including new observability fields)
    mock_cur.description = [
        ('id',), ('source_tag',), ('source_schema',), ('source_table',), ('source_db_type',),
        ('target_database_type',),
        ('landing_table',), ('history_table',), ('target_table',), ('target_schema',),
        ('is_active',), ('priority',), ('load_type',), ('is_cdc',), ('cdc_columns',),
        ('cdc_p_days',), ('delta_days',), ('last_success_timestamp',), ('is_reconcile',),
        ('reconcile_column',), ('reconcile_measure',), ('reconcile_threshold',), ('reconcile_action',),
        ('rerun_flag',), ('last_success_date',), ('chunk_size',), ('num_partitions',),
        ('partition_column',), ('source_key_prefix',), ('natural_key_column',), ('notes',),
        ('last_load_pct',), ('last_run_end_time',)
    ]
    
    # Mock one table row (with last_load_pct and last_run_end_time)
    mock_cur.fetchall.return_value = [
        (1, "mysql_membership", "db", "users", "mysql", "postgres", "lnd_users", "hist_users", "users", "land_n",
         "y", 1, "full", "n", None, 7, 1, None, "y", None, "count", 0.5, "FAIL", "n", None,
         10000, 5, "id", "mysql_membership-", "id", "Some notes", 100.0, None)
    ]
    
    mock_conn_manager = MagicMock()
    mock_conn_manager.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    
    reader = MetadataReader(mock_conn_manager)
    tables = reader.get_tables("mysql_membership")
    
    assert len(tables) == 1
    row = tables[0]
    assert isinstance(row, MetadataRow)
    assert row.id == 1
    assert row.source_table == "users"
    assert row.is_reconcile == "y"
    assert row.reconcile_threshold == 0.5
    assert row.reconcile_action == "FAIL"
    assert row.rerun_flag == "n"
    assert row.last_success_date is None
    
    # Verify the SQL matches and contains ALL skip logic including self-healing triggers
    call_args = mock_cur.execute.call_args[0]
    sql_query = call_args[0]
    assert "etl.rerun_flag = 'y'" in sql_query
    assert "etl.last_success_date IS NULL" in sql_query
    assert "etl.last_success_date < CURRENT_DATE" in sql_query
    assert "lb.load_pct < 100" in sql_query
    assert "lb.end_time < NOW()" in sql_query
    assert "last_batch" in sql_query  # CTE must be present


def test_metadata_reader_get_tables_with_override():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    
    mock_cur.description = [
        ('id',), ('source_tag',), ('source_schema',), ('source_table',), ('source_db_type',),
        ('target_database_type',),
        ('landing_table',), ('history_table',), ('target_table',), ('target_schema',),
        ('is_active',), ('priority',), ('load_type',), ('is_cdc',), ('cdc_columns',),
        ('cdc_p_days',), ('delta_days',), ('last_success_timestamp',), ('is_reconcile',),
        ('reconcile_column',), ('reconcile_measure',), ('reconcile_threshold',), ('reconcile_action',),
        ('rerun_flag',), ('last_success_date',), ('chunk_size',), ('num_partitions',),
        ('partition_column',), ('source_key_prefix',), ('natural_key_column',), ('notes',),
        ('last_load_pct',), ('last_run_end_time',)
    ]
    
    mock_cur.fetchall.return_value = [
        (1, "mysql_membership", "db", "users", "mysql", "postgres", "lnd_users", "hist_users", "users", "land_n",
         "y", 1, "full", "n", None, 7, 1, None, "y", None, "count", 0.5, "FAIL", "n", None,
         10000, 5, "id", "mysql_membership-", "id", "Some notes", 95.5, None)
    ]
    
    mock_conn_manager = MagicMock()
    mock_conn_manager.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    
    reader = MetadataReader(mock_conn_manager)
    tables = reader.get_tables("mysql_membership", table_name="users")
    
    assert len(tables) == 1
    
    # Verify the SQL does NOT contain the skip logic when table_name is overridden
    call_args = mock_cur.execute.call_args[0]
    sql_query = call_args[0]
    params = call_args[1]
    
    assert "lb.load_pct < 100" not in sql_query
    assert "etl.rerun_flag = 'y'" not in sql_query
    assert "etl.source_table = %s" in sql_query
    assert "users" in params
    # CTE is still present (it's always included in the query template)
    assert "last_batch" in sql_query
