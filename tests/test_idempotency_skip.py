# tests/test_idempotency_skip.py
import pytest
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock
from src.connections.connection_manager import ConnectionManager, ConnectionConfig

from src.orchestrator.etl import ETL
from src.constants import RunStatus
from src.metadata.metadata_reader import MetadataReader
from src.validation.reconciler import Reconciler

pytestmark = pytest.mark.live

@pytest.fixture(scope="module")
def db_conn():
    conn_mgr = ConnectionManager("config/config.local.yaml")
    with conn_mgr.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            # Clean up stale batch logs and metadata state from previous test runs to ensure isolation
            cur.execute("DELETE FROM metadata.batch_log WHERE source_table = 'directorates';")
            cur.execute("""
                INSERT INTO metadata.db_tables_etl (
                    source_tag, source_database, source_database_type, source_schema, source_table,
                    target_tag, target_database, target_database_type, target_schema, target_table,
                    landing_table, history_table, priority, is_active
                ) VALUES (
                    'mysql_membership', 'db', 'mysql', 'db', 'directorates',
                    'land_n', 'opf', 'postgres', 'land_n', 'directorates',
                    'directorates', 'directorates', 1, 'y'
                ) ON CONFLICT (source_tag, source_table) DO UPDATE SET is_active = 'y', target_tag = 'land_n', target_database_type = 'postgres', source_database_type = 'mysql';
            """)
            cur.execute("""
                INSERT INTO metadata.db_columns_etl (
                    table_id, source_column, source_column_datatype,
                    target_column, target_column_datatype, target_column_length,
                    is_active, column_sequence
                )
                SELECT table_id, 'id', 'integer', 'id', 'integer', 0, 'y', 1
                FROM metadata.db_tables_etl
                WHERE source_tag = 'mysql_membership' AND source_table = 'directorates'
                ON CONFLICT DO NOTHING;
            """)
        conn.commit()
        yield conn
        with conn.cursor() as cur:
            cur.execute("DELETE FROM metadata.db_columns_etl WHERE table_id IN (SELECT table_id FROM metadata.db_tables_etl WHERE source_table = 'directorates');")
            cur.execute("DELETE FROM metadata.db_tables_etl WHERE source_tag = 'mysql_membership';")
        conn.commit()

def test_skip_logic_queries(db_conn):
    # Setup reader
    from src.connections.connection_manager import ConnectionManager
    conn_mgr = ConnectionManager("config/config.local.yaml")
    reader = MetadataReader(conn_mgr)
    
    # 1. Find the table ID for directorates
    cur = db_conn.cursor()
    cur.execute("SELECT table_id FROM metadata.db_tables_etl WHERE source_table='directorates' AND source_tag='mysql_membership'")
    row = cur.fetchone()
    assert row is not None, "directorates table must be promoted in db_tables_etl"
    table_id = row[0]
    
    try:
        # Scenario A: rerun_flag='y' -> Should be returned
        cur.execute("""
            UPDATE metadata.db_tables_etl 
            SET last_success_date = CURRENT_TIMESTAMP, rerun_flag = 'y', is_active = 'y'
            WHERE table_id = %s
        """, [table_id])
        db_conn.commit()
        
        tables = reader.get_tables("mysql_membership", table_name="directorates")
        assert len(tables) == 1, "Should return table when rerun_flag = 'y' even if last_success_date is TODAY"
        
        # Scenario B: rerun_flag='n', last_success_date = TODAY -> Should NOT be returned (skipped)
        cur.execute("""
            UPDATE metadata.db_tables_etl 
            SET last_success_date = CURRENT_TIMESTAMP, rerun_flag = 'n', is_active = 'y'
            WHERE table_id = %s
        """, [table_id])
        db_conn.commit()
        
        tables = reader.get_tables("mysql_membership")
        assert not any(t.source_table == "directorates" for t in tables), "Should skip table when last_success_date is TODAY and rerun_flag is 'n'"
        
        # Scenario C: last_success_date is NULL -> Should be returned
        cur.execute("""
            UPDATE metadata.db_tables_etl 
            SET last_success_date = NULL, rerun_flag = 'n', is_active = 'y'
            WHERE table_id = %s
        """, [table_id])
        db_conn.commit()
        
        tables = reader.get_tables("mysql_membership", table_name="directorates")
        assert len(tables) == 1, "Should return table when last_success_date is NULL"
        
        # Scenario D: last_success_date is in the past -> Should be returned
        yesterday = date.today() - timedelta(days=1)
        cur.execute("""
            UPDATE metadata.db_tables_etl 
            SET last_success_date = %s, rerun_flag = 'n', is_active = 'y'
            WHERE table_id = %s
        """, [yesterday, table_id])
        db_conn.commit()
        
        tables = reader.get_tables("mysql_membership", table_name="directorates")
        assert len(tables) == 1, "Should return table when last_success_date is in the past"
        
    finally:
        # Restore status
        cur.execute("""
            UPDATE metadata.db_tables_etl 
            SET last_success_date = NULL, rerun_flag = 'n', is_reconcile = 'y', 
                reconcile_threshold = 0.0, reconcile_action = 'FAIL'
            WHERE table_id = %s
        """, [table_id])
        db_conn.commit()
        cur.close()

def test_end_to_end_reconciliation_and_idempotency(db_conn):
    # Setup Connection
    cur = db_conn.cursor()
    cur.execute("SELECT table_id FROM metadata.db_tables_etl WHERE source_table='directorates' AND source_tag='mysql_membership'")
    table_id = cur.fetchone()[0]
    
    # 1. Reset state: Null success date, enable reconciliation
    cur.execute("""
        UPDATE metadata.db_tables_etl 
        SET last_success_date = NULL, rerun_flag = 'n', is_active = 'y',
            is_reconcile = 'y', reconcile_threshold = 0.0, reconcile_action = 'FAIL'
        WHERE table_id = %s
    """, [table_id])
    db_conn.commit()
    
    etl = ETL("config/config.local.yaml", schema_certifier=MagicMock())
    etl.validate_metadata = MagicMock()
    etl.conn_manager._configs["mysql_membership"] = ConnectionConfig(
        source_tag="mysql_membership", db_type="mysql", jdbc_url="", username="", credential_reference="", driver="com.mysql.cj.jdbc.Driver"
    )
    
    # Mock Reconciler.count_parquet and Reconciler.count_delta to return exact source count (8) by default
    with patch("src.orchestrator.etl.test_jdbc_connection", return_value=True), \
         patch.object(etl.extractor, 'read', return_value=MagicMock()), \
         patch.object(etl.loader, 'write_landing', return_value="fake_path"), \
         patch.object(Reconciler, 'count_and_cache', return_value=8), \
         patch.object(Reconciler, 'count_parquet', return_value=8), \
         patch.object(Reconciler, 'count_delta', return_value=8):
        
        try:
            # Run 1: First run (Success)
            etl.load_with_db_link(priority=1, target_tag='mysql_membership', table_name='directorates')
            
            # Verify db updated to today and rerun_flag is 'n'
            cur.execute("SELECT last_success_date, rerun_flag FROM metadata.db_tables_etl WHERE table_id = %s", [table_id])
            last_success_date, rerun_flag = cur.fetchone()
            # Convert to date if it's a datetime object
            actual_date = last_success_date.date() if isinstance(last_success_date, datetime) else last_success_date
            assert actual_date == date.today()
            assert rerun_flag == 'n'
            
            # Run 2: Second run (Should be skipped because last_success_date is today and rerun_flag='n')
            # We verify that the table is skipped by checking that it is not returned in the active tables list
            tables = etl.metadata_reader.get_tables("mysql_membership")
            assert not any(t.source_table == "directorates" for t in tables)
            
            # Run 3: Manual override (Rerun)
            cur.execute("UPDATE metadata.db_tables_etl SET rerun_flag = 'y' WHERE table_id = %s", [table_id])
            db_conn.commit()
            
            # Now it should be returned again
            tables = etl.metadata_reader.get_tables("mysql_membership", table_name="directorates")
            assert len(tables) == 1
            
            # Execute run (will reset rerun_flag to 'n')
            etl.load_with_db_link(priority=1, target_tag='mysql_membership', table_name='directorates')
            
            cur.execute("SELECT last_success_date, rerun_flag FROM metadata.db_tables_etl WHERE table_id = %s", [table_id])
            last_success_date, rerun_flag = cur.fetchone()
            actual_date = last_success_date.date() if isinstance(last_success_date, datetime) else last_success_date
            assert actual_date == date.today()
            assert rerun_flag == 'n'
            
            # Run 4: Reconciliation Failure (Self-Healing Retry)
            # Mock count_parquet to simulate a count mismatch (target_count is 999)
            with patch.object(Reconciler, 'count_parquet', return_value=999):
                cur.execute("""
                    UPDATE metadata.db_tables_etl 
                    SET last_success_date = NULL, rerun_flag = 'y'
                    WHERE table_id = %s
                """, [table_id])
                db_conn.commit()
                
                etl.load_with_db_link(priority=1, target_tag='mysql_membership', table_name='directorates')
                
                # Verify self-healing behaviour: last_success_date is NOT updated (table did not succeed),
                # rerun_flag is set to 'y' so the table is automatically retried on the next run.
                cur.execute("SELECT last_success_date, rerun_flag FROM metadata.db_tables_etl WHERE table_id = %s", [table_id])
                last_success_date, rerun_flag = cur.fetchone()
                assert last_success_date is None, "Failure must NOT set last_success_date (table did not succeed)"
                assert rerun_flag == 'y', "Failure must set rerun_flag='y' for automatic retry"
                
                # Verify batch log records RECONCILIATION_FAILED
                cur.execute("""
                    SELECT status FROM metadata.batch_log 
                    WHERE source_table = 'directorates' AND source_tag = 'mysql_membership' AND load_type = 'bronze'
                    ORDER BY id DESC LIMIT 1
                """)
                status = cur.fetchone()[0]
                assert status == RunStatus.RECONCILIATION_FAILED
                
        finally:
            # Restore status
            cur.execute("""
                UPDATE metadata.db_tables_etl 
                SET last_success_date = NULL, rerun_flag = 'n', is_reconcile = 'n'
                WHERE table_id = %s
            """, [table_id])
            db_conn.commit()
            cur.close()


def test_skipped_tables_never_retry_or_duplicate_logs(db_conn):
    """Verify SKIPPED and DEPENDENCY_FAILED tables produce exactly 1 log record and are never retried."""
    import uuid
    etl = ETL("config/config.local.yaml", schema_certifier=MagicMock())
    
    # 1. Insert SKIPPED and DEPENDENCY_FAILED entries for a mock run_id
    run_id = str(uuid.uuid4())
    cur = db_conn.cursor()
    try:
        cur.execute("""
            INSERT INTO metadata.batch_log (
                run_id, source_tag, target_tag, source_table, target_table, load_type, priority,
                start_time, end_time, elapsed_seconds, status, current_phase, error_message
            ) VALUES 
            (%s, 'mysql_membership', 'land_n', 'tbl_skip', 'tbl_skip', 'bronze', 1, NOW(), NOW(), 1.0, 'SKIPPED', 'SKIPPED', 'Table is locked'),
            (%s, 'mysql_membership', 'land_n', 'tbl_dep_fail', 'tbl_dep_fail', 'bronze', 1, NOW(), NOW(), 1.0, 'DEPENDENCY_FAILED', 'DEPENDENCY_FAILED', 'Skipped due to upstream dependency failure.')
        """, [run_id, run_id])
        db_conn.commit()

        # 2. Run retry_failed_tables for this run_id
        with patch.object(etl, '_is_transient_error', wraps=etl._is_transient_error) as mock_is_transient:
            etl.retry_failed_tables(run_id=run_id, max_attempts=2)

        # 3. Assert SKIPPED and DEPENDENCY_FAILED rows were NOT queried for retry
        cur.execute("SELECT COUNT(1) FROM metadata.batch_log WHERE run_id::text = %s AND source_table = 'tbl_skip'", [run_id])
        count_skip = cur.fetchone()[0]
        assert count_skip == 1, "SKIPPED table must have exactly 1 log row per run_id"

        cur.execute("SELECT COUNT(1) FROM metadata.batch_log WHERE run_id::text = %s AND source_table = 'tbl_dep_fail'", [run_id])
        count_dep = cur.fetchone()[0]
        assert count_dep == 1, "DEPENDENCY_FAILED table must have exactly 1 log row per run_id"
    finally:
        cur.execute("DELETE FROM metadata.batch_log WHERE run_id::text = %s", [run_id])
        db_conn.commit()
        cur.close()

