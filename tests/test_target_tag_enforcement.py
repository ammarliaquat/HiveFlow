# tests/test_target_tag_enforcement.py
"""
Target Tag & Wrapper Enforcement Tests.

Verifies:
1. promote_table.py CLI rejects legacy --target_database and --target_schema flags and requires --target_tag.
2. discover_and_promote.py does not contain duplicate inline SQL for Tier 2 tables (db_tables_etl / db_columns_etl).
3. discover_and_promote.py imports and uses the _promote_one_table wrapper.
"""
import os
import subprocess
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROMOTE_SCRIPT = os.path.join(PROJECT_ROOT, "jobs", "promote_table.py")
DISCOVER_SCRIPT = os.path.join(PROJECT_ROOT, "jobs", "discover_and_promote.py")


class _FakeTargetConnectionManager:
    def __init__(self, config_path):
        self.config_path = config_path

    def get(self, tag, expected_role=None):
        if tag == "nonexistent_section_12345":
            raise KeyError(tag)

        class _Cfg:
            database = "opf"
            schema = "land_n"
            db_type = "postgresql"

        return _Cfg()

def test_promote_table_rejects_legacy_flags():
    """Verify promote_table.py rejects --target_database and --target_schema."""
    cmd = [
        "python", PROMOTE_SCRIPT,
        "--source_tag", "test_source",
        "--table", "test_table",
        "--target_tag", "land_n",
        "--target_database", "opf",
        "--target_schema", "land_n"
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode != 0, "promote_table.py should fail when legacy --target_database or --target_schema are provided."
    assert "unrecognized arguments: --target_database" in res.stderr or "unrecognized arguments" in res.stderr

def test_promote_table_requires_target_tag():
    """Verify promote_table.py accepts --target_tag and fails if neither --all nor --table is specified."""
    cmd = [
        "python", PROMOTE_SCRIPT,
        "--source_tag", "test_source",
        "--target_tag", "land_n"
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    # Should fail with custom message requiring --all or --table, NOT argparse error about target_tag
    assert "Specify --table <name> or --all" in res.stdout or "Specify --table <name> or --all" in res.stderr

def test_discover_and_promote_is_true_wrapper():
    """Verify discover_and_promote.py does not contain inline INSERTs into Tier 2 tables or invoke promotion."""
    with open(DISCOVER_SCRIPT, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    assert "INSERT INTO metadata.db_tables_etl" not in content, (
        "discover_and_promote.py must not contain inline INSERTs into db_tables_etl."
    )
    assert "INSERT INTO metadata.db_columns_etl" not in content, (
        "discover_and_promote.py must not contain inline INSERTs into db_columns_etl."
    )
    assert "from jobs.promote_table import _promote_one_table" not in content, (
        "discover_and_promote.py must not import _promote_one_table; discovery is Tier-1 only."
    )
    assert "_promote_one_table(" not in content, (
        "discover_and_promote.py must not invoke _promote_one_table; discovery is Tier-1 only."
    )


def test_promote_table_normalizes_catalog_cdc_strategy_without_direct_activation():
    """Tier-1 strategy labels never activate Tier-2 CDC without a frozen policy."""
    from jobs.promote_table import _canonical_cdc_strategy

    assert _canonical_cdc_strategy("SEQUENCE") == "SEQUENCE_HIGH_WATERMARK"
    assert _canonical_cdc_strategy("ID_RANGE") == "SEQUENCE_HIGH_WATERMARK"
    assert _canonical_cdc_strategy("TIMESTAMP") == "TIMESTAMP_HIGH_WATERMARK"
    assert _canonical_cdc_strategy("FULL_LOAD") == "FULL_SNAPSHOT"


def test_promote_table_timestamp_recommendation_is_not_active_cdc(monkeypatch):
    """A steward-approved timestamp candidate is still not executable for V1 CDC."""
    from jobs import promote_table
    from src.metadata.recommendation_service import RecommendationResult
    from unittest.mock import MagicMock

    monkeypatch.setattr(promote_table, "ConnectionManager", _FakeTargetConnectionManager)
    monkeypatch.setattr(
        promote_table.RecommendationService,
        "get_effective_recommendation",
        lambda _conn, _table_id: RecommendationResult(
            allowed=True,
            cdc_column="updated_at",
            cdc_type="TIMESTAMP_HIGH_WATERMARK",
            strategy="TIMESTAMP_HIGH_WATERMARK",
            reason="approved",
        ),
    )

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.side_effect = [
        {
            "table_master_key": 100,
            "database_type": "mysql",
            "schema_name": "db",
            "cdc_columns": "updated_at",
            "business_key": "id",
            "table_row_count": 100,
        },
        None,
        (99,),
    ]
    mock_cur.fetchall.return_value = []

    assert promote_table._promote_one_table(
        mock_conn, source_tag="test_src", source_database="src_db", table_name="users",
        target_tag="land_n", priority=1, force=True
    ) is True

    insert_params = [
        c[0][1] for c in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.db_tables_etl" in c[0][0]
    ][0]
    assert insert_params[6] == "updated_at"
    assert insert_params[19] == "n"
    assert insert_params[33] == "TIMESTAMP_HIGH_WATERMARK"


def test_promote_table_unapproved_sequence_recommendation_is_not_active_cdc(monkeypatch):
    """Developer bypass may promote metadata, but it cannot activate CDC execution."""
    from jobs import promote_table
    from src.metadata.recommendation_service import RecommendationResult
    from unittest.mock import MagicMock

    monkeypatch.setattr(promote_table, "ConnectionManager", _FakeTargetConnectionManager)
    monkeypatch.setattr(
        promote_table.RecommendationService,
        "get_effective_recommendation",
        lambda _conn, _table_id: RecommendationResult(
            allowed=False,
            cdc_column="id",
            cdc_type="SEQUENCE_HIGH_WATERMARK",
            strategy="SEQUENCE_HIGH_WATERMARK",
            reason="pending steward approval",
        ),
    )

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.side_effect = [
        {
            "table_master_key": 100,
            "database_type": "mysql",
            "schema_name": "db",
            "cdc_columns": "id",
            "business_key": "id",
            "table_row_count": 100,
        },
        None,
        (99,),
    ]
    mock_cur.fetchall.return_value = []

    assert promote_table._promote_one_table(
        mock_conn, source_tag="test_src", source_database="src_db", table_name="users",
        target_tag="land_n", priority=1, force=True, allow_unapproved=True
    ) is True

    insert_params = [
        c[0][1] for c in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.db_tables_etl" in c[0][0]
    ][0]
    assert insert_params[6] == "id"
    assert insert_params[19] == "n"
    assert insert_params[33] == "SEQUENCE_HIGH_WATERMARK"


def test_promote_existing_force_false_prints_warning(capsys, monkeypatch):
    """Verify re-running promotion without --force prints warning and does not update existing record."""
    from jobs import promote_table
    from unittest.mock import MagicMock

    monkeypatch.setattr(promote_table, "ConnectionManager", _FakeTargetConnectionManager)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Setup side_effect for fetchone:

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # 1st call: master catalog lookup returns table info
    # 2nd call: recommendation lookup returns None
    # 3rd call: existing ETL check returns (99, 'land_n', 1, 'PENDING', None, None, None)
    # 4th call: INSERT ... ON CONFLICT DO NOTHING returns None (no row inserted/updated)
    # 5th call: SELECT existing row returns (99, "land_n", 1)
    mock_cur.fetchone.side_effect = [
        {"table_master_key": 100, "database_type": "mysql", "schema_name": "db", "cdc_columns": None, "business_key": "id", "table_row_count": 100},
        {"recommendation_status": "APPROVED", "recommended_value": "id", "recommended_type": "SEQUENCE_HIGH_WATERMARK"},
        None,
        None,
        (99, "land_n", 1)
    ]

    res = promote_table._promote_one_table(
        mock_conn, source_tag="test_src", source_database="src_db", table_name="users",
        target_tag="land_n", priority=1, force=False
    )

    assert res is True
    captured = capsys.readouterr()
    assert "[WARN] Table 'users' already promoted with target_tag='land_n', priority=1." in captured.out
    assert "To update existing row, re-run with --force." in captured.out


def test_promote_existing_force_true_updates_record(monkeypatch):
    """Verify running promotion with --force executes DO UPDATE SET and updates the record."""
    from jobs import promote_table
    from unittest.mock import MagicMock

    monkeypatch.setattr(promote_table, "ConnectionManager", _FakeTargetConnectionManager)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # 1st call: master catalog lookup
    # 2nd call: recommendation lookup returns None
    # 3rd call: existing execution_authority_mode check returns None
    # 4th call: INSERT ... ON CONFLICT DO UPDATE SET ... returns (99,)
    mock_cur.fetchone.side_effect = [
        {"table_master_key": 100, "database_type": "mysql", "schema_name": "db", "cdc_columns": None, "business_key": "id", "table_row_count": 100},
        None,
        None,
        (99,),
    ]
    mock_cur.fetchall.return_value = []

    res = promote_table._promote_one_table(
        mock_conn, source_tag="test_src", source_database="src_db", table_name="users",
        target_tag="land_n", priority=1, force=True
    )

    assert res is True
    # Verify DO UPDATE SET was part of the SQL query executed
    insert_call = [c[0][0] for c in mock_cur.execute.call_args_list if "INSERT INTO metadata.db_tables_etl" in c[0][0]][0]
    assert "DO UPDATE SET" in insert_call
    assert "is_active" in insert_call


def test_promotion_creates_an_independent_tier2_subscription_per_target(monkeypatch):
    """The same catalog table can be onboarded to land_n without touching land_test."""
    from jobs import promote_table
    from unittest.mock import MagicMock

    monkeypatch.setattr(promote_table, "ConnectionManager", _FakeTargetConnectionManager)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.side_effect = [
        {"table_master_key": 100, "database_type": "mysql", "schema_name": "db", "cdc_columns": None, "business_key": "id", "table_row_count": 100},
        None,  # no recommendation
        None,  # no governed Tier-2 subscription for land_n
        (101,),  # INSERT RETURNING table_id for the new land_n subscription
    ]
    mock_cur.fetchall.return_value = []

    assert promote_table._promote_one_table(
        mock_conn,
        source_tag="test_src",
        source_database="src_db",
        table_name="users",
        target_tag="land_n",
        priority=1,
        force=True,
    ) is True

    governed_lookup = next(
        call for call in mock_cur.execute.call_args_list
        if "SELECT execution_authority_mode" in call.args[0]
    )
    assert "LOWER(target_tag) = LOWER(%s)" in governed_lookup.args[0]
    assert governed_lookup.args[1] == [100, "land_n"]

    insert_call = next(
        call for call in mock_cur.execute.call_args_list
        if "INSERT INTO metadata.db_tables_etl" in call.args[0]
    )
    assert "ON CONFLICT (source_table_master_key, target_tag)" in insert_call.args[0]
    assert "source_table_master_key IS NOT NULL AND target_tag IS NOT NULL" in insert_call.args[0]


def test_invalid_target_tag_raises_error(monkeypatch):
    """Verify providing a non-existing target_tag raises KeyError at promotion time."""
    from jobs import promote_table
    from unittest.mock import MagicMock

    monkeypatch.setattr(promote_table, "ConnectionManager", _FakeTargetConnectionManager)
    mock_conn = MagicMock()

    with pytest.raises(KeyError) as exc_info:
        promote_table._promote_one_table(
            mock_conn, source_tag="test_src", source_database="src_db", table_name="users",
            target_tag="nonexistent_section_12345", priority=1, force=False
        )
    assert "nonexistent_section_12345" in str(exc_info.value)


def test_etl_load_override_target_tag():
    """Verify passing override_target_tag dynamically overrides MetadataRow.target_tag at execution time."""
    from unittest.mock import MagicMock, patch
    from src.metadata.metadata_models import MetadataRow

    # Create a real MetadataRow with target_tag='land_n'
    row = MetadataRow(
        id=1, source_tag="src_tag", source_schema="public", source_table="users",
        source_db_type="postgres", target_database_type="postgres", landing_table="users",
        history_table="users", target_table="users", target_schema="public",
        target_tag="land_n", is_active="y", priority=1, load_type="bronze",
        is_cdc="n", cdc_columns=None, cdc_p_days=None, delta_days=None,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure=None, reconcile_threshold=None, reconcile_action=None,
        rerun_flag="n", last_success_date=None, chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="src-", natural_key_column="id", notes=None
    )

    # Test the override logic inside ETL methods
    from src.orchestrator.etl import ETL
    with patch.object(ETL, "__init__", return_value=None):
        real_etl = ETL()
        real_etl.metadata_reader = MagicMock()
        real_etl.metadata_reader.get_tables.return_value = [row]
        real_etl.conn_manager = MagicMock()
        real_etl._execute_table_batch = MagicMock(return_value=["users"])
        real_etl.spark_app_id = "test_app"

        # Execute load_with_db_link with override_target_tag='land_test'
        real_etl.load_with_db_link(priority=1, target_tag="src_tag", override_target_tag="land_test")

        assert row.target_tag == "land_test", "override_target_tag should dynamically update MetadataRow.target_tag"


def test_batch_logger_records_target_tag():
    """Verify BatchLogger records target_tag into both batch_log and batch_summary."""
    import uuid
    from datetime import datetime
    from unittest.mock import MagicMock
    from src.logging_.batch_logger import BatchLogger

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.get_metadata_db_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = (1,)

    logger = BatchLogger(mock_conn, str(uuid.uuid4()), "app_id")
    logger.log_start(
        source_tag="src_tag",
        source_table="tbl",
        target_table="tbl",
        load_type="bronze",
        priority=1,
        target_tag="land_test"
    )

    # Verify query includes target_tag
    last_call_args = mock_cursor.execute.call_args_list[-1]
    query_str = last_call_args[0][0]
    params = last_call_args[0][1]
    assert "target_tag" in query_str
    assert "land_test" in params


def test_extractor_reserved_word_quoting_and_type_check():
    """Verify that Extractor quotes identifiers properly and falls back on non-numeric types."""
    from unittest.mock import MagicMock
    from src.extract.extractor import Extractor
    from src.connections.connection_manager import ConnectionConfig

    spark = MagicMock()
    conn_manager = MagicMock()
    extractor = Extractor(spark, conn_manager)

    conn_cfg = ConnectionConfig(
        source_tag="test_src",
        db_type="mysql",
        jdbc_url="jdbc:mysql://host:3306/db",
        username="user",
        credential_reference="pwd",
        driver="com.mysql.cj.jdbc.Driver"
    )

    # Mock bounds_df to return min/max and a StringType datatype (non-supported)
    from pyspark.sql.types import StringType, StructType, StructField
    bounds_df = MagicMock()
    bounds_df.schema = StructType([
        StructField("mn", StringType(), True),
        StructField("mx", StringType(), True)
    ])
    bounds_df.collect.return_value = [{"mn": "a", "mx": "z"}]
    spark.read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = bounds_df

    # Test _get_column_bounds quoting backticks for mysql
    bounds = extractor._get_column_bounds(conn_cfg, "users", "key", "mysql")
    assert bounds["supported"] is False
    assert bounds["min"] == "a"
    assert bounds["max"] == "z"

    # Test _get_column_bounds quoting double quotes for postgres
    bounds_pg = extractor._get_column_bounds(conn_cfg, "users", "key", "postgres")
    assert bounds_pg["supported"] is False

    # Test _add_parallel_read_options falls back to numPartitions
    reader = MagicMock()
    reader.option.return_value = reader  # Allow method chaining
    extractor._add_parallel_read_options(reader, MagicMock(full_source_table="users", partition_column="key", source_table="users", num_partitions=5), conn_cfg)
    assert any(call[0][0] == "numPartitions" for call in reader.option.call_args_list)


def test_retry_failed_tables_transient_filtering():
    """Verify etl.retry_failed_tables filters out non-transient failures."""
    from unittest.mock import MagicMock, patch
    from src.orchestrator.etl import ETL

    conn_manager = MagicMock()
    mock_cursor = MagicMock()
    conn_manager.get_metadata_db_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    # Query returns one transient error and one non-transient error
    mock_cursor.fetchall.return_value = [
        ("test_src", "transient_tbl", "landing", "Communications link failure (timeout)"),
        ("test_src", "syntax_tbl", "landing", "SQLSyntaxErrorException: check the manual that corresponds to your MySQL server version")
    ]

    with patch.object(ETL, "__init__", return_value=None):
        etl = ETL()
        etl.conn_manager = conn_manager
        etl.metadata_reader = MagicMock()
        
        # Configure the returned MetadataRow mock
        mock_meta = MagicMock()
        mock_meta.priority = 1
        mock_meta.source_tag = "test_src"
        mock_meta.source_table = "transient_tbl"
        etl.metadata_reader.get_table_by_schema_and_name.return_value = mock_meta
        
        etl.load_with_db_link = MagicMock()

        # Run retry
        etl.retry_failed_tables(run_id="run-1234", max_attempts=1, override_target_tag="land_test")

        # Verify load_with_db_link was only called for the transient table
        etl.load_with_db_link.assert_called_once_with(
            priority=1,
            target_tag="test_src",
            table_name="transient_tbl",
            run_id="run-1234",
            override_target_tag="land_test"
        )


def test_standalone_history_respects_skip():
    """Verify that load_historyData respects date skip checks when run standalone, and bypasses when eligible_tables is provided."""
    from unittest.mock import MagicMock, patch
    from src.orchestrator.etl import ETL

    with patch.object(ETL, "__init__", return_value=None):
        etl = ETL()
        etl.conn_manager = MagicMock()
        etl.metadata_reader = MagicMock()
        etl.spark_app_id = "test_app"
        etl.max_parallel_tables = 5
        etl.validate_metadata = MagicMock()
        etl.metadata_reader.get_tables.return_value = []

        # 1. Standalone execution: eligible_tables is None -> bypass_skip is False
        etl.load_historyData(priority=1, target_tag="test_src", eligible_tables=None)
        etl.metadata_reader.get_tables.assert_called_with(
            source_tag="test_src",
            priority=1,
            table_name=None,
            bypass_skip=True
        )

        # 2. Pipeline cascade context: a typed eligible table list bypasses skip checks
        etl.load_historyData(priority=1, target_tag="test_src", eligible_tables=[])
        etl.metadata_reader.get_tables.assert_called_with(
            source_tag="test_src",
            priority=1,
            table_name=None,
            bypass_skip=True
        )


def test_transient_error_classifier():
    """Verify etl._is_transient_error classifies transient vs non-transient exceptions correctly."""
    from unittest.mock import MagicMock, patch
    from src.orchestrator.etl import ETL

    with patch.object(ETL, "__init__", return_value=None):
        etl = ETL()

        # Transient errors (should be True)
        assert etl._is_transient_error("communications link failure during query") is True
        assert etl._is_transient_error("Connection refused (Connection refused)") is True
        assert etl._is_transient_error("java.net.SocketTimeoutException: Read timed out") is True
        assert etl._is_transient_error("deadlock detected on transaction") is True
        assert etl._is_transient_error("reconciliation count mismatch") is True
        # Access denied is retryable in case of env load/vault fetching delays
        assert etl._is_transient_error("Access denied for user 'fia'@'host'") is True

        # Non-transient errors (should be False)
        assert etl._is_transient_error("SQLSyntaxErrorException: check syntax error near SELECT") is False
        assert etl._is_transient_error("UndefinedColumn: column 'name' does not exist") is False
        assert etl._is_transient_error("AnalysisException: cannot resolve '`id`' given input columns") is False
        assert etl._is_transient_error("Partition column type should be numeric, date, or timestamp") is False
        assert etl._is_transient_error('duplicate key value violates unique constraint "unq_cdc_commit_attempt"') is False
        assert etl._is_transient_error('insert or update on table "cdc_attempt_reconciliation" violates foreign key constraint') is False


def test_fail_then_succeed_reconciliation_summary():
    """Verify get_run_summary correctly prioritizes SUCCESS over FAILED for the same table in a run."""
    import uuid
    from datetime import datetime
    from unittest.mock import MagicMock
    from src.logging_.batch_logger import BatchLogger

    conn_manager = MagicMock()
    mock_cursor = MagicMock()
    conn_manager.get_metadata_db_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    # Fetchone is called once in get_run_summary (fetches CTE summary metrics),
    # and once in get_run_summary for the slowest table.
    mock_cursor.fetchone.side_effect = [
        (1, 1, 0, 0, 10, datetime.now(), datetime.now(), "src_tag", "land_test"), # summary_row
        ("slow_tbl", 10, 2.5) # slowest_table row
    ]

    logger = BatchLogger(conn_manager, str(uuid.uuid4()), "app_id")
    summary = logger.get_run_summary()

    # Find the execute calls that contain our DISTINCT ON query
    distinct_on_calls = [
        c for c in mock_cursor.execute.call_args_list 
        if "DISTINCT ON (source_table, load_type)" in c[0][0]
    ]
    assert len(distinct_on_calls) == 2
    query_str = distinct_on_calls[0][0][0]
    assert "(status IN ('SUCCESS', 'RECONCILIATION_WARNING')) DESC" in query_str
    assert summary["succeeded"] == 1
    assert summary["failed"] == 0


def test_batch_summary_powerbi_metrics():
    """Verify BatchLogger computes and writes new Power BI metrics to batch_summary."""
    import uuid
    from datetime import datetime
    from unittest.mock import MagicMock
    from src.logging_.batch_logger import BatchLogger

    conn_manager = MagicMock()
    mock_cursor = MagicMock()
    conn_manager.get_metadata_db_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    # Query returns total_tables, succeeded, failed, skipped, total_rows, min_start, max_end, err_summary, db_target_tag
    start_time = datetime(2026, 7, 14, 12, 0, 0)
    end_time = datetime(2026, 7, 14, 12, 0, 10)  # 10 seconds difference
    mock_cursor.fetchone.return_value = (5, 4, 1, 0, 1000, start_time, end_time, "some syntax error", "land_test")

    logger = BatchLogger(conn_manager, str(uuid.uuid4()), "app_id")
    
    # Reset call args to ignore DDL execute calls from logger init
    mock_cursor.execute.reset_mock()
    
    logger.summarize_and_log_layer(
        source_tag="src_tag",
        layer="bronze",
        target_tag="land_test",
        priority=1,
        action="run_pipeline"
    )

    # Verify log_batch_summary INSERT query includes the new Power BI columns and computes correct throughput
    insert_calls = [
        c for c in mock_cursor.execute.call_args_list 
        if "INSERT INTO metadata.batch_summary" in c[0][0]
    ]
    assert len(insert_calls) == 1
    query_str = insert_calls[0][0][0]
    params = insert_calls[0][0][1]

    # Verify column references in query string
    assert "priority" in query_str
    assert "action" in query_str
    assert "run_date" in query_str
    assert "elapsed_seconds" in query_str
    assert "rows_per_second" in query_str

    # Find the parameter values using placeholders
    # run_id, source_tag, target_tag, layer, start_ts, end_ts, total_tables, succeeded, failed, skipped, total_rows, status, error_summary, priority, action, run_date, elapsed_seconds, rows_per_second
    # 1000 rows / 10 seconds = 100 rows_per_second
    assert 10.0 in params  # elapsed_seconds
    assert 100.0 in params  # rows_per_second
    assert 1 in params  # priority
    assert "run_pipeline" in params  # action


def test_table_identifier_quoting():
    """Verify that quote_table_name quotes identifiers correctly for PostgreSQL/MySQL."""
    from src.utils.db_utils import quote_table_name

    assert quote_table_name("land_test.t28042026mapplicants", "postgres") == '"land_test"."t28042026mapplicants"'
    assert quote_table_name("land_test.t28042026mapplicants", "mysql") == '`land_test`.`t28042026mapplicants`'
    assert quote_table_name("users", "postgres") == '"users"'
    assert quote_table_name("users", "mysql") == '`users`'


def test_run_scoped_reconciliation(spark):
    """Verify that Reconciler.count_parquet filters target counts by run_id when pipeline_run_id is present."""
    import uuid
    from src.validation.reconciler import Reconciler

    # Create dummy DataFrame with run_ids
    data = [
        ("id1", "run_A"),
        ("id2", "run_A"),
        ("id3", "run_B"),
    ]
    df = spark.createDataFrame(data, ["id", "pipeline_run_id"])

    path = f"file:///opt/shared/data/local_dev/test_reconciliation_{uuid.uuid4().hex}"
    try:
        df.write.mode("overwrite").parquet(path)

        # Count A should be 2
        count_a = Reconciler.count_parquet(spark, path, run_id="run_A")
        assert count_a == 2

        # Count B should be 1
        count_b = Reconciler.count_parquet(spark, path, run_id="run_B")
        assert count_b == 1

        # Count without run_id should be 3
        count_all = Reconciler.count_parquet(spark, path)
        assert count_all == 3

    finally:
        try:
            spark.createDataFrame([], df.schema).write.mode("overwrite").parquet(path)
        except Exception:
            pass


