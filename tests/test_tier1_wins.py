# tests/test_tier1_wins.py
"""
Tests for Tier 1 Operational Improvements:
1. Framework Metadata Validator (`src/validation/metadata_validator.py`)
2. On-Demand Execution Summary Dashboard (`src/logging_/batch_logger.py`)
3. Modular Health Checker (`src/validation/health_checker.py`)
4. Unified CLI Integration (`jobs/app_etl.py`)
"""
import pytest
import uuid
from datetime import datetime
from unittest.mock import patch, MagicMock
from src.connections.connection_manager import ConnectionManager
from src.validation.metadata_validator import MetadataValidator
from src.validation.health_checker import HealthChecker
from src.logging_.batch_logger import BatchLogger
from src.constants import MetadataConstants, RunStatus

pytestmark = pytest.mark.live


@pytest.fixture
def conn_manager():
    return ConnectionManager("config/config.local.yaml")


# ── 1. MetadataValidator Tests ────────────────────────────────────────────────
def test_metadata_validator_valid_metadata(conn_manager):
    validator = MetadataValidator(conn_manager)
    is_valid, errors, warnings = validator.validate(source_tag="mysql_membership", quiet=True)
    # Our test db has mysql_membership configured
    assert isinstance(is_valid, bool)
    assert isinstance(errors, list)
    assert isinstance(warnings, list)


def test_metadata_validator_detects_defects(conn_manager):
    """Test validator catches duplicate active tables and missing CDC keys."""
    validator = MetadataValidator(conn_manager)

    with conn_manager.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            # Insert a temporary malformed CDC table
            cur.execute("""
                INSERT INTO metadata.db_tables_etl (
                    source_tag, source_database, source_database_type, source_schema, source_table,
                    target_tag, target_database, target_database_type, target_schema, target_table,
                    landing_table, history_table, priority, is_active, table_type, is_cdc, cdc_columns
                ) VALUES (
                    'mysql_membership', 'test', 'mysql', 'test', 'bad_cdc_table',
                    'land_n', 'test', 'postgres', 'test', 'bad_cdc_table',
                    'bad_cdc_table', 'bad_cdc_table', 1, 'y', 'cdc', 'y', ''
                ) ON CONFLICT (source_tag, source_table) DO UPDATE
                SET is_active = 'y', table_type = 'cdc', is_cdc = 'y', cdc_columns = '';
            """)
        conn.commit()

    try:
        is_valid, errors, _ = validator.validate(source_tag="mysql_membership", quiet=True)
        assert not is_valid
        assert any("bad_cdc_table" in err and "cdc_columns is missing" in err for err in errors)
    finally:
        with conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM metadata.db_tables_etl WHERE source_table = 'bad_cdc_table';")
            conn.commit()


# ── 2. BatchLogger Run Summary Tests ──────────────────────────────────────────
def test_batch_logger_run_summary(conn_manager, capsys):
    test_run_id = str(uuid.uuid4())
    logger = BatchLogger(conn_manager, test_run_id, "test_app")

    # Insert mock entries into batch_log
    log_id_1 = logger.log_start("test_src", "table_a", "table_a", "bronze", 1)
    logger.log(
        source_tag="test_src",
        source_table="table_a",
        target_table="table_a",
        load_type="bronze",
        priority=1,
        source_count=10000,
        target_count=10000,
        start_time=datetime(2026, 7, 10, 10, 0, 0),
        end_time=datetime(2026, 7, 10, 10, 0, 2),
        status=RunStatus.SUCCESS,
        log_id=log_id_1,
    )
    log_id_2 = logger.log_start("test_src", "table_b", "table_b", "bronze", 1)
    logger.log(
        source_tag="test_src",
        source_table="table_b",
        target_table="table_b",
        load_type="bronze",
        priority=1,
        source_count=500,
        target_count=500,
        start_time=datetime(2026, 7, 10, 10, 0, 2),
        end_time=datetime(2026, 7, 10, 10, 0, 3),
        status=RunStatus.FAILED,
        error_message="Connection timeout",
        log_id=log_id_2,
    )

    summary = logger.get_run_summary(test_run_id)
    assert summary["run_id"] == test_run_id
    assert summary["total_tables"] == 2
    assert summary["succeeded"] == 1
    assert summary["failed"] == 1
    assert summary["total_rows"] == 10500
    assert summary["slowest_table"] is not None
    assert summary["slowest_table"]["table"] == "table_a"
    assert "5.0K rows/sec" in summary["slowest_table"]["rate_str"]

    # Test print formatting
    logger.print_execution_summary(test_run_id)
    captured = capsys.readouterr()
    assert "PIPELINE EXECUTION SUMMARY" in captured.out
    assert "table_a (10,000 rows" in captured.out
    assert "Connection timeout" in captured.out


# ── 3. Modular HealthChecker Tests ────────────────────────────────────────────
def test_health_checker_modular_methods(conn_manager, capsys):
    hc = HealthChecker(conn_manager)

    rep_ok, rep_msg = hc.check_repository_db()
    assert rep_ok
    assert rep_msg == "OK"

    ver_ok, ver_msg = hc.check_schema_version()
    assert ver_ok, f"Schema version check failed with message: {ver_msg}"

    spk_ok, spk_msg = hc.check_spark_connect()
    assert spk_ok
    assert "SKIPPED" in spk_msg

    # Test run_all_checks output formatting
    result = hc.run_all_checks(source_tag="mysql_membership", quiet=False)
    captured = capsys.readouterr()
    assert "Pipeline Health" in captured.out
    assert "Repository DB        [ OK ]" in captured.out
    assert "Schema Version       [ OK ] (3.0.0)" in captured.out
    assert "Metadata Validation" in captured.out


# ── 4. CLI Argument Parsing Tests ─────────────────────────────────────────────
def test_app_etl_cli_parser():
    from jobs.app_etl import build_parser

    parser = build_parser()

    # Test --validate-only
    args = parser.parse_args(["--validate-only"])
    assert args.validate_only is True

    # Test healthcheck action
    args2 = parser.parse_args(["--action", "healthcheck"])
    assert args2.action == "healthcheck"
