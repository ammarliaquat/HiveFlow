# tests/test_cdc_spark_boundary_capture.py
"""
Tests for governed CDC upper boundary capture via Spark JDBC:
- Verifies Oracle boundary capture routes through Spark JDBC (ojdbc) and never calls get_db_connection.
- Verifies NUMBER(38) / exact DECIMAL text and INT64 preservation.
- Verifies Oracle schema-qualification (opfservices.tblop_dec24) and Oracle 11g ROWNUM = 1 syntax.
- Verifies fail-closed behavior: Spark JDBC failure leaves no CDC attempt window, no target change, and no cursor advancement.
"""

from unittest.mock import MagicMock, patch
from decimal import Decimal
import pytest
import uuid

from src.connections.connection_manager import ConnectionManager
from src.metadata.policy_models import (
    CDCStrategy,
    SourceMutationMode,
    CursorFinalityMode,
    CursorLogicalType,
    ResolvedCDCPolicy,
    CDCCursor,
    ResolvedCDCWindow,
)
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings
from src.cdc.cdc_window_resolver import CDCWindowResolver
from src.exceptions import CDCUpperBoundCaptureFailedError


@pytest.fixture
def mock_conn_cfg_oracle():
    cfg = MagicMock()
    cfg.db_type = "oracle"
    cfg.jdbc_url = "jdbc:oracle:thin:@//192.0.2.10:1521/source_oracle_db"
    cfg.url = "jdbc:oracle:thin:@//192.0.2.10:1521/source_oracle_db"
    cfg.username = "opfservices"
    cfg.password = "secret"
    cfg.driver = "oracle.jdbc.OracleDriver"
    cfg.jdbc_properties = {}
    return cfg


@pytest.fixture
def base_contract():
    policy = ResolvedCDCPolicy.build(
        table_id=166,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("reg_id",),
        cursor_types=(CursorLogicalType.INT64,),
    )
    contract = MagicMock(spec=ResolvedTableContract)
    contract.table_id = 166
    contract.policy_version_id = "v1-uuid"
    contract.version_number = 1
    contract.cdc_policy = policy
    return contract


def test_oracle_boundary_capture_uses_spark_jdbc_and_never_calls_get_db_connection(mock_conn_cfg_oracle, base_contract):
    """
    Verifies that when spark is provided, CDCWindowResolver captures upper bound
    using spark.read.format('jdbc') and does NOT call conn_manager.get_db_connection.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cm.get.return_value = mock_conn_cfg_oracle

    mock_spark = MagicMock()
    mock_reader = MagicMock()
    mock_df = MagicMock()
    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda idx: Decimal("7130437") if idx == 0 else None
    mock_df.first.return_value = mock_row
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mock_spark.read.format.return_value = mock_reader

    resolver = CDCWindowResolver(mock_cm, spark=mock_spark)

    cursor = resolver._capture_source_upper_boundary(
        source_tag="opf_oracle",
        source_table="tblop_dec24",
        cdc_policy=base_contract.cdc_policy,
        source_schema="opfservices",
    )

    # 1. Direct Python driver must NEVER be called
    mock_cm.get_db_connection.assert_not_called()

    # 2. Spark JDBC must be used
    mock_spark.read.format.assert_called_with("jdbc")

    # 3. Query must be schema-qualified
    calls = mock_reader.option.call_args_list
    dbtable_calls = [call for call in calls if call[0][0] == "dbtable"]
    assert len(dbtable_calls) == 1
    dbtable_sql = dbtable_calls[0][0][1]
    assert "opfservices.tblop_dec24" in dbtable_sql
    assert "SELECT MAX(reg_id) AS max_val FROM opfservices.tblop_dec24" in dbtable_sql

    # 4. Cursor value must be correctly parsed
    assert cursor.values == (7130437,)


def test_number38_upper_cursor_preserves_exact_decimal_text(mock_conn_cfg_oracle):
    """
    Verifies that large integer / NUMBER(38) values preserve exact text representation
    without floating-point loss.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cm.get.return_value = mock_conn_cfg_oracle

    mock_spark = MagicMock()
    mock_reader = MagicMock()
    mock_df = MagicMock()
    mock_row = MagicMock()

    # Exact 38-digit integer
    large_num = Decimal("99999999999999999999999999999999999999")
    mock_row.__getitem__.side_effect = lambda idx: large_num if idx == 0 else None
    mock_df.first.return_value = mock_row
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mock_spark.read.format.return_value = mock_reader

    # Policy with DECIMAL cursor type
    policy = ResolvedCDCPolicy.build(
        table_id=166,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("reg_id",),
        cursor_types=(CursorLogicalType.DECIMAL,),
    )

    resolver = CDCWindowResolver(mock_cm, spark=mock_spark)
    cursor = resolver._capture_source_upper_boundary(
        source_tag="opf_oracle",
        source_table="tblop_dec24",
        cdc_policy=policy,
        source_schema="opfservices",
    )

    assert cursor.values == ("99999999999999999999999999999999999999",)


def test_oracle_schema_qualified_query_and_11g_timestamp_syntax(mock_conn_cfg_oracle):
    """
    Verifies that for Oracle timestamp CDC, the query is schema-qualified and uses
    Oracle 11g compatible subquery with ROWNUM = 1, rather than 12c+ FETCH FIRST 1 ROW ONLY.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cm.get.return_value = mock_conn_cfg_oracle

    mock_spark = MagicMock()
    mock_reader = MagicMock()
    mock_df = MagicMock()
    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda idx: "2026-09-14 17:00:00" if idx == 0 else (100 if idx == 1 else None)
    mock_row.__len__.return_value = 2
    mock_df.first.return_value = mock_row
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mock_spark.read.format.return_value = mock_reader

    policy = ResolvedCDCPolicy.build(
        table_id=166,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("reg_date", "reg_id"),
        cursor_types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        tie_breaker_columns=("reg_id",),
    )

    resolver = CDCWindowResolver(mock_cm, spark=mock_spark)
    cursor = resolver._capture_source_upper_boundary(
        source_tag="opf_oracle",
        source_table="tblop_dec24",
        cdc_policy=policy,
        source_schema="opfservices",
    )

    calls = mock_reader.option.call_args_list
    dbtable_calls = [call for call in calls if call[0][0] == "dbtable"]
    dbtable_sql = dbtable_calls[0][0][1]

    # Must contain Oracle 11g ROWNUM = 1 syntax
    assert "ROWNUM = 1" in dbtable_sql
    # Must NOT contain Oracle 12c+ FETCH FIRST syntax
    assert "FETCH FIRST" not in dbtable_sql
    # Must contain fully-qualified schema and table
    assert "opfservices.tblop_dec24" in dbtable_sql


def test_spark_boundary_query_failure_fail_closed(mock_conn_cfg_oracle, base_contract):
    """
    Verifies fail-closed semantics:
    If Spark JDBC fails to capture H, an exception is raised with the Spark cause chained,
    and NO attempt window is persisted in metadata.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cm.get.return_value = mock_conn_cfg_oracle

    mock_spark = MagicMock()
    mock_reader = MagicMock()
    spark_jdbc_error = RuntimeError("java.sql.SQLException: ORA-01017: invalid username/password")
    mock_reader.option.return_value = mock_reader
    mock_reader.load.side_effect = spark_jdbc_error
    mock_spark.read.format.return_value = mock_reader

    # Mock metadata connection for loading committed cursor
    mock_meta_conn = MagicMock()
    mock_meta_cur = MagicMock()
    mock_meta_cur.fetchone.return_value = None  # No prior window
    mock_meta_conn.cursor.return_value.__enter__.return_value = mock_meta_cur
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_meta_conn

    resolver = CDCWindowResolver(mock_cm, spark=mock_spark)

    # Calling resolve_and_persist_attempt_window must fail-closed
    with pytest.raises(CDCUpperBoundCaptureFailedError) as exc_info:
        resolver.resolve_and_persist_attempt_window(
            run_id=str(uuid.uuid4()),
            table_id=166,
            attempt_number=1,
            contract=base_contract,
            source_tag="opf_oracle",
            source_schema="opfservices",
            source_table="tblop_dec24",
        )

    assert "Failed to capture source upper boundary on opf_oracle.opfservices.tblop_dec24" in str(exc_info.value)
    assert exc_info.value.__cause__ is spark_jdbc_error

    # Ensure no insert into the actual CDC window ledger or cursor update occurred.
    for call in mock_meta_cur.execute.call_args_list:
        executed_sql = call[0][0].upper()
        assert "INSERT INTO METADATA.CDC_ATTEMPT_WINDOW" not in executed_sql
        assert "UPDATE METADATA.TABLE_CDC_STATE" not in executed_sql


def test_oracle_timestamp_filter_uses_unquoted_identifiers(mock_conn_cfg_oracle):
    """Normal Oracle identifiers must not become case-sensitive lower-case names."""
    policy = ResolvedCDCPolicy.build(
        table_id=166,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("reg_date", "reg_id"),
        cursor_types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        tie_breaker_columns=("reg_id",),
    )
    window = ResolvedCDCWindow(
        window_id="window",
        run_id="run",
        table_id=166,
        attempt_number=1,
        policy_version_id="policy",
        cdc_policy_fingerprint="fingerprint",
        strategy=policy.strategy,
        lower_cursor=CDCCursor(
            1, policy.strategy, policy.cursor_columns, policy.cursor_types,
            ("2026-09-13 00:00:00", 10),
        ),
        upper_cursor=CDCCursor(
            1, policy.strategy, policy.cursor_columns, policy.cursor_types,
            ("2026-09-14 00:00:00", 20),
        ),
        window_fingerprint="window-fingerprint",
        captured_at=None,
    )

    sql = CDCWindowResolver(MagicMock()).build_extraction_filter_sql(
        window, dialect="oracle", overlap_days=7,
    )

    assert '"reg_date"' not in sql
    assert '"reg_id"' not in sql
    assert "reg_date" in sql
    assert "reg_id" in sql


def test_fallback_to_direct_connection_when_spark_is_none(mock_conn_cfg_oracle, base_contract):
    """
    Verifies that when spark is None (e.g. offline unit test), CDCWindowResolver
    falls back to get_db_connection and still applies proper schema qualification.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cm.get.return_value = mock_conn_cfg_oracle

    mock_db_conn = MagicMock()
    mock_db_cur = MagicMock()
    mock_db_cur.fetchone.return_value = (5000,)
    mock_db_conn.cursor.return_value.__enter__.return_value = mock_db_cur
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_db_conn

    # Spark is None
    resolver = CDCWindowResolver(mock_cm, spark=None)
    cursor = resolver._capture_source_upper_boundary(
        source_tag="opf_oracle",
        source_table="tblop_dec24",
        cdc_policy=base_contract.cdc_policy,
        source_schema="opfservices",
    )

    mock_cm.get_db_connection.assert_called_once_with("opf_oracle", expected_role="source")
    executed_sql = mock_db_cur.execute.call_args[0][0]
    assert "opfservices.tblop_dec24" in executed_sql
    assert cursor.values == (5000,)
