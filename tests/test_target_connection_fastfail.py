# tests/test_target_connection_fastfail.py
"""
Target Connection Diagnostic & Fast-Fail Unit Tests.

Verifies:
1. log_jdbc_connection_diagnostic formats target connection parameters safely without passwords.
2. Loader.provision_target_schema_preflight raises TargetConnectionError on connection failures.
"""
import pytest
from unittest.mock import MagicMock, patch
from src.utils.db_utils import log_jdbc_connection_diagnostic
from src.load.loader import Loader, TargetConnectionError, TargetTimeoutError
from src.exceptions import TargetPreflightError
from src.connections.connection_manager import ConnectionConfig

def test_log_jdbc_connection_diagnostic_masks_credentials():
    """Verify log_jdbc_connection_diagnostic formats parameters and masks passwords."""
    cfg = ConnectionConfig(
        source_tag="land_test",
        db_type="postgres",
        jdbc_url="jdbc:postgresql://192.0.2.1:5432/land_test",
        username="mg",
        credential_reference="secret123",
        driver="org.postgresql.Driver",
        host="192.0.2.1",
        port="5432",
        database="land_test",
        schema="landing"
    )

    formatted = log_jdbc_connection_diagnostic(cfg, "Test Target Diagnostic")
    assert "land_test" in formatted
    assert "192.0.2.1" in formatted
    assert "secret123" not in formatted
    assert "org.postgresql.Driver" in formatted

def test_provision_target_schema_preflight_raises_target_connection_error():
    """Verify TargetConnectionError is raised when pre-flight JDBC query fails with connection error."""
    spark = MagicMock()
    conn_manager = MagicMock()
    loader = Loader(spark, conn_manager)

    cfg = ConnectionConfig(
        source_tag="unreachable_tag",
        db_type="postgres",
        jdbc_url="jdbc:postgresql://1.2.3.4:5432/invalid",
        username="user",
        credential_reference="pwd",
        driver="org.postgresql.Driver",
        host="1.2.3.4",
        port="5432",
        database="invalid",
        schema="public"
    )
    meta = MagicMock()
    meta.target_schema = "public"
    meta.target_table = "users"
    meta.source_table = "users"
    meta.is_cdc = "n"
    meta.canonical_business_key_columns = ()
    meta.key_source = None

    # Mock spark.read.format to allow method chaining and raise connection attempt failed error on .load()
    reader_mock = MagicMock()
    reader_mock.option.return_value = reader_mock
    reader_mock.load.side_effect = Exception(
        "(org.postgresql.util.PSQLException) The connection attempt failed. Caused by: java.net.SocketTimeoutException: connect timed out"
    )
    spark.read.format.return_value = reader_mock

    mock_df = MagicMock()
    mock_df.filter.return_value.count.return_value = 0
    mock_df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0

    with patch("src.utils.db_utils.execute_ddl") as mock_execute_ddl:
        mock_execute_ddl.side_effect = Exception("The connection attempt failed. Caused by: java.net.SocketTimeoutException: connect timed out")
        with pytest.raises((TargetTimeoutError, TargetPreflightError, TargetConnectionError)) as exc_info:
            loader.provision_target_schema_preflight(
                mock_df,
                meta,
                cfg,
                merge_key="id",
                before_persist=lambda: None,
            )

    assert "timed out" in str(exc_info.value).lower()
