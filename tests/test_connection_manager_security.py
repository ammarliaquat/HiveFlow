# tests/test_connection_manager_security.py
import os
import pytest
from unittest.mock import MagicMock, patch
from src.connections.connection_manager import ConnectionManager, ConfigurationError, ExecutionContext


def test_target_spark_endpoint_uses_deployment_override_when_profile_has_no_override(monkeypatch):
    """A target uses the declared Spark-network deployment host when metadata has none."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = [
        (
            "land_test", "DATABASE", False, True, "DEV", "postgres",
            "192.0.2.1", 5432, "land_test", "landing", "etl",
            "${LAND_TEST_DB_PASSWORD}", "", {}, "Admin", "Data", "Target", 10,
        )
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cm = ConnectionManager.__new__(ConnectionManager)
    cm._raw_config = {"pipeline": {}}
    cm._configs = {}
    cm.get_metadata_db_connection = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    cm._resolve_env_vars = lambda value: "secret" if value == "${LAND_TEST_DB_PASSWORD}" else value
    monkeypatch.setenv("SPARK_CONTAINER_DB_HOST", "PGDBBI")

    cm._build_connections()

    cfg = cm.get("land_test", expected_role="target")
    assert cfg.spark_host == "PGDBBI"
    assert "PGDBBI" in cm.resolve_jdbc(cfg, context=ExecutionContext.SPARK).url


def test_explicit_connection_spark_host_wins_over_deployment_override(monkeypatch):
    """Per-profile routing remains the highest-precedence Spark endpoint authority."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = [
        (
            "profiled_target", "DATABASE", False, True, "DEV", "postgres",
            "192.0.2.1", 5432, "land_test", "landing", "etl",
            "${LAND_TEST_DB_PASSWORD}", "", {"spark_host": "${LAND_TEST_SPARK_HOST}"}, "Admin", "Data", "Target", 10,
        )
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cm = ConnectionManager.__new__(ConnectionManager)
    cm._raw_config = {"pipeline": {}}
    cm._configs = {}
    cm.get_metadata_db_connection = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    cm._resolve_env_vars = lambda value: {
        "${LAND_TEST_DB_PASSWORD}": "secret",
        "${LAND_TEST_SPARK_HOST}": "profile-host",
    }.get(value, value)
    monkeypatch.setenv("SPARK_CONTAINER_DB_HOST", "deployment-host")

    cm._build_connections()

    assert cm.get("profiled_target", expected_role="target").spark_host == "profile-host"


def test_source_endpoint_is_not_rewritten_by_target_deployment_override(monkeypatch):
    """The target-only deployment route must never leak into source extraction."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = [
        (
            "source_db", "DATABASE", True, False, "PROD", "postgres",
            "source.internal", 5432, "source", "public", "etl",
            "${SOURCE_DB_PASSWORD}", "", {}, "Admin", "Data", "Source", 10,
        )
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cm = ConnectionManager.__new__(ConnectionManager)
    cm._raw_config = {"pipeline": {}}
    cm._configs = {}
    cm.get_metadata_db_connection = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    cm._resolve_env_vars = lambda value: "secret" if value == "${SOURCE_DB_PASSWORD}" else value
    monkeypatch.setenv("SPARK_CONTAINER_DB_HOST", "target-container")

    cm._build_connections()

    cfg = cm.get("source_db", expected_role="source")
    assert cfg.spark_host == ""
    assert "source.internal" in cm.resolve_jdbc(cfg, ExecutionContext.SPARK).url


def test_empty_explicit_spark_host_fails_closed(monkeypatch):
    """A declared but unresolved route must not silently fall back to the driver host."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = [
        (
            "land_test", "DATABASE", False, True, "DEV", "postgres",
            "192.0.2.1", 5432, "land_test", "landing", "etl",
            "${LAND_TEST_DB_PASSWORD}", "", {"spark_host": "${MISSING_SPARK_HOST}"}, "Admin", "Data", "Target", 10,
        )
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cm = ConnectionManager.__new__(ConnectionManager)
    cm._raw_config = {"pipeline": {}}
    cm._configs = {}
    cm.get_metadata_db_connection = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    cm._resolve_env_vars = lambda value: {
        "${LAND_TEST_DB_PASSWORD}": "secret",
        "${MISSING_SPARK_HOST}": "",
    }.get(value, value)
    monkeypatch.setenv("SPARK_CONTAINER_DB_HOST", "deployment-host")

    with pytest.raises(RuntimeError, match="declares spark_host but it resolved to an empty value"):
        cm._build_connections()

def test_resolve_env_vars_password_present():
    """Verify env var resolution succeeds when password environment variable is present."""
    with patch.dict(os.environ, {"DB_TEST_SECRET_PASSWORD": "SecretPassword123!"}):
        conn_mgr = ConnectionManager.__new__(ConnectionManager)
        res = conn_mgr._resolve_env_vars("prefix_${DB_TEST_SECRET_PASSWORD}_suffix")
        assert res == "prefix_SecretPassword123!_suffix"

def test_resolve_env_vars_password_missing_raises_error():
    """Verify ConfigurationError is raised when password environment variable is missing (D-004)."""
    with patch.dict(os.environ, {}, clear=True):
        conn_mgr = ConnectionManager.__new__(ConnectionManager)
        with pytest.raises(ConfigurationError) as exc_info:
            conn_mgr._resolve_env_vars("${MISSING_DB_PASSWORD}")
        assert "Missing required credential environment variable" in str(exc_info.value)
        assert "MISSING_DB_PASSWORD" in str(exc_info.value)

def test_resolve_env_vars_password_empty_raises_error():
    """Verify ConfigurationError is raised when password environment variable is empty string (D-004)."""
    with patch.dict(os.environ, {"EMPTY_DB_PASSWORD": "   "}):
        conn_mgr = ConnectionManager.__new__(ConnectionManager)
        with pytest.raises(ConfigurationError) as exc_info:
            conn_mgr._resolve_env_vars("${EMPTY_DB_PASSWORD}")
        assert "Missing required credential environment variable" in str(exc_info.value)


def test_build_source_profile_preserves_env_references():
    """GUI onboarding should register metadata.connections without storing raw secrets."""
    from src.services.connection_service import ConnectionService

    profile = ConnectionService.build_source_profile(
        tag="New_Source",
        db_type="mysql",
        server="${NEW_SOURCE_HOST}",
        port=3306,
        database_name="source_db",
        schema_name="",
        username="${NEW_SOURCE_USER}",
        credential_reference="${NEW_SOURCE_PASSWORD}",
        environment="prod",
    )

    assert profile["tag"] == "new_source"
    assert profile["supports_source"] is True
    assert profile["supports_target"] is False
    assert profile["server"] == "${NEW_SOURCE_HOST}"
    assert profile["username"] == "${NEW_SOURCE_USER}"
    assert profile["credential_reference"] == "${NEW_SOURCE_PASSWORD}"
    assert profile["schema_name"] == "source_db"
    assert profile["environment"] == "PROD"


def test_build_source_profile_requires_secret_reference():
    from src.services.connection_service import ConnectionService

    with pytest.raises(ValueError, match="Credential reference is required"):
        ConnectionService.build_source_profile(
            tag="new_source",
            db_type="mysql",
            server="${NEW_SOURCE_HOST}",
            port=3306,
            database_name="source_db",
            schema_name="source_db",
            username="${NEW_SOURCE_USER}",
            credential_reference="",
        )


@patch("src.services.connection_service.pymysql.connect")
def test_connection_test_persists_resolved_server_and_status(mock_mysql_connect):
    """Diagnostics must update metadata.connections health fields and resolved non-secret server."""
    from src.services.connection_service import ConnectionService

    mock_cm = MagicMock()
    mock_cm._resolve_env_vars.side_effect = lambda value: {
        "${MYSQL_MEMBERSHIP_HOST:source-db-host}": "resolved-source-db-host",
        "${MSOPFORG_DB_PASSWORD}": "secret-password",
    }.get(value, value)
    mock_cm._normalize_db_type.side_effect = lambda value: "mysql" if "mysql" in value.lower() else value.lower()

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch.object(ConnectionService, "_get_cm", return_value=mock_cm), \
         patch.object(ConnectionService, "get_connection", return_value={"tag": "msopforg_opforg"}), \
         patch("src.services.notification_deriver.NotificationDeriver.on_connection_test_finished"):
        result = ConnectionService.test_connection({
            "tag": "msopforg_opforg",
            "db_type": "mysql",
            "server": "${MYSQL_MEMBERSHIP_HOST:source-db-host}",
            "port": 3306,
            "database_name": "source_membership_db",
            "username": "source_app_user",
            "credential_reference": "${MSOPFORG_DB_PASSWORD}",
        }, update_db=True)

    assert result["status"] == "SUCCESS"
    update_sql, update_params = mock_cur.execute.call_args.args
    assert "last_test_status" in update_sql
    assert "server = %s" in update_sql
    assert update_params[3] == "mysql"
    assert update_params[4] == "resolved-source-db-host"


def test_build_jdbc_url_static_mysql_preserves_server_timezone():
    """Verify MySQL JDBC URL explicitly enforces connectionTimeZone=SERVER and preserveInstants=false to avoid driver timezone shifts."""
    url = ConnectionManager.build_jdbc_url_static("mysql", "localhost", "3306", "test_db")
    assert "connectionTimeZone=SERVER" in url
    assert "preserveInstants=false" in url
    assert "yearIsDateType=false" in url
    assert "zeroDateTimeBehavior=convertToNull" in url

def test_build_jdbc_url_static_mysql_batch_rewriting():
    """MySQL JDBC URL must include rewriteBatchedStatements=true for Gold write throughput."""
    url = ConnectionManager.build_jdbc_url_static("mysql", "localhost", "3306", "test_db")
    assert "rewriteBatchedStatements=true" in url


def test_build_jdbc_url_static_postgres_clean_url():
    """PostgreSQL JDBC URL must not contain query parameters to prevent protocol buffer stalls."""
    url = ConnectionManager.build_jdbc_url_static("postgres", "dbhost", "5432", "mydb")
    assert url == "jdbc:postgresql://dbhost:5432/mydb"


def test_external_mysql_target_does_not_leak_postgres_spark_container_host(monkeypatch):
    """Context isolation (Rule 2): SPARK_CONTAINER_DB_HOST must not leak into external MySQL targets."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = [
        (
            "land_housing_prod", "DATABASE", False, True, "PROD", "mysql",
            "192.0.2.1", 3306, "land_housing_prod", "land_housing_prod", "etl",
            "${MYSQL_PASSWORD}", "", {}, "Admin", "Data", "Target", 10,
        )
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cm = ConnectionManager.__new__(ConnectionManager)
    cm._raw_config = {"pipeline": {}}
    cm._configs = {}
    cm.get_metadata_db_connection = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    cm._resolve_env_vars = lambda value: "secret" if value == "${MYSQL_PASSWORD}" else value
    monkeypatch.setenv("SPARK_CONTAINER_DB_HOST", "PGDBBI")

    cm._build_connections()

    cfg = cm.get("land_housing_prod", expected_role="target")
    assert cfg.spark_host == ""
    assert "192.0.2.1" in cm.resolve_jdbc(cfg, context=ExecutionContext.SPARK).url
    assert "PGDBBI" not in cm.resolve_jdbc(cfg, context=ExecutionContext.SPARK).url

