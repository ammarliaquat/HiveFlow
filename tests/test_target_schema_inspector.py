from types import MappingProxyType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.connections.connection_manager import ConnectionConfig, ExecutionContext, ResolvedJdbcConfig
from src.exceptions import CDCPolicyNotMigratedError, TargetPreflightError
from src.load.loader import Loader
from src.load.target_schema_inspector import PostgreSQLSchemaInspector
from src.utils.db_utils import execute_ddl


@patch("psycopg2.connect")
@patch("src.utils.db_utils._parse_postgres_connection", return_value=("db.internal", 5432, "warehouse"))
def test_postgres_schema_inspector_uses_connection_config_username(
    mock_parse_connection, mock_connect
):
    """Schema inspection uses the canonical ConnectionConfig credential name."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    mock_connect.return_value = connection
    config = ConnectionConfig(
        source_tag="target",
        db_type="postgres",
        jdbc_url="jdbc:postgresql://db.internal:5432/warehouse",
        username="pipeline_user",
        credential_reference="pipeline_password",
        driver="org.postgresql.Driver",
    )

    assert not PostgreSQLSchemaInspector().supports_unique_key(
        "landing", "usermodules", ("id",), config
    )

    assert mock_connect.call_args.kwargs["user"] == "pipeline_user"
    assert mock_connect.call_args.kwargs["password"] == config.password
    connection.close.assert_called_once()


@patch("psycopg2.connect")
@patch("src.utils.db_utils._parse_postgres_connection", return_value=("db.internal", 5432, "warehouse"))
def test_postgres_schema_inspector_accepts_resolved_jdbc_credentials(
    mock_parse_connection, mock_connect
):
    """Snapshot Spark routing may supply a ResolvedJdbcConfig with ``user``."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    mock_connect.return_value = connection
    config = ResolvedJdbcConfig(
        connection_tag="target",
        db_type="postgres",
        role="target",
        context=ExecutionContext.SPARK,
        url="jdbc:postgresql://db.internal:5432/warehouse",
        user="pipeline_user",
        password="pipeline_password",
        driver="org.postgresql.Driver",
    )

    assert not PostgreSQLSchemaInspector().supports_unique_key(
        "landing", "usermodules", ("id",), config
    )

    assert mock_connect.call_args.kwargs["user"] == config.user


@patch("src.utils.db_utils.execute_ddl")
@patch("src.load.target_schema_inspector.get_schema_inspector")
@patch("src.utils.db_utils.apply_jdbc_options")
def test_stage0_blocks_cdc_before_constraint_inspection(
    mock_apply_jdbc_options, mock_inspector_factory, mock_execute_ddl
):
    """Stage 0 must reject CDC before any target constraint inspection."""
    loader = Loader.__new__(Loader)
    loader.spark = MagicMock()
    mock_apply_jdbc_options.return_value.option.return_value.load.return_value = MagicMock()
    mock_inspector_factory.return_value.supports_unique_key.side_effect = AttributeError(
        "ConnectionConfig has no attribute user"
    )
    meta = SimpleNamespace(
        canonical_business_key_columns=("id",),
        source_table="usermodules",
        target_table="usermodules",
        target_schema="landing",
        is_cdc="y",
        key_source="VERIFIED_PRIMARY_KEY",
        canonical_source_business_key=("id",),
    )
    target_cfg = SimpleNamespace(schema="landing", db_type="postgres")

    with pytest.raises(CDCPolicyNotMigratedError, match="CDC_POLICY_NOT_MIGRATED"):
        loader.provision_target_schema_preflight(
            None, meta, target_cfg, "id", before_persist=lambda: None
        )

    mock_inspector_factory.assert_not_called()
    mock_execute_ddl.assert_not_called()


@patch("psycopg2.connect")
@patch("src.utils.db_utils._parse_postgres_connection", return_value=("db.internal", 5432, "warehouse"))
def test_execute_ddl_accepts_connection_config_credentials(
    mock_parse_connection, mock_connect
):
    """PostgreSQL DDL must accept ConnectionConfig without dict-style access."""
    connection = MagicMock()
    mock_connect.return_value = connection
    config = ConnectionConfig(
        source_tag="target",
        db_type="postgres",
        jdbc_url="jdbc:postgresql://db.internal:5432/warehouse",
        username="pipeline_user",
        credential_reference="pipeline_password",
        driver="org.postgresql.Driver",
    )

    execute_ddl(MagicMock(), "SELECT 1", config)

    assert mock_connect.call_args.kwargs["user"] == config.username
    assert mock_connect.call_args.kwargs["password"] == config.password
    connection.cursor.return_value.__enter__.return_value.execute.assert_called_once_with("SELECT 1")


@patch("psycopg2.connect")
@patch("src.utils.db_utils._parse_postgres_connection", return_value=("db.internal", 5432, "warehouse"))
def test_execute_ddl_accepts_mapping_credentials(mock_parse_connection, mock_connect):
    """PostgreSQL DDL detects its dialect and credentials from a Mapping."""
    connection = MagicMock()
    mock_connect.return_value = connection
    config = MappingProxyType({
        "db_type": "postgres",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://db.internal:5432/warehouse",
        "user": "pipeline_user",
        "password": "pipeline_password",
    })

    execute_ddl(MagicMock(), "SELECT 1", config)

    assert mock_connect.call_args.kwargs["user"] == "pipeline_user"
    assert mock_connect.call_args.kwargs["password"] == "pipeline_password"


@patch("psycopg2.connect")
@patch("src.utils.db_utils._parse_postgres_connection", return_value=("db.internal", 5432, "warehouse"))
def test_execute_ddl_accepts_resolved_jdbc_credentials(mock_parse_connection, mock_connect):
    """PostgreSQL DDL accepts Spark/driver-resolved credentials."""
    connection = MagicMock()
    mock_connect.return_value = connection
    config = ResolvedJdbcConfig(
        connection_tag="target",
        db_type="postgres",
        role="target",
        context=ExecutionContext.DRIVER,
        url="jdbc:postgresql://db.internal:5432/warehouse",
        user="pipeline_user",
        password="pipeline_password",
        driver="org.postgresql.Driver",
    )

    execute_ddl(MagicMock(), "SELECT 1", config)

    assert mock_connect.call_args.kwargs["user"] == config.user
    assert mock_connect.call_args.kwargs["password"] == config.password


@patch("psycopg2.connect")
@patch("src.utils.db_utils._parse_postgres_connection", return_value=("db.internal", 5432, "warehouse"))
def test_postgres_schema_inspector_matches_double_quoted_constraint_def(
    mock_parse_connection, mock_connect
):
    """PostgreSQL constraint definitions containing double-quoted identifiers are parsed correctly."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("uq_etl_0fa980692272674a", "u", 'UNIQUE ("LOC_ID")')
    ]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    mock_connect.return_value = connection
    config = ConnectionConfig(
        source_tag="target",
        db_type="postgres",
        jdbc_url="jdbc:postgresql://db.internal:5432/warehouse",
        username="pipeline_user",
        credential_reference="pipeline_password",
        driver="org.postgresql.Driver",
    )

    assert PostgreSQLSchemaInspector().supports_unique_key(
        "land_n", "tbllocation", ("LOC_ID",), config
    )

