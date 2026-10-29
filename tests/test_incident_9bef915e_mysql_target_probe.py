# tests/test_incident_9bef915e_mysql_target_probe.py
"""
Regression test suite for Incident INC-20261026-9bef915e:
1. Target probe error classification for MySQL 'Table doesn't exist' and apostrophe variants.
2. MySQLSchemaInspector unique key and primary key verification against information_schema.STATISTICS.
3. Target schema inspector registration for MySQL.
4. _parse_mysql_connection hostname, port, database resolution.
5. execute_ddl native pymysql execution path avoiding Py4J SparkContext dependency under Spark Connect.
"""
import pytest
from unittest.mock import MagicMock, patch

from src.load.loader import (
    Loader,
    TargetPreflightError,
    TargetTimeoutError,
    TargetRefusedError,
    TargetAuthenticationError,
    TargetUnreachableError,
)
from src.load.target_schema_inspector import (
    get_schema_inspector,
    MySQLSchemaInspector,
    PostgreSQLSchemaInspector,
    GenericSchemaInspector,
)
from src.utils.db_utils import _parse_mysql_connection, execute_ddl


def test_classify_target_probe_failure_mysql_absence():
    """Verify MySQL table absence markers return cleanly without raising."""
    # 1. Exact error reported in Incident INC-20261026-9bef915e
    incident_err = Exception(
        "(java.sql.SQLSyntaxErrorException) Table 'land_housing_prod.zonev' doesn't exist"
    )
    Loader._classify_target_probe_failure(incident_err)  # must not raise

    # 2. MySQL error with curly quotation mark (U+2019)
    curly_err = Exception(
        "(java.sql.SQLSyntaxErrorException) Table 'land_housing_prod.tblapplicant' doesn’t exist"
    )
    Loader._classify_target_probe_failure(curly_err)  # must not raise

    # 3. MySQL alternate 'unknown table' marker
    unknown_tbl_err = Exception(
        "(java.sql.SQLException) Unknown table 'land_housing_prod.plots'"
    )
    Loader._classify_target_probe_failure(unknown_tbl_err)  # must not raise

    # 4. PostgreSQL relation does not exist
    pg_err = Exception(
        'org.postgresql.util.PSQLException: ERROR: relation "land_housing.applicant" does not exist'
    )
    Loader._classify_target_probe_failure(pg_err)  # must not raise

    # 5. Oracle ORA-00942
    oracle_err = Exception("java.sql.SQLSyntaxErrorException: ORA-00942: table or view does not exist")
    Loader._classify_target_probe_failure(oracle_err)  # must not raise

    # 6. MSSQL invalid object name
    mssql_err = Exception(
        "com.microsoft.sqlserver.jdbc.SQLServerException: Invalid object name 'dbo.applicant'."
    )
    Loader._classify_target_probe_failure(mssql_err)  # must not raise

    # 7. SQLite no such table
    sqlite_err = Exception("java.sql.SQLException: no such table: main.applicant")
    Loader._classify_target_probe_failure(sqlite_err)  # must not raise


def test_classify_target_probe_failure_real_errors_raise():
    """Verify genuine infrastructure and security failures continue to raise classified errors."""
    # Timeout
    with pytest.raises(TargetTimeoutError, match="Target connection timed out"):
        Loader._classify_target_probe_failure(Exception("SocketTimeoutException: connect timed out"))

    # Connection refused
    with pytest.raises(TargetRefusedError, match="Target connection was refused"):
        Loader._classify_target_probe_failure(Exception("Connection refused (Connection refused)"))

    # Authentication failure
    with pytest.raises(TargetAuthenticationError, match="Target authentication failed"):
        Loader._classify_target_probe_failure(Exception("Access denied for user 'hiveworx'@'192.0.2.1' (using password: YES)"))

    # Unreachable
    with pytest.raises(TargetUnreachableError, match="Target is unreachable"):
        Loader._classify_target_probe_failure(Exception("Host is unreachable: connection attempt failed"))

    # Generic unexpected error
    with pytest.raises(TargetPreflightError, match="Target table probe failed"):
        Loader._classify_target_probe_failure(Exception("Fatal hardware I/O error"))


def test_get_schema_inspector_registration():
    """Verify get_schema_inspector registers PostgreSQL, MySQL, and Generic inspectors."""
    pg_cfg = MagicMock(driver="org.postgresql.Driver", db_type="postgres")
    assert isinstance(get_schema_inspector(pg_cfg), PostgreSQLSchemaInspector)

    mysql_cfg = MagicMock(driver="com.mysql.cj.jdbc.Driver", db_type="mysql")
    assert isinstance(get_schema_inspector(mysql_cfg), MySQLSchemaInspector)

    mysql_dict = {"driver": "com.mysql.cj.jdbc.Driver", "db_type": "mysql"}
    assert isinstance(get_schema_inspector(mysql_dict), MySQLSchemaInspector)

    unknown_cfg = MagicMock(driver="com.ibm.db2.jcc.DB2Driver", db_type="db2")
    assert isinstance(get_schema_inspector(unknown_cfg), GenericSchemaInspector)


@patch("pymysql.connect")
def test_mysql_schema_inspector_supports_unique_key(mock_connect):
    """Verify MySQLSchemaInspector queries information_schema.STATISTICS for unique constraints."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock statistics query output:
    # PRIMARY KEY: (id)
    # UNIQUE KEY uq_composite: (schno, plotno)
    mock_cur.fetchall.return_value = [
        ("PRIMARY", "id"),
        ("uq_composite", "schno"),
        ("uq_composite", "plotno"),
    ]

    inspector = MySQLSchemaInspector()
    cfg = {
        "host": "192.0.2.1",
        "port": 3306,
        "database": "land_housing_prod",
        "username": "hiveworx",
        "password": "secret_password",
    }

    # 1. Exact match on single primary key
    assert inspector.supports_unique_key("land_housing_prod", "zonev", ("id",), cfg) is True

    # 2. Case-insensitive composite key match
    assert inspector.supports_unique_key(
        "land_housing_prod", "zonev", ("SCHNO", "PLOTNO"), cfg
    ) is True

    # 3. Missing column
    assert inspector.supports_unique_key(
        "land_housing_prod", "zonev", ("schno", "plotno", "phase"), cfg
    ) is False

    # 4. Empty key
    assert inspector.supports_unique_key("land_housing_prod", "zonev", tuple(), cfg) is False

    # Verify query and parameters
    mock_cur.execute.assert_called_with(
        """
        SELECT INDEX_NAME, COLUMN_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND NON_UNIQUE = 0
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """,
        ("land_housing_prod", "zonev"),
    )


def test_parse_mysql_connection():
    """Verify _parse_mysql_connection extracts host, port, database from configs and URLs."""
    # From dictionary
    dict_cfg = {
        "host": "192.0.2.1",
        "port": 3306,
        "database": "land_housing_prod",
    }
    host, port, db = _parse_mysql_connection(dict_cfg)
    assert host == "192.0.2.1"
    assert port == 3306
    assert db == "land_housing_prod"

    # From JDBC URL
    url_cfg = {
        "url": "jdbc:mysql://192.0.2.1:3306/land_housing_prod?yearIsDateType=false&zeroDateTimeBehavior=convertToNull",
    }
    host, port, db = _parse_mysql_connection(url_cfg)
    assert host == "192.0.2.1"
    assert port == 3306
    assert db == "land_housing_prod"


@patch("pymysql.connect")
def test_execute_ddl_mysql_pymysql_path(mock_connect):
    """Verify execute_ddl executes MySQL DDL directly via pymysql without SparkContext."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    spark = MagicMock()
    # Spark Connect has no sparkContext attribute; accessing it raises AttributeError
    del spark.sparkContext

    config = {
        "driver": "com.mysql.cj.jdbc.Driver",
        "db_type": "mysql",
        "host": "192.0.2.1",
        "port": 3306,
        "database": "land_housing_prod",
        "username": "hiveworx",
        "password": "secret_password",
    }

    execute_ddl(spark, "DROP TABLE IF EXISTS `land_housing_prod`.`zonev_stg`", config)

    mock_connect.assert_called_once_with(
        host="192.0.2.1",
        port=3306,
        database="land_housing_prod",
        user="hiveworx",
        password="secret_password",
        autocommit=True,
        connect_timeout=15,
    )
    mock_cur.execute.assert_called_once_with(
        "DROP TABLE IF EXISTS `land_housing_prod`.`zonev_stg`"
    )


def test_ensure_target_key_constraint_modifies_mysql_blob_text_columns():
    """Verify MySQL BLOB/LONGTEXT business key columns are modified to VARCHAR(255) to prevent error 1170."""
    loader = Loader.__new__(Loader)
    loader.conn_manager = MagicMock()

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    loader.conn_manager.get_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock information_schema returning plotid as longtext
    mock_cur.fetchall.return_value = [
        ("plotid", "longtext"),
        ("schemeid", "varchar"),
    ]

    target_cfg = MagicMock()
    target_cfg.db_type = "mysql"
    target_cfg.schema = "land_housing_prod"

    meta = MagicMock()
    meta.target_tag = "land_housing_prod"
    meta.target_schema = "land_housing_prod"

    before_persist = MagicMock()

    loader._create_target_unique_constraint(
        target_fqn="land_housing_prod.gujrat_data",
        key_tuple=("plotid",),
        target_cfg=target_cfg,
        meta=meta,
        before_persist=before_persist,
    )

    # Verify executed SQL statements
    executed_statements = [call.args[0] for call in mock_cur.execute.call_args_list]

    # 1. Must query information_schema columns and data_type
    assert any("SELECT column_name, data_type FROM information_schema.columns" in s for s in executed_statements)

    # 2. Must ALTER TABLE to MODIFY plotid to VARCHAR(255)
    assert any("MODIFY `plotid` VARCHAR(255)" in s for s in executed_statements)

    # 3. Must ADD CONSTRAINT UNIQUE (`plotid`)
    assert any("ADD CONSTRAINT" in s and "UNIQUE (`plotid`)" in s for s in executed_statements)

