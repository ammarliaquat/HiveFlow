import time
import os
import logging
import re
from collections.abc import Mapping, Iterable
from typing import List, Optional, Union
from pyspark.sql import DataFrame

log = logging.getLogger("db_utils")
# Additional imports for retry handling
from src.constants import MetadataConstants, DBType
import psycopg2


def _coerce_to_value_list(values) -> list:
    """Return a plain list from any sequence-like or dict payload.

    Callers may pass the exception-key set as a tuple, a set, a list, a
    frozenset, or a legacy dict with a ``values`` key.  This helper
    normalises all forms so downstream code always receives a plain list.
    Strings are deliberately excluded (they are iterable but a single
    string is almost never a valid collection-of-values here).
    """
    if values is None:
        return []
    if isinstance(values, Mapping):
        # Legacy dict payload: {"values": [...], ...}
        return list(values.get("values", []))
    if isinstance(values, str):
        # A bare string is a single scalar value, not a collection.
        return [values]
    if isinstance(values, Iterable):
        return list(values)
    return []


# ── Retryable lock-error patterns (all supported DB dialects) ────────────────
RETRYABLE_LOCK_PATTERNS = [
    "ORA-00054",   # Oracle: resource busy / acquire with NOWAIT
    "55P03",       # PostgreSQL: lock_not_available
    "40P01",       # PostgreSQL: deadlock_detected
    "1222",        # SQL Server: lock request time out period exceeded
    "1205",        # MySQL: lock wait timeout exceeded
]


def _is_retryable_lock_error(msg: str) -> bool:
    """Check if an error message matches any known lock/deadlock pattern."""
    msg_upper = msg.upper()
    return any(p.upper() in msg_upper for p in RETRYABLE_LOCK_PATTERNS)


def _cfg_value(cfg, *names, default=""):
    """Read a config value from either a mapping or a config object."""
    for name in names:
        if isinstance(cfg, Mapping) and name in cfg:
            return cfg.get(name, default)
        if hasattr(cfg, name):
            return getattr(cfg, name)
    return default


# ══════════════════════════════════════════════════════════════════════════════
# Fetch failed tables for retry
# ══════════════════════════════════════════════════════════════════════════════

def fetch_failed_tables(conn_manager):
    """Retrieve tables from db_tables_etl where rerun_flag = 'y'."""
    with conn_manager.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT source_schema, source_table, source_tag FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE} WHERE LOWER(rerun_flag) = 'y'"
            )
            rows = cur.fetchall()
            return [{"schema": r[0], "table": r[1], "tag": r[2]} for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Pre-flight JDBC connection test & options helper
# ══════════════════════════════════════════════════════════════════════════════

def log_jdbc_connection_diagnostic(cfg, context_name: str = "JDBC Connection") -> str:
    """
    Log target connection resolution details safely without credential exposure.
    """
    if hasattr(cfg, "jdbc_url"):
        url = cfg.jdbc_url
        host = getattr(cfg, "host", "")
        port = getattr(cfg, "port", "")
        database = getattr(cfg, "database", "")
        schema = getattr(cfg, "schema", "")
        username = getattr(cfg, "username", "")
        driver = getattr(cfg, "driver", "")
        target_tag = getattr(cfg, "source_tag", "")
    else:
        url = cfg.get("url") or cfg.get("jdbc_url", "")
        host = cfg.get("host", "")
        port = cfg.get("port", "")
        database = cfg.get("database", "")
        schema = cfg.get("schema", "")
        username = cfg.get("username") or cfg.get("user", "")
        driver = cfg.get("driver", "")
        target_tag = cfg.get("target_tag") or cfg.get("source_tag", "")

    # Mask password from URL if present
    safe_url = url
    if "@" in url:
        parts = url.split("@")
        safe_url = f"jdbc:***@{parts[-1]}"

    diag_lines = [
        f"\n==================================================",
        f"  {context_name.upper()} RESOLUTION DIAGNOSTIC",
        f"==================================================",
        f"  target_tag  : {target_tag}",
        f"  host        : {host}",
        f"  port        : {port}",
        f"  database    : {database}",
        f"  schema      : {schema}",
        f"  username    : {username}",
        f"  jdbc_url    : {safe_url}",
        f"  driver      : {driver}",
        f"  spark_conn  : {os.environ.get('SPARK_CONNECT_URL', 'LOCAL')}",
        f"=================================================="
    ]
    formatted = "\n".join(diag_lines)
    log.info(formatted)
    return formatted


def apply_jdbc_options(builder, cfg):
    """Apply JDBC options onto a Spark DataFrame reader or writer.

    Accepts either a ConnectionConfig object, a ResolvedJdbcConfig object, or a config dict.
    Resolves context-appropriate JDBC URL.
    """
    if hasattr(cfg, "url"):
        url = cfg.url
        db_type = getattr(cfg, "db_type", "")
        driver = getattr(cfg, "driver", "")
    elif hasattr(cfg, "jdbc_url"):
        url = cfg.jdbc_url
        db_type = getattr(cfg, "db_type", "")
        driver = getattr(cfg, "driver", "")
    else:
        url = cfg.get("url", "")
        db_type = cfg.get("db_type", "")
        driver = cfg.get("driver", "")

    is_oracle = "oracle" in driver.lower() or "oracle" in url.lower() or db_type.lower() == "oracle"
    if is_oracle and url and "oracle.jdbc.timezoneAsRegion" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}oracle.jdbc.timezoneAsRegion=false"

    if hasattr(cfg, "connection_options") and isinstance(getattr(cfg, "connection_options", None), dict):
        for opt_k, opt_v in cfg.connection_options.items():
            if opt_k in ("connectTimeout", "socketTimeout", "loginTimeout"):
                builder = builder.option(opt_k, str(opt_v))

    if hasattr(cfg, "jdbc_properties"):
        for k, v in cfg.jdbc_properties.items():
            if k == "url":
                builder = builder.option("url", url)
            else:
                builder = builder.option(k, str(v))
        if url:
            builder = builder.option("url", url)
    else:
        if is_oracle:
            builder = builder.option("oracle.jdbc.timezoneAsRegion", "false")
            builder = builder.option("oracle.sessionTimeZone", "UTC")
            builder = builder.option("sessionTimeZone", "UTC")
        if url: builder = builder.option("url", url)
        user = _cfg_value(cfg, "user", "username")
        password = _cfg_value(cfg, "password", "password_resolved")
        if user: builder = builder.option("user", user)
        if password: builder = builder.option("password", password)
        if driver: builder = builder.option("driver", driver)
        options = _cfg_value(cfg, "options", "connection_options", default={}) or {}
        if isinstance(options, dict):
            for k, v in options.items():
                builder = builder.option(k, str(v))
    return builder


def test_jdbc_connection(cfg, spark=None, timeout_sec: int = 30) -> bool:
    """Perform a lightweight SELECT 1 on the configured DB.

    Accepts either a ConnectionConfig object (preferred) or a plain dict
    with keys: url/jdbc_url, user/username, password, driver, db_type.

    *timeout_sec* (default 30) caps how long we wait for the Spark Connect
    round-trip.  When the Spark Connect server is down, gRPC blocks until the
    OS TCP timeout (~10 min); without this cap the healthcheck would hang for
    hours.

    Returns True if the connection succeeds within the timeout, False otherwise.
    """
    driver = _cfg_value(cfg, "driver")
    is_oracle = "oracle" in driver.lower()
    ping_query = "SELECT 1 FROM DUAL" if is_oracle else "SELECT 1"

    url = _cfg_value(cfg, "url", "jdbc_url")

    try:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

        if not spark:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

        def _run_probe():
            reader = apply_jdbc_options(spark.read.format("jdbc"), cfg)
            reader.option("query", ping_query).load().limit(1).collect()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_run_probe)
            future.result(timeout=timeout_sec)   # raises TimeoutError if too slow

        return True

    except FuturesTimeoutError:
        url_display = url if 'url' in dir() else str(cfg)
        log.error(
            f"Pre‑flight JDBC connection timed out after {timeout_sec}s for {url_display}. "
            f"Spark Connect may be unreachable."
        )
        return False
    except Exception as exc:
        url_display = url if 'url' in dir() else str(cfg)
        log.error(f"Pre‑flight JDBC connection failed for {url_display}: {exc}")
        return False



# ══════════════════════════════════════════════════════════════════════════════
# Safe JDBC execution with multi-DB lock retry
# ══════════════════════════════════════════════════════════════════════════════

def safe_execute_jdbc(df: DataFrame, target_cfg: dict, operation: str = "write",
                      max_retries: int = 3, backoff: float = 2.0, spark = None) -> None:
    """Execute a JDBC operation with retry on lock/deadlock errors.

    Supports retryable errors from Oracle, PostgreSQL, SQL Server, and MySQL.

    Parameters:
        df          – Spark DataFrame to write (used for "write" and "upsert" operations).
        target_cfg  – Dictionary with keys: url, user, password, driver, schema, table.
        operation   – "truncate", "write", "execute_sql", or "drop".
        max_retries – Maximum retry attempts.
        backoff     – Base seconds for exponential back‑off.
        spark       – Optional active SparkSession.
    """
    schema = target_cfg.get('schema', '')
    table = target_cfg.get('table', '')
    table_fqn = f"{schema}.{table}" if schema else table

    driver = target_cfg.get("driver", "")
    db_type = "postgres"
    if "oracle" in driver.lower():
        db_type = "oracle"
    elif "sqlserver" in driver.lower():
        db_type = "mssql"
    elif "mysql" in driver.lower():
        db_type = "mysql"
    quoted_table_fqn = quote_table_name(table_fqn, db_type)

    attempt = 0
    while True:
        try:
            if not spark:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                if not spark:
                    try:
                        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
                        spark = ConnectSparkSession.builder.getOrCreate()
                    except Exception:
                        pass
                if not spark:
                    spark = SparkSession.builder.getOrCreate()

            if operation == "truncate":
                sql = f"TRUNCATE TABLE {quoted_table_fqn}"
                # Execute via JDBC connection (Spark SQL cannot talk to external DBs)
                execute_ddl(spark, sql, target_cfg)

            elif operation == "drop":
                sql = target_cfg.get("sql", f"DROP TABLE IF EXISTS {quoted_table_fqn}")
                execute_ddl(spark, sql, target_cfg)

            elif operation == "execute_sql":
                sql = target_cfg["sql"]
                execute_ddl(spark, sql, target_cfg)

            else:  # write operation
                writer = apply_jdbc_options(df.write.format("jdbc"), target_cfg)
                writer.option("dbtable", quoted_table_fqn) \
                    .mode("append") \
                    .save()
            break  # success
        except Exception as exc:
            msg = str(exc)
            if _is_retryable_lock_error(msg) and attempt < max_retries:
                attempt += 1
                wait = backoff ** attempt
                log.warning(
                    f"[Retry {attempt}/{max_retries}] Lock/deadlock on {table_fqn}; "
                    f"sleeping {wait:.1f}s before retry."
                )
                time.sleep(wait)
                continue
            else:
                raise


def _parse_postgres_connection(target_cfg):
    """Parse a PostgreSQL JDBC config dict and resolve hostname, port, and database name."""
    if hasattr(target_cfg, "get_jdbc_url"):
        jdbc_url = target_cfg.get_jdbc_url("driver")
    elif isinstance(target_cfg, Mapping):
        jdbc_url = target_cfg.get("url") or target_cfg.get("jdbc_url")
    else:
        jdbc_url = getattr(target_cfg, 'url', getattr(target_cfg, 'jdbc_url', None))
    url_part = jdbc_url.replace("jdbc:postgresql://", "")
    host_port, database = url_part.rsplit("/", 1)
    if "?" in database:
        database = database.split("?")[0]
    if ":" in host_port:
        host, port_str = host_port.split(":", 1)
        port = int(port_str)
    else:
        host, port = host_port, 5432

    resolved_host = host
    if host and not host.replace('.', '').isdigit():
        import socket
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            from src.exceptions import ConnectionResolutionError
            raise ConnectionResolutionError(
                message=f"TARGET_RESOLUTION_FAILED: Could not resolve target host {host}",
                connection_tag=getattr(target_cfg, 'connection_tag', 'unknown'),
                role=getattr(target_cfg, 'role', 'target'),
                context=getattr(target_cfg, 'context', 'unknown'),
                host=host
            )
    return resolved_host, port, database


def _parse_mysql_connection(target_cfg):
    """Parse a MySQL JDBC config dict or DatabaseConfig and resolve hostname, port, and database name."""
    host = None
    port = None
    database = None
    jdbc_url = None
    if isinstance(target_cfg, Mapping):
        host = target_cfg.get("host")
        port = target_cfg.get("port")
        database = target_cfg.get("database") or target_cfg.get("schema")
        jdbc_url = target_cfg.get("url") or target_cfg.get("jdbc_url")
    else:
        host = getattr(target_cfg, "host", None)
        port = getattr(target_cfg, "port", None)
        database = getattr(target_cfg, "database", None) or getattr(target_cfg, "schema", None)
        jdbc_url = getattr(target_cfg, "url", getattr(target_cfg, "jdbc_url", None))

    if not host or not database:
        if jdbc_url:
            clean_url = str(jdbc_url).replace("jdbc:mysql://", "")
            if "/" in clean_url:
                host_port, db_query = clean_url.split("/", 1)
                db_name = db_query.split("?")[0]
                if not database:
                    database = db_name
                if not host:
                    if ":" in host_port:
                        h, p = host_port.split(":", 1)
                        host = h
                        port = int(p)
                    else:
                        host = host_port
                        port = 3306

    host = host or "localhost"
    port = int(port or 3306)
    return host, port, database



def _verify_staging_target_convergence(
    *,
    spark,
    target_cfg,
    target_table: str,
    stage_table: str,
    merge_key: str,
    db_type: str,
    expected_rows: int,
) -> dict:
    """Observe whether every staged business key exists in the target.

    The query stays inside the target database.  It returns aggregate evidence
    only, so reconciliation never transports a potentially large key set back
    to the driver.
    """
    key_columns = tuple(
        column.strip() for column in str(merge_key or "").split(",") if column.strip()
    )
    if not key_columns:
        raise ValueError("Target reconciliation requires at least one merge key.")

    quoted_stage = quote_table_name(stage_table, db_type)
    quoted_target = quote_table_name(target_table, db_type)
    join_predicate = " AND ".join(
        f"s.{quote_column_name(column, db_type)} = t.{quote_column_name(column, db_type)}"
        for column in key_columns
    )
    null_probe = f"t.{quote_column_name(key_columns[0], db_type)}"
    evidence_sql = f"""
        SELECT
            COUNT(*) AS staged_rows,
            COUNT({null_probe}) AS matched_rows,
            SUM(CASE WHEN {null_probe} IS NULL THEN 1 ELSE 0 END) AS missing_rows
        FROM {quoted_stage} s
        LEFT JOIN {quoted_target} t ON {join_predicate}
    """
    reader = apply_jdbc_options(spark.read.format("jdbc"), target_cfg)
    result = reader.option("dbtable", f"({evidence_sql}) reconciliation_evidence").load().first()

    def _value(name: str, default: int = 0) -> int:
        if result is None:
            return default
        if isinstance(result, Mapping):
            value = result.get(name, default)
        else:
            value = result[name] if name in result else default
        return int(value or 0)

    staged_rows = _value("staged_rows")
    return {
        "proof_mode": "STAGING_TARGET_LEFT_JOIN",
        "staging_table": stage_table,
        "staged_rows": staged_rows,
        "matched_rows": _value("matched_rows"),
        "missing_rows": _value("missing_rows"),
        "expected_rows": int(expected_rows),
    }


def execute_ddl(spark, sql: str, config):
    """
    True dialect-aware DDL execution abstraction.
    Never swallows exceptions. A successful DDL returns normally.
    """
    if isinstance(config, Mapping):
        driver = str(config.get("driver", "")).lower()
        db_type = str(config.get("db_type", "")).lower()
    else:
        driver = str(getattr(config, "driver", "")).lower()
        db_type = str(getattr(config, "db_type", "")).lower()
    
    if "postgresql" in driver or db_type == "postgres":
        import psycopg2 as pg2
        resolved_host, port, database = _parse_postgres_connection(config)
        
        if isinstance(config, Mapping):
            username = config.get("username") or config.get("user")
            password = config.get("password_resolved") or config.get("password")
        else:
            username = getattr(config, "username", None) or getattr(config, "user", None)
            password = getattr(config, "password_resolved", None) or getattr(config, "password", None)
        if not username or not password:
            raise ValueError("Target connection configuration is missing database credentials")
        conn = pg2.connect(
            host=resolved_host, port=port, dbname=database,
            user=username, password=password
        )
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql)
            return
        finally:
            conn.close()

    if "mysql" in driver or db_type == "mysql":
        import pymysql
        resolved_host, port, database = _parse_mysql_connection(config)
        if isinstance(config, Mapping):
            username = config.get("username") or config.get("user")
            password = config.get("password_resolved") or config.get("password")
        else:
            username = getattr(config, "username", None) or getattr(config, "user", None)
            password = getattr(config, "password_resolved", None) or getattr(config, "password", None)
        if not username or not password:
            raise ValueError("Target connection configuration is missing database credentials")
        conn = pymysql.connect(
            host=resolved_host,
            port=port,
            database=database,
            user=username,
            password=password,
            autocommit=True,
            connect_timeout=15,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            return
        finally:
            conn.close()

    # Generic JDBC via Py4J for other dialects
    sc = spark.sparkContext
    gateway = sc._gateway
    from py4j.java_gateway import java_import
    java_import(gateway.jvm, "java.sql.DriverManager")
    
    if isinstance(config, Mapping):
        username = config.get("username") or config.get("user")
        password = config.get("password_resolved") or config.get("password")
        jdbc_url = config.get("url") or config.get("jdbc_url")
    else:
        username = getattr(config, "username", None) or getattr(config, "user", None)
        password = getattr(config, "password_resolved", None) or getattr(config, "password", None)
        jdbc_url = getattr(config, "url", None) or getattr(config, "jdbc_url", None)
    if not username or not password or not jdbc_url:
        raise ValueError("Target connection configuration is incomplete for DDL execution")

    props = gateway.jvm.java.util.Properties()
    props.put("user", username)
    props.put("password", password)
    if hasattr(config, 'properties'):
        for k, v in config.properties.items():
            props.put(k, str(v))
            
    conn = gateway.jvm.DriverManager.getConnection(jdbc_url, props)
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# Dynamic UPSERT SQL generation (all 4 DB types)
# ══════════════════════════════════════════════════════════════════════════════

def _validate_identifier(identifier: str) -> str:
    """Reject metadata identifiers that cannot be safely quoted as SQL names."""
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_$#]*", str(identifier or "")):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return str(identifier)


def quote_column_name(column_name: str, db_type: str) -> str:
    """Quote column identifier for the target database dialect."""
    if not column_name:
        return column_name
    column_name = _validate_identifier(column_name)
    db_type_lower = str(db_type).lower() if db_type else ""
    if db_type_lower in (DBType.MYSQL, "mysql"):
        return f"`{column_name}`"
    elif db_type_lower in (DBType.ORACLE, "oracle", "ora"):
        return column_name
    else:
        return f'"{column_name}"'


def get_table_alias_keyword(db_type: str) -> str:
    """Return SQL table alias keyword (' AS ' or ' ') based on database dialect."""
    db_type_lower = str(db_type).lower() if db_type else ""
    if db_type_lower in (DBType.ORACLE, "oracle", "ora"):
        return " "
    return " AS "


def quote_table_name(table_fqn: str, db_type: str) -> str:
    """Quote table identifier (schema and table name) for the target database dialect."""
    if not table_fqn:
        return table_fqn
    parts = table_fqn.split(".")
    parts = [_validate_identifier(part) for part in parts]
    db_type_lower = str(db_type).lower() if db_type else ""
    if db_type_lower in (DBType.MYSQL, "mysql"):
        return ".".join(f"`{p}`" for p in parts)
    elif db_type_lower in (DBType.ORACLE, "oracle", "ora"):
        # In Oracle, double quoting forces case-sensitive matching against dictionary. Standard objects are uppercase.
        return ".".join(parts)
    else:
        return ".".join(f'"{p}"' for p in parts)


def normalize_and_validate_merge_keys(
    merge_key: Union[str, List[str]],
    columns: Optional[List[str]] = None,
) -> List[str]:
    """
    Normalize and strictly validate single or composite merge keys.

    Validation Rules:
      - Empty string / list / None -> ValueError("Merge key cannot be empty")
      - Element containing empty parts (e.g., "a,,b", ",a", "a,") -> ValueError("Invalid composite merge key: contains empty column element")
      - Duplicate column elements (e.g., "a,a") -> ValueError("Invalid composite merge key: contains duplicate column elements")
      - Unknown column elements not present in `columns` (if columns is provided) -> ValueError

    Returns:
      List[str]: Canonical list of stripped column names.
    """
    if merge_key is None:
        raise ValueError("Merge key cannot be empty")

    if isinstance(merge_key, list):
        if not merge_key:
            raise ValueError("Merge key cannot be empty")
        raw_parts = [str(k) for k in merge_key]
    elif isinstance(merge_key, str):
        if not merge_key.strip():
            raise ValueError("Merge key cannot be empty")
        raw_parts = merge_key.split(",")
    else:
        raise ValueError(f"Unsupported merge_key type: {type(merge_key)}")

    merge_keys = [p.strip() for p in raw_parts]

    if any(not k for k in merge_keys):
        raise ValueError("Invalid composite merge key: contains empty column element")

    if len(merge_keys) != len(set(merge_keys)):
        raise ValueError("Invalid composite merge key: contains duplicate column elements")

    if columns:
        col_map_lower = {str(c).lower(): str(c) for c in columns}
        canonical_keys = []
        for k in merge_keys:
            k_lower = k.lower()
            if k_lower not in col_map_lower:
                raise ValueError(f"Merge key column '{k}' not found in target columns: {columns}")
            canonical_keys.append(col_map_lower[k_lower])
        return canonical_keys

    return merge_keys


def _build_postgres_upsert(
    target_table: str, stage_table: str, merge_keys: List[str], columns: List[str]
) -> str:
    db_type = DBType.POSTGRES
    q_target = quote_table_name(target_table, db_type)
    q_stage = quote_table_name(stage_table, db_type)
    all_cols = ", ".join(quote_column_name(c, db_type) for c in columns)
    col_case_map = {c.lower(): c for c in columns}
    resolved_merge_keys = [col_case_map.get(k.lower(), k) for k in merge_keys]
    merge_key_lower_set = {k.lower() for k in merge_keys}
    update_cols = [c for c in columns if c.lower() not in merge_key_lower_set]
    conflict_target = ", ".join(quote_column_name(k, db_type) for k in resolved_merge_keys)

    if update_cols:
        set_clause = ", ".join(
            f"{quote_column_name(c, db_type)} = EXCLUDED.{quote_column_name(c, db_type)}"
            for c in update_cols
        )
        # Performance & Idempotency Optimization:
        # Avoid rewriting millions of unchanged rows by adding a change-detection WHERE clause.
        # Run-ephemeral audit timestamps (dml_timestamp, landing_ingestion_timestamp, pipeline_run_id)
        # must NOT trigger row updates if business data and lineage are identical.
        ephemeral_run_audit_cols = {
            "dml_timestamp",
            "landing_ingestion_timestamp",
            "pipeline_run_id",
        }
        compare_cols = [c for c in update_cols if c.lower() not in ephemeral_run_audit_cols]
        if compare_cols:
            target_tuple = ", ".join(f"t.{quote_column_name(c, db_type)}" for c in compare_cols)
            excluded_tuple = ", ".join(f"EXCLUDED.{quote_column_name(c, db_type)}" for c in compare_cols)
            where_clause = f" WHERE ({target_tuple}) IS DISTINCT FROM ({excluded_tuple})"
        else:
            where_clause = ""
        return (
            f"INSERT INTO {q_target} AS t ({all_cols}) "
            f"SELECT {all_cols} FROM {q_stage} "
            f"ON CONFLICT ({conflict_target}) DO UPDATE SET {set_clause}{where_clause}"
        )
    else:
        return (
            f"INSERT INTO {q_target} ({all_cols}) "
            f"SELECT {all_cols} FROM {q_stage} "
            f"ON CONFLICT ({conflict_target}) DO NOTHING"
        )


def _build_oracle_upsert(
    target_table: str, stage_table: str, merge_keys: List[str], columns: List[str]
) -> str:
    db_type = DBType.ORACLE
    q_target = quote_table_name(target_table, db_type)
    q_stage = quote_table_name(stage_table, db_type)
    on_clause = " AND ".join(
        f"t.{quote_column_name(k, db_type)} = s.{quote_column_name(k, db_type)}"
        for k in merge_keys
    )
    insert_cols = ", ".join(quote_column_name(c, db_type) for c in columns)
    insert_vals = ", ".join(f"s.{quote_column_name(c, db_type)}" for c in columns)
    update_cols = [c for c in columns if c not in merge_keys]

    if update_cols:
        update_set = ", ".join(
            f"t.{quote_column_name(c, db_type)} = s.{quote_column_name(c, db_type)}"
            for c in update_cols
        )
        return (
            f"MERGE INTO {q_target} t "
            f"USING {q_stage} s ON ({on_clause}) "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
    else:
        return (
            f"MERGE INTO {q_target} t "
            f"USING {q_stage} s ON ({on_clause}) "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )


def _build_mssql_upsert(
    target_table: str, stage_table: str, merge_keys: List[str], columns: List[str]
) -> str:
    db_type = DBType.MSSQL
    q_target = quote_table_name(target_table, db_type)
    q_stage = quote_table_name(stage_table, db_type)
    on_clause = " AND ".join(
        f"t.{quote_column_name(k, db_type)} = s.{quote_column_name(k, db_type)}"
        for k in merge_keys
    )
    insert_cols = ", ".join(quote_column_name(c, db_type) for c in columns)
    insert_vals = ", ".join(f"s.{quote_column_name(c, db_type)}" for c in columns)
    update_cols = [c for c in columns if c not in merge_keys]

    if update_cols:
        update_set = ", ".join(
            f"t.{quote_column_name(c, db_type)} = s.{quote_column_name(c, db_type)}"
            for c in update_cols
        )
        return (
            f"MERGE {q_target} AS t "
            f"USING {q_stage} AS s ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )
    else:
        return (
            f"MERGE {q_target} AS t "
            f"USING {q_stage} AS s ON {on_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )


def _build_mysql_upsert(
    target_table: str, stage_table: str, merge_keys: List[str], columns: List[str]
) -> str:
    db_type = DBType.MYSQL
    q_target = quote_table_name(target_table, db_type)
    q_stage = quote_table_name(stage_table, db_type)
    all_cols = ", ".join(quote_column_name(c, db_type) for c in columns)
    update_cols = [c for c in columns if c not in merge_keys]

    if update_cols:
        set_clause = ", ".join(
            f"{quote_column_name(c, db_type)} = VALUES({quote_column_name(c, db_type)})"
            for c in update_cols
        )
    else:
        first_key = quote_column_name(merge_keys[0], db_type)
        set_clause = f"{first_key} = {first_key}"

    return (
        f"INSERT INTO {q_target} ({all_cols}) "
        f"SELECT {all_cols} FROM {q_stage} "
        f"ON DUPLICATE KEY UPDATE {set_clause}"
    )


def build_upsert_sql(
    db_type: str,
    stage_table: str,
    target_table: str,
    merge_key: Union[str, List[str]],
    columns: List[str],
) -> str:
    """Generate an UPSERT SQL statement appropriate for the target database.

    Consumes canonical_merge_keys directly (must be a tuple).
    """
    if isinstance(merge_key, str):
        canonical_merge_keys = [k.strip() for k in merge_key.split(",") if k.strip()]
    else:
        canonical_merge_keys = merge_key

    if db_type == DBType.POSTGRES:
        return _build_postgres_upsert(target_table, stage_table, canonical_merge_keys, columns)
    elif db_type == DBType.ORACLE:
        return _build_oracle_upsert(target_table, stage_table, canonical_merge_keys, columns)
    elif db_type == DBType.MSSQL:
        return _build_mssql_upsert(target_table, stage_table, canonical_merge_keys, columns)
    elif db_type == DBType.MYSQL:
        return _build_mysql_upsert(target_table, stage_table, canonical_merge_keys, columns)
    else:
        raise ValueError(
            f"Unsupported database type for UPSERT: '{db_type}'. "
            f"Supported: {DBType.POSTGRES}, {DBType.ORACLE}, {DBType.MSSQL}, {DBType.MYSQL}"
        )


def _relax_not_null_constraints(target_cfg, target_table: str, merge_key: str) -> None:
    """
    Drop NOT NULL constraints from all non-PK columns in a PostgreSQL table.

    Spark infers NOT NULL from source data; if the source later sends NULLs for
    those columns the UPSERT will fail. We make every non-key column nullable to
    ensure the landing/gold layer always accepts source data as-is.

    Rules:
      - Skips ALL primary key columns (not just the merge_key), because PostgreSQL
        does not allow dropping NOT NULL from a column that is part of a PK.
      - Executes each ALTER statement individually so one failure never blocks others.
      - No-op for any non-PostgreSQL database type.
    """
    from typing import Mapping
    if isinstance(target_cfg, Mapping):
        driver = str(target_cfg.get("driver", "")).lower()
        username = target_cfg.get("username") or target_cfg.get("user")
        password = target_cfg.get("password_resolved") or target_cfg.get("password")
    else:
        driver = str(getattr(target_cfg, "driver", "")).lower()
        username = getattr(target_cfg, "username", None) or getattr(target_cfg, "user", None)
        password = getattr(target_cfg, "password_resolved", None) or getattr(target_cfg, "password", None)

    if "postgresql" not in driver and "postgres" not in driver:
        return

    resolved_host, port, database = _parse_postgres_connection(target_cfg)

    # Parse schema and table from the fully-qualified table name (schema.table)
    parts = target_table.split(".")
    schema_name = parts[0] if len(parts) > 1 else "public"
    table_name  = parts[-1]

    try:
        import psycopg2 as pg2
        conn = pg2.connect(
            host=resolved_host, port=port, dbname=database,
            user=username, password=password
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            # Step A: Collect ALL primary key columns for this table
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints  tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema    = kcu.table_schema
                 AND tc.table_name      = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND LOWER(tc.table_schema) = LOWER(%s)
                  AND LOWER(tc.table_name)   = LOWER(%s)
            """, [schema_name, table_name])
            pk_cols = {row[0] for row in cur.fetchall()}

            # Step B: Find all NOT NULL columns that are NOT part of any PK
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE LOWER(table_schema) = LOWER(%s)
                  AND LOWER(table_name)   = LOWER(%s)
                  AND is_nullable  = 'NO'
            """, [schema_name, table_name])
            not_null_cols = [row[0] for row in cur.fetchall() if row[0] not in pk_cols]

            if not_null_cols:
                log.info(
                    f"[UPSERT] Relaxing NOT NULL on {len(not_null_cols)} non-PK column(s) "
                    f"in {target_table}: {not_null_cols}"
                )
                # Step C: Execute each ALTER individually — one failure must not block others
                for col in not_null_cols:
                    try:
                        cur.execute(
                            f'ALTER TABLE {quote_table_name(target_table, DBType.POSTGRES)} ALTER COLUMN "{col}" DROP NOT NULL'
                        )
                    except Exception as col_err:
                        log.warning(
                            f'[UPSERT] Could not drop NOT NULL for column "{col}" '
                            f'in {target_table}: {col_err}'
                        )
        conn.close()
    except Exception as e:
        log.warning(f"[UPSERT] Could not relax NOT NULL constraints on {target_table}: {e}")




def _collect_postgres_merge_stats(target_cfg: dict, target_table: str) -> Optional[dict]:
    """Collect WAL position and table statistics (inserts, updates, HOT updates) for target_table."""
    driver = target_cfg.get("driver", "")
    if "postgresql" not in driver.lower():
        return None

    parts = target_table.split(".")
    schema_name = parts[0] if len(parts) > 1 else "public"
    table_name = parts[-1]

    try:
        import psycopg2 as pg2
        resolved_host, port, database = _parse_postgres_connection(target_cfg)
        conn = pg2.connect(
            host=resolved_host, port=port, dbname=database,
            user=target_cfg["user"], password=target_cfg["password"]
        )
        conn.autocommit = True
        stats = {}
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT pg_current_wal_lsn()")
                stats["wal_lsn"] = cur.fetchone()[0]
            except Exception:
                stats["wal_lsn"] = None

            cur.execute("""
                SELECT n_tup_ins, n_tup_upd, n_tup_hot_upd, n_dead_tup
                FROM pg_stat_user_tables
                WHERE schemaname = %s AND relname = %s
            """, [schema_name, table_name])
            row = cur.fetchone()
            if row:
                stats["n_tup_ins"] = row[0] or 0
                stats["n_tup_upd"] = row[1] or 0
                stats["n_tup_hot_upd"] = row[2] or 0
                stats["n_dead_tup"] = row[3] or 0
            else:
                stats["n_tup_ins"] = stats["n_tup_upd"] = stats["n_tup_hot_upd"] = stats["n_dead_tup"] = 0
        conn.close()
        return stats
    except Exception as e:
        log.warning(f"[Merge Diagnostics] Could not collect stats for {target_table}: {e}")
        return None


def build_bounded_exception_dml(
    db_type: str,
    target_table: str,
    normal_stage_table: str,
    exception_stage_table: str,
    merge_key: Union[str, List[str]],
    columns: List[str],
    exception_values: Optional[List[int]] = None,
) -> dict:
    """Build bounded partial-index-safe upsert DML for keyed exception rows.

    Returns a dict with two keys:
      - 'normal_upsert':     ON CONFLICT ... WHERE key NOT IN (...) partial-index upsert
                             for the normal (non-exception) staging lane. None for non-postgres.
      - 'exception_replace': Atomic CTE DELETE+INSERT for the exception staging lane.
                             None for non-postgres dialects (fail-closed).

    Strictly validates that exception_values is a non-empty list of pure integers
    (bool, str, float are all rejected with ValueError).

    Args:
        db_type:               Target database type. Only 'postgres'/'postgresql' is supported.
        target_table:          Fully-qualified target table name (e.g. 'landing.events').
        normal_stage_table:    Staging table for normal (non-exception) rows.
        exception_stage_table: Staging table for exception rows.
        merge_key:             Business key column name(s) — str or List[str].
        columns:               Full column list to insert.
        exception_values:      Non-empty list of pure int exception key values.

    Returns:
        Dict with 'normal_upsert' and 'exception_replace' keys.

    Raises:
        ValueError: If exception_values is empty or contains non-integer values.
    """
    # Normalise merge_key to a list
    if isinstance(merge_key, str):
        key_list = [k.strip() for k in merge_key.split(",") if k.strip()]
    else:
        key_list = list(merge_key)

    # Validate exception_values — required, non-empty, pure integers only
    if not exception_values:
        raise ValueError(
            "build_bounded_exception_dml: exception values are invalid — "
            "a non-empty list of integers is required."
        )
    validated: List[int] = []
    for v in exception_values:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError(
                f"build_bounded_exception_dml: exception values are invalid — "
                f"all entries must be pure int; got {type(v).__name__!r} for {v!r}."
            )
        validated.append(v)

    is_postgres = str(db_type).lower() in ("postgres", "postgresql")

    if not is_postgres:
        log.warning(
            f"[BoundedExceptionDML] dialect '{db_type}' is not supported; "
            "exception DML is fail-closed (None)."
        )
        return {"normal_upsert": None, "exception_replace": None}

    q_target = quote_table_name(target_table, db_type)
    q_normal_stage = quote_table_name(normal_stage_table, db_type)
    q_exc_stage = quote_table_name(exception_stage_table, db_type)
    q_key = quote_column_name(key_list[0], db_type)
    in_list = ", ".join(str(v) for v in validated)
    col_list = ", ".join(quote_column_name(c, db_type) for c in columns)

    # Normal lane: partial-index ON CONFLICT excludes exception keys
    key_name_lower = key_list[0].lower()
    update_cols = [c for c in columns if c.lower() != key_name_lower]
    if update_cols:
        set_cols = ", ".join(f"{quote_column_name(c, db_type)} = EXCLUDED.{quote_column_name(c, db_type)}" for c in update_cols)
        ephemeral_run_audit_cols = {
            "dml_timestamp",
            "landing_ingestion_timestamp",
            "pipeline_run_id",
        }
        compare_cols = [c for c in update_cols if c.lower() not in ephemeral_run_audit_cols]
        if compare_cols:
            target_tuple = ", ".join(f"t.{quote_column_name(c, db_type)}" for c in compare_cols)
            excluded_tuple = ", ".join(f"EXCLUDED.{quote_column_name(c, db_type)}" for c in compare_cols)
            where_clause = f"\nWHERE ({target_tuple}) IS DISTINCT FROM ({excluded_tuple})"
        else:
            where_clause = ""
        set_clause = f"DO UPDATE SET {set_cols}{where_clause}"
    else:
        set_clause = "DO NOTHING"

    normal_upsert = (
        f"INSERT INTO {q_target} AS t ({col_list})\n"
        f"SELECT {col_list} FROM {q_normal_stage}\n"
        f"ON CONFLICT ({q_key}) WHERE {q_key} NOT IN ({in_list})\n"
        f"{set_clause};"
    )

    # Exception lane: atomic CTE delete-then-replace
    exception_replace = (
        f"WITH deleted AS (\n"
        f"    DELETE FROM {q_target} AS t\n"
        f"    WHERE t.{q_key} IN ({in_list})\n"
        f"    RETURNING 1\n"
        f")\n"
        f"INSERT INTO {q_target} ({col_list})\n"
        f"SELECT {col_list} FROM {q_exc_stage}\n"
        f"WHERE {q_key} IN ({in_list});"
    )

    return {"normal_upsert": normal_upsert, "exception_replace": exception_replace}


def execute_upsert(spark, df: DataFrame, target_cfg: dict,
                   target_table: str, merge_key: str,
                   columns: List[str], db_type: str,
                   before_target_dml=None,
                   driver_target_cfg: Optional[dict] = None,
                   conditional_exception_values: Optional[dict] = None,
                   **kwargs) -> dict:
    """Execute a full staging-table UPSERT workflow with 4-part telemetry and PostgreSQL diagnostics.

    0. [PostgreSQL only] Relax NOT NULL constraints on non-PK target columns.
    1. Write DataFrame to a temporary staging table via JDBC overwrite.
    2. Generate and execute the UPSERT SQL for the target DB type.
    3. Drop the staging table.

    Returns:
        Dict containing 4-part execution telemetry, sub-stage rates, and PostgreSQL merge diagnostics.
    """
    import time
    from src.exceptions import ExecutionOwnershipRequiredError

    if before_target_dml is None:
        raise ExecutionOwnershipRequiredError(
            "UPSERT requires an active batch_log lease."
        )
    driver_cfg = driver_target_cfg or target_cfg
    stage_table = f"{target_table}_stg"
    row_count = df.count()
    metrics = {
        "rows": row_count,
        "create_staging_sec": 0.0,
        "spark_to_staging_sec": 0.0,
        "spark_to_staging_rps": 0.0,
        "merge_execute_sec": 0.0,
        "merge_execute_rps": 0.0,
        "drop_staging_sec": 0.0,
        "total_sec": 0.0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
        "update_ratio_pct": 0.0,
        "hot_update_ratio_pct": 0.0,
        "wal_bytes_generated": 0,
        "wal_mb": 0.0,
        "target_persisted_rows": 0,
        "target_reconciliation": {},
    }

    try:
        # Step 1: Write to staging table (overwrite)
        log.info(f"[UPSERT] Writing {row_count:,} rows to staging table {stage_table}")
        before_target_dml()
        t1_start = time.perf_counter()
        writer = apply_jdbc_options(df.write.format("jdbc"), target_cfg)
        writer.option("dbtable", quote_table_name(stage_table, db_type)) \
            .option("batchsize", "10000") \
            .mode("overwrite") \
            .save()
        t1_elapsed = time.perf_counter() - t1_start
        metrics["spark_to_staging_sec"] = round(t1_elapsed, 3)
        metrics["spark_to_staging_rps"] = round(row_count / t1_elapsed, 2) if t1_elapsed > 0 else 0.0

        # Step 2: Pre-merge PostgreSQL Diagnostics Snapshot
        pre_stats = _collect_postgres_merge_stats(driver_cfg, target_table)

        # EXPLAIN ANALYZE is deliberately disabled for DML because it executes
        # the statement. Use EXPLAIN without ANALYZE in an offline benchmark.
        if isinstance(merge_key, str):
            canonical_merge_keys = [k.strip() for k in merge_key.split(",") if k.strip()]
        else:
            canonical_merge_keys = merge_key

        upsert_sql = build_upsert_sql(db_type, stage_table, target_table, merge_key, columns)
        if conditional_exception_values:
            # Normalise to a plain list regardless of whether the caller passed
            # a tuple, set, frozenset, list, or legacy dict payload.
            exc_values = _coerce_to_value_list(conditional_exception_values)
            if exc_values:
                exc_dml = build_bounded_exception_dml(
                    db_type=db_type,
                    target_table=target_table,
                    normal_stage_table=stage_table,
                    exception_stage_table=f"{stage_table}_exc",
                    merge_key=merge_key,
                    columns=columns,
                    exception_values=exc_values,
                )
                if exc_dml.get("normal_upsert"):
                    upsert_sql = exc_dml["normal_upsert"]

        # Step 2: Execute actual UPSERT SQL
        t2_start = time.perf_counter()
        before_target_dml()
        if str(db_type).lower() in ("postgres", "postgresql") and not upsert_sql.strip().startswith("SET "):
            stage_q = quote_table_name(stage_table, db_type)
            upsert_sql = (
                f"SET synchronous_commit = 'off';\n"
                f"SET work_mem = '64MB';\n"
                f"ALTER TABLE {stage_q} SET (autovacuum_enabled = false);\n"
                f"{upsert_sql}"
            )
        log.info(f"[UPSERT] Executing: {upsert_sql[:200]}...")
        upsert_cfg = {**driver_cfg, "sql": upsert_sql}
        safe_execute_jdbc(df=None, target_cfg=upsert_cfg, operation="execute_sql", spark=spark)

        t2_elapsed = time.perf_counter() - t2_start
        metrics["merge_execute_sec"] = round(t2_elapsed, 3)
        metrics["merge_execute_rps"] = round(row_count / t2_elapsed, 2) if t2_elapsed > 0 else 0.0

        # Step 2c: Convergence verification.
        # IMPORTANT — context isolation:
        # _verify_staging_target_convergence executes a query via spark.read.format("jdbc"),
        # which runs on Spark Connect executors inside the container/Docker network.
        # Executors can only reach the target database through the Spark-network route
        # (target_cfg), NOT through the driver's direct host (driver_cfg).
        # Passing driver_cfg here would route executor JDBC connections to the driver's
        # local IP, which is unreachable from inside the container and causes the
        # convergence query to fail, leaving all reconciliation row-counts as -1.
        try:
            convergence = _verify_staging_target_convergence(
                spark=spark,
                target_cfg=target_cfg,   # Spark-network route (executor context)
                target_table=target_table,
                stage_table=stage_table,
                merge_key=merge_key,
                db_type=db_type,
                expected_rows=row_count,
            )
            metrics["target_reconciliation"] = convergence
            metrics["target_persisted_rows"] = convergence.get("matched_rows", 0)
        except Exception as reconcile_exc:
            log.warning(f"[UPSERT] Convergence verification failed for {target_table}: {reconcile_exc}")
            metrics["target_reconciliation"] = {"error": str(reconcile_exc)}
            metrics["target_persisted_rows"] = 0

        # Step 2d: Post-merge PostgreSQL Diagnostics Snapshot & Delta Calculation
        post_stats = _collect_postgres_merge_stats(driver_cfg, target_table)
        if pre_stats and post_stats:
            rows_ins = max(0, post_stats["n_tup_ins"] - pre_stats["n_tup_ins"])
            rows_upd = max(0, post_stats["n_tup_upd"] - pre_stats["n_tup_upd"])
            hot_upd = max(0, post_stats["n_tup_hot_upd"] - pre_stats["n_tup_hot_upd"])
            skipped = max(0, row_count - (rows_ins + rows_upd))

            metrics["rows_inserted"] = rows_ins
            metrics["rows_updated"] = rows_upd
            metrics["rows_skipped"] = skipped
            metrics["update_ratio_pct"] = round((rows_upd / row_count) * 100, 2) if row_count > 0 else 0.0
            metrics["hot_update_ratio_pct"] = round((hot_upd / rows_upd) * 100, 2) if rows_upd > 0 else 0.0

            if pre_stats.get("wal_lsn") and post_stats.get("wal_lsn"):
                try:
                    import psycopg2 as pg2
                    resolved_host, port, database = _parse_postgres_connection(driver_cfg)
                    conn = pg2.connect(host=resolved_host, port=port, dbname=database, user=driver_cfg["user"], password=driver_cfg["password"])
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_wal_lsn_diff(%s, %s)", [post_stats["wal_lsn"], pre_stats["wal_lsn"]])
                        wal_bytes = cur.fetchone()[0] or 0
                        metrics["wal_bytes_generated"] = wal_bytes
                        metrics["wal_mb"] = round(wal_bytes / (1024 * 1024), 2)
                    conn.close()
                except Exception:
                    pass

        log.info(
            f"[UPSERT] Successfully merged into {target_table} "
            f"(spark_to_staging: {metrics['spark_to_staging_sec']}s @ {metrics['spark_to_staging_rps']} rps, "
            f"merge_execute: {metrics['merge_execute_sec']}s @ {metrics['merge_execute_rps']} rps, "
            f"rows_updated: {metrics['rows_updated']:,}, rows_skipped: {metrics['rows_skipped']:,}, "
            f"wal_generated: {metrics['wal_mb']} MB)"
        )

    finally:
        # Step 3: Drop staging table
        t3_start = time.perf_counter()
        try:
            drop_sql = f"DROP TABLE IF EXISTS {quote_table_name(stage_table, db_type)}"
            drop_cfg = {**driver_cfg, "sql": drop_sql}
            safe_execute_jdbc(df=None, target_cfg=drop_cfg, operation="drop", spark=spark)
            log.info(f"[UPSERT] Dropped staging table {stage_table}")
        except Exception as drop_exc:
            log.warning(f"[UPSERT] Could not drop staging table {stage_table}: {drop_exc}")
        metrics["drop_staging_sec"] = round(time.perf_counter() - t3_start, 3)
        metrics["total_sec"] = round(
            metrics["create_staging_sec"] + metrics["spark_to_staging_sec"] +
            metrics["merge_execute_sec"] + metrics["drop_staging_sec"], 3
        )

    return metrics





def drop_staging_table(spark, target_cfg: dict, stage_table: str) -> None:
    """Drop a staging table. Convenience wrapper around safe_execute_jdbc."""
    db_type = "postgres"
    driver = target_cfg.get("driver", "")
    if "oracle" in driver.lower():
        db_type = "oracle"
    elif "sqlserver" in driver.lower():
        db_type = "mssql"
    elif "mysql" in driver.lower():
        db_type = "mysql"
    quoted_stage = quote_table_name(stage_table, db_type)
    drop_cfg = {**target_cfg, "sql": f"DROP TABLE IF EXISTS {quoted_stage}"}
    safe_execute_jdbc(df=None, target_cfg=drop_cfg, operation="drop", spark=spark)
