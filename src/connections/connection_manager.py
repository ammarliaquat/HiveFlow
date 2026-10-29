# src/connections/connection_manager.py
"""
Manages JDBC connection properties per source_tag.
Never stores live connections — Spark manages those internally.
This class holds the JDBC URL and properties dict for each source.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import yaml
import os
import configparser
from contextlib import contextmanager
import psycopg2
import logging
from enum import Enum
from dataclasses import field
from types import MappingProxyType

logger = logging.getLogger(__name__)
log = logger

from src.constants import DBType

class ConfigurationError(RuntimeError):
    """Raised when configuration validation fails, such as missing required credential environment variables."""
    pass

class ExecutionContext(Enum):
    DRIVER = "driver"
    SPARK = "spark"

@dataclass(frozen=True)
class ResolvedJdbcConfig:
    connection_tag: str
    db_type: str
    role: str
    context: ExecutionContext
    url: str
    user: str
    password: str = field(repr=False, compare=False)
    driver: str = ""
    properties: MappingProxyType = field(
        default_factory=lambda: MappingProxyType({}),
        repr=False,
        compare=False,
    )

@dataclass
class ConnectionConfig:

    source_tag: str
    db_type: str
    jdbc_url: str
    username: str
    credential_reference: str
    driver: str
    connection_options: Dict[str, Any] = None
    database: str = ""
    schema: str = ""
    host: str = ""
    port: str = ""
    spark_host: str = ""
    supports_source: bool = True
    supports_target: bool = False
    env: str = "PROD"
    connection_type: str = "DATABASE"
    owner: str = "Admin"
    team: str = "Data Engineering"
    purpose: str = ""
    password_resolved: str = ""
    max_concurrent_connections: int = 10

    @property
    def max_parallel_tables(self) -> int:
        """Backwards compatibility property."""
        return self.max_concurrent_connections

    def __post_init__(self):
        if self.connection_options is None:
            self.connection_options = {}

    def get_jdbc_url(self, context: str = "driver") -> str:
        """Return context-appropriate JDBC URL.

        - 'driver': Uses self.host (resolvable by Python driver process)
        - 'spark': Uses self.spark_host if explicitly configured, falling back to self.host
        """
        import os
        is_airflow = bool(os.environ.get("AIRFLOW_CTX_DAG_ID"))
        if (context.lower() in ("spark", "executor", "worker") or is_airflow) and self.spark_host:
            effective_host = self.spark_host
        else:
            effective_host = self.host

        if not effective_host or effective_host == self.host:
            return self.jdbc_url

        service_val = self.connection_options.get("service") if isinstance(self.connection_options, dict) else None
        return ConnectionManager.build_jdbc_url_static(
            db_type=self.db_type,
            server=effective_host,
            port=self.port,
            database=self.database,
            service=service_val
        )

    @property
    def password(self) -> str:
        if self.password_resolved:
            return self.password_resolved
        return self.credential_reference

    @property
    def role(self) -> str:
        """Backwards compatibility property returning 'source', 'target', or 'both'."""
        if self.supports_source and self.supports_target:
            return "both"
        if self.supports_target:
            return "target"
        return "source"

    @property
    def jdbc_properties(self) -> Dict[str, str]:
        """Returns the properties dict used by spark.read.jdbc()"""
        props = {
            "user": self.username,
            "password": self.password,
            "driver": self.driver,
        }
        if self.db_type.lower() == "oracle":
            props["oracle.jdbc.timezoneAsRegion"] = "false"
            props["oracle.sessionTimeZone"] = "UTC"
            props["sessionTimeZone"] = "UTC"
            
        if self.connection_options:
            for k, v in self.connection_options.items():
                if k not in {"spark_host"} and k not in props and isinstance(v, (str, int, bool)):
                    props[k] = str(v)
        return props


class ConnectionManager:
    """
    Singleton-style manager that holds all connection configs.
    Loaded once at startup from config.yaml.
    """

    def __init__(self, config_path: Optional[str] = None):
        if config_path is None or config_path == "config/config.yaml":
            if os.path.exists("config/config.local.yaml"):
                config_path = "config/config.local.yaml"
            else:
                config_path = "config/config.yaml"

        # Ensure .env is loaded automatically with override=True to hot-reload changes
        try:
            from dotenv import load_dotenv
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            load_dotenv(os.path.join(base_dir, ".env"), override=True)
        except ImportError:
            pass

        # Self-healing environment fixes for PySpark on Windows
        import sys
        if not os.environ.get("JAVA_HOME") or not os.path.exists(os.environ.get("JAVA_HOME", "")):
            if os.path.exists("C:/Program Files/Java/jdk-17"):
                os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17"
        if os.path.exists("C:/hadoop"):
            os.environ["HADOOP_HOME"] = "C:/hadoop"
            os.environ["hadoop.home.dir"] = "C:/hadoop"
            if "C:\\hadoop\\bin" not in os.environ.get("PATH", "") and "C:/hadoop/bin" not in os.environ.get("PATH", ""):
                os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

        # Resolve config path relative to the current file if it's not absolute or found directly
        if not os.path.isabs(config_path) and not os.path.exists(config_path):
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(base_dir, config_path)
            
        self._configs: Dict[str, ConnectionConfig] = {}
        self._raw_config = self._load_config(config_path)
        self._build_connections()

        import threading
        self._semaphores: Dict[str, threading.Semaphore] = {
            tag: threading.Semaphore(cfg.max_parallel_tables)
            for tag, cfg in self._configs.items()
        }

    def get_semaphore(self, source_tag: str):
        tag_key = source_tag.lower()
        if tag_key not in self._semaphores:
            if tag_key in self._configs:
                import threading
                self._semaphores[tag_key] = threading.Semaphore(self._configs[tag_key].max_parallel_tables)
            else:
                raise KeyError(f"No semaphore found for connection '{source_tag}'.")
        return self._semaphores[tag_key]

    def _load_config(self, path: str) -> dict:
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        return self._resolve_env_vars(raw)

    def _resolve_env_vars(self, obj):
        """Replace ${ENV_VAR} patterns with actual env variable values."""
        if isinstance(obj, str):
            import re
            pattern = re.compile(r'\$\{([^}]+)\}')
            alias_map = {
                "MSOPFORG_DB_PASSWORD": ["MYSQL_MEMBERSHIP_PASSWORD"],
                "METADATA_DB_PASSWORD": ["DB_PASSWORD"],
                "LAND_N_DB_PASSWORD": ["DB_PASSWORD"],
                "LAND_TEST_DB_PASSWORD": ["DB_PASSWORD"]
            }
            def replacer(m):
                expr = m.group(1)
                default_val = ""
                var_name = expr.strip()
                if ":-" in expr:
                    var_name, default_val = expr.split(":-", 1)
                    var_name = var_name.strip()
                    default_val = default_val.strip()
                elif ":" in expr:
                    var_name, default_val = expr.split(":", 1)
                    var_name = var_name.strip()
                is_password_field = any(k in var_name.upper() for k in ("PASSWORD", "SECRET", "TOKEN", "CREDENTIAL", "PASS"))
                val = os.environ.get(var_name)
                if (val is None or not str(val).strip()) and var_name in alias_map:
                    for alias in alias_map[var_name]:
                        alias_val = os.environ.get(alias)
                        if alias_val is not None and str(alias_val).strip():
                            val = alias_val
                            break

                if is_password_field:
                    if val is None or not str(val).strip():
                        raise ConfigurationError(
                            f"Missing required credential environment variable: '${{{var_name}}}'. "
                            f"Passwords must be set via environment variables with zero fallback."
                        )
                    return val

                return val if val is not None else default_val
            return pattern.sub(replacer, obj)

        elif isinstance(obj, dict):
            return {k: self._resolve_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_env_vars(i) for i in obj]
        return obj



    def _normalize_db_type(self, db_type_str: str) -> str:
        db_type_lower = db_type_str.lower()
        if "postgres" in db_type_lower:
            return "postgres"
        if "oracle" in db_type_lower:
            return "oracle"
        if "mssql" in db_type_lower or "sqlserver" in db_type_lower:
            return "mssql"
        if "mysql" in db_type_lower:
            return "mysql"
        return db_type_lower

    @staticmethod
    def build_jdbc_url_static(db_type: str, server: str, port: str, database: str, service: str = None) -> str:
        db_type_lower = db_type.lower()
        if "postgres" in db_type_lower:
            return f"jdbc:postgresql://{server}:{port}/{database}"
        elif "mysql" in db_type_lower:
            return (
                f"jdbc:mysql://{server}:{port}/{database}"
                f"?yearIsDateType=false&zeroDateTimeBehavior=convertToNull"
                f"&useSSL=false&allowPublicKeyRetrieval=true"
                f"&connectionTimeZone=SERVER&preserveInstants=false"
                f"&connectTimeout=20000&socketTimeout=60000"
                f"&rewriteBatchedStatements=true"
            )
        elif "mssql" in db_type_lower or "sqlserver" in db_type_lower:
            return f"jdbc:sqlserver://{server}:{port};databaseName={database}"
        elif "oracle" in db_type_lower:
            if service and service.lower() != "none" and service.strip() != "":
                url = f"jdbc:oracle:thin:@//{server}:{port}/{service}"
            else:
                url = f"jdbc:oracle:thin:@{server}:{port}:{database}"
            return f"{url}?oracle.jdbc.timezoneAsRegion=false"
        elif db_type_lower in ("file", "csv", "json", "parquet", "api", "s3", "ftp"):
            return f"{db_type_lower}://{server}/{database}".rstrip("/")
        else:
            return f"custom://{server}:{port}/{database}".rstrip("/")

    def _build_jdbc_url(self, db_type: str, server: str, port: str, database: str, service: str = None) -> str:
        return self.build_jdbc_url_static(db_type, server, port, database, service)

    def _build_connections(self):
        meta_section = self._raw_config.get("metadata_db_section", "RepositoryDB").lower()
        
        skipped = 0
        errors = 0
        
        try:
            with self.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    # Schema version check
                    try:
                        cur.execute("SELECT setting_value FROM metadata.system_settings WHERE setting_key = 'schema_version'")
                        version_row = cur.fetchone()
                        if version_row:
                            version = version_row[0]
                            required_version = self.pipeline_config.get("required_schema_version", "1.0.0")
                            if version != required_version:
                                raise RuntimeError(f"Metadata schema version mismatch: Found {version}, required {required_version}")
                    except Exception as e:
                        if isinstance(e, RuntimeError):
                            raise e
                        # Table might not exist in old environments
                        logger.warning(f"Could not verify schema_version (is 010_system_settings.sql applied?): {e}")
                        conn.rollback()
                        
                    query = """
                        SELECT tag, connection_type, supports_source, supports_target, environment,
                               db_type, server, port, database_name, schema_name, username,
                               credential_reference, service_name, connection_options,
                               owner, team, purpose, max_concurrent_connections
                        FROM metadata.connections
                        WHERE is_active = 'y' AND deleted_at IS NULL
                    """
                    cur.execute(query)
                    rows = cur.fetchall()
                    for row in rows:
                        (tag, c_type, s_src, s_tgt, env, db_type, server, port, database_name, schema_name,
                         username, cred_ref, service_name, conn_opts, owner, team, purpose, max_concurrent_connections) = row
                        
                        if tag.lower() == meta_section:
                            continue
                            
                        if tag.lower() in self._configs:
                            errors += 1
                            raise RuntimeError(f"Startup error: Duplicate source_tag detected: '{tag}'")
                            
                        # Validation
                        if not db_type:
                            logger.error(f"Skipping connection '{tag}': Missing db_type")
                            skipped += 1
                            continue
                        if not server:
                            logger.error(f"Skipping connection '{tag}': Missing server (host)")
                            skipped += 1
                            continue
                        if not schema_name and db_type.lower() in ("oracle", "postgres", "postgresql", "mssql"):
                            logger.warning(f"Connection '{tag}' is missing a schema name. Queries may default to user schema.")
                            
                        db_type_norm = self._normalize_db_type(db_type)
                        driver = DBType.JDBC_DRIVERS.get(db_type_norm)
                        password_res = self._resolve_env_vars(cred_ref or "")
                        username_res = self._resolve_env_vars(username or "")
                        server_res = self._resolve_env_vars(server or "")

                        # Parse options if JSON string
                        if isinstance(conn_opts, str):
                            import json
                            try:
                                conn_opts = json.loads(conn_opts)
                            except Exception:
                                conn_opts = {}
                        elif conn_opts is None:
                            conn_opts = {}

                        # Endpoint Resolution Contract:
                        # 1. Explicit connection_options["spark_host"] has highest priority.
                        # 2. SPARK_CONTAINER_DB_HOST is the deployment-wide target-network override for containerized PostgreSQL targets.
                        # 3. Otherwise the configured driver host remains authoritative.
                        # Source routes and external targets (MySQL, Oracle, MSSQL) are never rewritten by a container deployment override.
                        spark_host_opt = conn_opts.get("spark_host", "") if isinstance(conn_opts, dict) else ""
                        if spark_host_opt:
                            spark_host_res = self._resolve_env_vars(spark_host_opt)
                            if not str(spark_host_res or "").strip():
                                raise ConfigurationError(
                                    f"Connection '{tag}' declares spark_host but it resolved to an empty value."
                                )
                        elif bool(s_tgt) and not bool(s_src) and db_type_norm in ("postgres", "postgresql"):
                            spark_host_res = str(os.environ.get("SPARK_CONTAINER_DB_HOST", "") or "").strip()
                        else:
                            spark_host_res = ""
                        
                        database_res = self._resolve_env_vars(database_name or "")
                        service_res = self._resolve_env_vars(service_name or "")
                        
                        jdbc_url = self._build_jdbc_url(
                            db_type=db_type,
                            server=server_res,
                            port=str(port),
                            database=database_res,
                            service=service_res
                        )
                        self._configs[tag.lower()] = ConnectionConfig(
                            source_tag=tag,
                            db_type=db_type_norm,
                            jdbc_url=jdbc_url,
                            username=username_res,
                            credential_reference=cred_ref or "",
                            driver=driver,
                            connection_options=conn_opts,
                            database=database_res,
                            schema=schema_name or (database_res if db_type_norm == "mysql" else ""),
                            host=server_res,
                            spark_host=spark_host_res,
                            port=str(port),
                            supports_source=bool(s_src),
                            supports_target=bool(s_tgt),
                            env=(env or "PROD").upper(),
                            connection_type=c_type or "DATABASE",
                            owner=owner or "Admin",
                            team=team or "Data Engineering",
                            purpose=purpose or "",
                            password_resolved=password_res,
                            max_concurrent_connections=max_concurrent_connections if max_concurrent_connections is not None else 10
                        )
        except Exception as e:
            error_msg = f"Cannot initialize ConnectionManager. Metadata database unavailable: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
            
        # Validation Report
        report_lines = ["\n" + "="*40, "  Connection Validation Report", "="*40]
        report_lines.append(f"Loaded: {len(self._configs)} connections\n")
        
        db_types = {}
        for cfg in self._configs.values():
            db_types[cfg.db_type] = db_types.get(cfg.db_type, 0) + 1
            
        for db, cnt in sorted(db_types.items()):
            report_lines.append(f"  {db.capitalize():<12} {cnt}")
            
        report_lines.append(f"\nSkipped: {skipped}")
        report_lines.append(f"Errors: {errors}")
        report_lines.append("="*40)
        
        logger.info("\n".join(report_lines))

    def get_capabilities(self, db_type: str) -> Dict[str, bool]:
        """Query metadata.system_capabilities matrix for database-specific capabilities."""
        db_norm = self._normalize_db_type(db_type)
        try:
            with self.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT supports_parallel_read, supports_incremental, 
                               supports_partitioning, supports_merge, supports_bulk_load,
                               supports_snapshot, supports_cdc
                        FROM metadata.system_capabilities
                        WHERE LOWER(db_type) = LOWER(%s)
                    """, [db_norm])
                    row = cur.fetchone()
                    if row:
                        return {
                            "supports_parallel_read": bool(row[0]),
                            "supports_incremental": bool(row[1]),
                            "supports_partitioning": bool(row[2]),
                            "supports_merge": bool(row[3]),
                            "supports_bulk_load": bool(row[4]),
                            "supports_snapshot": bool(row[5]),
                            "supports_cdc": bool(row[6]),
                        }
        except Exception as e:
            logger.warning(f"Could not fetch capabilities for {db_type}: {e}")
        
        # Safe default capabilities fallback
        return {
            "supports_parallel_read": True,
            "supports_incremental": True,
            "supports_partitioning": True,
            "supports_merge": False,
            "supports_bulk_load": False,
            "supports_snapshot": True,
            "supports_cdc": False,
        }

    def get_dynamic_concurrency(self, source_tag: str, spark_executor_slots: int = 10, airflow_pool_slots: int = 10) -> int:
        """
        Calculate concurrency dynamically:
        min(Spark Executor Slots, Source DB Max Connections, Airflow Pool Slots)
        """
        cfg = self.get(source_tag)
        max_conn = getattr(cfg, "max_concurrent_connections", 10) or 10
        concurrency = min(spark_executor_slots, max_conn, airflow_pool_slots)
        logger.info(
            f"[Dynamic Concurrency] Calculated pool for '{source_tag}': {concurrency} "
            f"(Spark Slots: {spark_executor_slots}, DB Limit: {max_conn}, Airflow Pool: {airflow_pool_slots})"
        )
        return max(1, concurrency)

    def refresh(self):
        """Clear cached connection profiles and hot-reload from database."""
        self._configs.clear()
        self._build_connections()
        self._build_connections()
        
        import threading
        self._semaphores = {
            tag: threading.Semaphore(cfg.max_parallel_tables)
            for tag, cfg in self._configs.items()
        }

    def get(self, source_tag: str, expected_role: Optional[str] = None,
            expected_env: Optional[str] = None) -> ConnectionConfig:
        tag_key = source_tag.lower()
        if tag_key not in self._configs:
            raise KeyError(f"No connection configured for source_tag: '{source_tag}'")
        cfg = self._configs[tag_key]
        if expected_role:
            exp = expected_role.lower()
            if exp == "source" and not cfg.supports_source:
                raise ValueError(
                    f"Security Exception: Connection '{source_tag}' is configured for role '{cfg.role}', but requested for operation role '{exp}'."
                )
            elif exp == "target" and not cfg.supports_target:
                raise ValueError(
                    f"Security Exception: Connection '{source_tag}' is configured for role '{cfg.role}', but requested for operation role '{exp}'."
                )
            elif exp == "both" and not (cfg.supports_source and cfg.supports_target):
                raise ValueError(
                    f"Security Exception: Connection '{source_tag}' is configured for role '{cfg.role}', but requested for operation role '{exp}'."
                )
        if expected_env and cfg.env and cfg.env.upper() != expected_env.upper():
            raise ValueError(
                f"Environment mismatch: '{source_tag}' is env='{cfg.env}', expected '{expected_env}'."
            )
        return cfg

    def has_connection(self, tag: str, expected_role: Optional[str] = None,
                       expected_env: Optional[str] = None) -> bool:
        """Check if a connection tag exists and optionally matches the expected role and env."""
        if not tag:
            return False
        tag_key = tag.lower()
        if tag_key not in self._configs:
            return False
        cfg = self._configs[tag_key]
        if expected_role:
            exp = expected_role.lower()
            if exp == "source" and not cfg.supports_source:
                return False
            elif exp == "target" and not cfg.supports_target:
                return False
            elif exp == "both" and not (cfg.supports_source and cfg.supports_target):
                return False
        if expected_env and cfg.env and cfg.env.upper() != expected_env.upper():
            return False
        return True

    def available_connection_tags(self, role: Optional[str] = None,
                                  env: Optional[str] = None) -> List[str]:
        """Return a list of available connection tag names matching optional role and env filters."""
        tags = []
        for tag_key, cfg in self._configs.items():
            if role:
                exp = role.lower()
                if exp == "source" and not cfg.supports_source:
                    continue
                elif exp == "target" and not cfg.supports_target:
                    continue
                elif exp == "both" and not (cfg.supports_source and cfg.supports_target):
                    continue
            if env and cfg.env and cfg.env.upper() != env.upper():
                continue
            tags.append(cfg.source_tag)
        return sorted(tags)

    def resolve_jdbc(self, connection: ConnectionConfig, context: ExecutionContext) -> ResolvedJdbcConfig:
        """
        Resolves a ConnectionConfig into an immutable, context-specific JDBC configuration block.
        """
        url = connection.get_jdbc_url(context=context.value)
        props = connection.jdbc_properties
        return ResolvedJdbcConfig(
            connection_tag=connection.source_tag,
            db_type=connection.db_type,
            role=connection.role,
            context=context,
            url=url,
            user=connection.username,
            password=connection.password,
            driver=connection.driver,
            properties=MappingProxyType(props)
        )

    @property
    def metadata_db_config(self) -> dict:
        repo_cfg = self._raw_config.get("repository_db")
        if not repo_cfg:
            raise KeyError("Metadata DB config 'repository_db' is missing from config.yaml.")
        return {
            "host": self._resolve_env_vars(repo_cfg.get("host", "")),
            "port": self._resolve_env_vars(str(repo_cfg.get("port", ""))),
            "database": self._resolve_env_vars(repo_cfg.get("database", "")),
            "username": self._resolve_env_vars(repo_cfg.get("username", "")),
            "password": self._resolve_env_vars(repo_cfg.get("password", "")),
            "schema": self._resolve_env_vars(repo_cfg.get("schema", "metadata"))
        }

    def get_metadata_db_url(self) -> str:
        meta = self.metadata_db_config
        return (
            f"jdbc:postgresql://{meta['host']}:{meta['port']}/{meta['database']}"
        )

    def get_metadata_db_properties(self) -> Dict[str, str]:
        meta = self.metadata_db_config
        return {
            "user": meta["username"],
            "password": meta["password"],
            "driver": "org.postgresql.Driver",
        }

    @property
    def paths(self) -> dict:
        return self._raw_config.get("paths", {})

    @property
    def pipeline_config(self) -> dict:
        return self._raw_config.get("pipeline", {})

    def get_raw_metadata_db_connection(self):
        """Return an un-managed open psycopg2 connection to the metadata DB (caller must close)."""
        cfg = self.metadata_db_config
        return psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["username"],
            password=cfg["password"],
            connect_timeout=5
        )

    @contextmanager
    def get_metadata_db_connection(self):
        """Yield an open psycopg2 connection to the metadata DB and close it automatically."""
        conn = self.get_raw_metadata_db_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def get_db_connection(self, tag: str, expected_role: Optional[str] = None):
        """Yield an open database connection for any connection tag in metadata.connections."""
        cfg = self.get(tag, expected_role=expected_role)
        db_type = cfg.db_type.lower()
        import os
        is_airflow = bool(os.environ.get("AIRFLOW_CTX_DAG_ID"))
        effective_host = cfg.spark_host if (is_airflow and cfg.spark_host) else cfg.host

        if db_type in ("postgresql", "postgres", "postgressql"):
            conn = psycopg2.connect(
                host=effective_host,
                port=int(cfg.port) if cfg.port and str(cfg.port).isdigit() else 5432,
                dbname=cfg.database,
                user=cfg.username,
                password=cfg.password,
            )
        elif db_type == "mysql":
            import pymysql
            conn = pymysql.connect(
                host=effective_host,
                port=int(cfg.port) if cfg.port and str(cfg.port).isdigit() else 3306,
                database=cfg.database,
                user=cfg.username,
                password=cfg.password,
            )
        elif db_type in ("oracle",):
            import oracledb
            # Enable Thin mode (doesn't need Oracle Client libraries)
            raw_dsn = cfg.jdbc_url.split("@")[1] if "@" in cfg.jdbc_url else cfg.database
            conn = oracledb.connect(
                user=cfg.username,
                password=cfg.password,
                dsn=raw_dsn.lstrip("/"),
            )
        else:
            raise NotImplementedError(f"Direct connection for db_type '{cfg.db_type}' is not supported.")
        try:
            yield conn
        finally:
            conn.close()
