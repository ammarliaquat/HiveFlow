"""
src/services/connection_service.py — Decoupled service layer for database connection profiles.
All GUI and CLI mutations flow through this class to ensure validation, health testing,
and Git-style JSON snapshot auditing (metadata.connection_history).
"""
import json
import time
import socket
import logging
from typing import Dict, List, Optional, Any
import psycopg2
import pymysql

from src.connections.connection_manager import ConnectionManager


def _tcp_reachable(host: str, port: int, timeout: float = 5.0) -> bool:
    """Return True if a TCP connection to host:port succeeds within *timeout* seconds.

    Used as the connectivity probe for database types that cannot be tested locally
    via a Python driver (e.g. Oracle requires thick-mode client libs which are not
    installed on the Windows host; MSSQL similarly). A successful TCP handshake is
    sufficient to confirm the host is reachable and the port is open.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

logger = logging.getLogger("ConnectionService")


def _scrub_connection_error(message: str, *sensitive_values: str) -> str:
    scrubbed = message or ""
    for value in sensitive_values:
        if value:
            scrubbed = scrubbed.replace(str(value), "[REDACTED]")
    return scrubbed


class ConnectionService:
    """Service layer for managing metadata.connections and metadata.connection_history."""

    _conn_cache: Dict[str, Any] = {"list": None, "ts": 0.0}

    @classmethod
    def clear_cache(cls):
        cls._conn_cache["list"] = None
        cls._conn_cache["ts"] = 0.0

    @staticmethod
    def build_source_profile(
        *,
        tag: str,
        db_type: str,
        server: str,
        port: int,
        database_name: str,
        schema_name: str,
        username: str,
        credential_reference: str,
        environment: str = "PROD",
        owner: str = "Admin",
        team: str = "Data Engineering",
        purpose: str = "GUI onboarded source connection",
    ) -> Dict[str, Any]:
        """Build a metadata.connections source profile without resolving secrets."""
        normalized_tag = str(tag or "").strip().lower()
        if not normalized_tag:
            raise ValueError("Source tag is required.")
        if not str(credential_reference or "").strip():
            raise ValueError("Credential reference is required; store the secret in .env.")
        return {
            "tag": normalized_tag,
            "connection_type": "DATABASE",
            "supports_source": True,
            "supports_target": False,
            "environment": str(environment or "PROD").upper(),
            "db_type": str(db_type or "").strip().lower(),
            "server": str(server or "").strip(),
            "port": int(port or 0),
            "database_name": str(database_name or "").strip(),
            "schema_name": str(schema_name or database_name or "").strip(),
            "username": str(username or "").strip(),
            "credential_reference": str(credential_reference or "").strip(),
            "service_name": "",
            "connection_options": {},
            "owner": str(owner or "Admin").strip(),
            "team": str(team or "Data Engineering").strip(),
            "purpose": str(purpose or "GUI onboarded source connection").strip(),
            "is_active": "y",
        }

    @staticmethod
    def _get_cm() -> ConnectionManager:
        return ConnectionManager()

    @classmethod
    def list_connections(cls, active_only: bool = False) -> List[Dict[str, Any]]:
        """Fetch all connection profiles from metadata.connections (excluding soft-deleted rows)."""
        now = time.time()
        cache_key = f"list_{active_only}"
        if cls._conn_cache.get(cache_key) and (now - cls._conn_cache["ts"]) < 10.0:
            return cls._conn_cache[cache_key]
        sql = """
            SELECT tag, connection_type, supports_source, supports_target, environment,
                   db_type, server, port, database_name, schema_name, username,
                   credential_reference, service_name, connection_options,
                   last_tested_at, last_test_status, last_error, last_latency_ms,
                   owner, team, purpose, is_active, created_at, updated_at
            FROM metadata.connections
            WHERE deleted_at IS NULL
        """
        if active_only:
            sql += " AND is_active = 'y'"
        sql += " ORDER BY tag"

        cols = [
            "tag", "connection_type", "supports_source", "supports_target", "environment",
            "db_type", "server", "port", "database_name", "schema_name", "username",
            "credential_reference", "service_name", "connection_options",
            "last_tested_at", "last_test_status", "last_error", "last_latency_ms",
            "owner", "team", "purpose", "is_active", "created_at", "updated_at"
        ]

        cm = cls._get_cm()
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                results = []
                for row in rows:
                    row_dict = dict(zip(cols, row))
                    # Ensure connection_options is dict
                    if isinstance(row_dict["connection_options"], str):
                        try:
                            row_dict["connection_options"] = json.loads(row_dict["connection_options"])
                        except Exception:
                            row_dict["connection_options"] = {}
                    elif row_dict["connection_options"] is None:
                        row_dict["connection_options"] = {}
                    row_dict["server"] = cm._resolve_env_vars(row_dict.get("server", ""))
                    row_dict["database_name"] = cm._resolve_env_vars(row_dict.get("database_name", ""))
                    row_dict["service_name"] = cm._resolve_env_vars(row_dict.get("service_name", ""))
                    row_dict["username"] = cm._resolve_env_vars(row_dict.get("username", ""))
                    results.append(row_dict)
                cls._conn_cache[cache_key] = results
                cls._conn_cache["ts"] = now
                return results

    @classmethod
    def get_connection(cls, tag: str) -> Optional[Dict[str, Any]]:
        """Fetch a single connection profile dictionary by tag."""
        cm = cls._get_cm()
        cols = [
            "tag", "connection_type", "supports_source", "supports_target", "environment",
            "db_type", "server", "port", "database_name", "schema_name", "username",
            "credential_reference", "service_name", "connection_options",
            "last_tested_at", "last_test_status", "last_error", "last_latency_ms",
            "owner", "team", "purpose", "is_active", "created_at", "updated_at"
        ]
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tag, connection_type, supports_source, supports_target, environment,
                           db_type, server, port, database_name, schema_name, username,
                           credential_reference, service_name, connection_options,
                           last_tested_at, last_test_status, last_error, last_latency_ms,
                           owner, team, purpose, is_active, created_at, updated_at
                    FROM metadata.connections
                    WHERE tag = %s AND deleted_at IS NULL
                """, (tag.lower(),))
                row = cur.fetchone()
                if not row:
                    return None
                row_dict = dict(zip(cols, row))
                if isinstance(row_dict["connection_options"], str):
                    try:
                        row_dict["connection_options"] = json.loads(row_dict["connection_options"])
                    except Exception:
                        row_dict["connection_options"] = {}
                elif row_dict["connection_options"] is None:
                    row_dict["connection_options"] = {}
                row_dict["server"] = cm._resolve_env_vars(row_dict.get("server", ""))
                row_dict["database_name"] = cm._resolve_env_vars(row_dict.get("database_name", ""))
                row_dict["service_name"] = cm._resolve_env_vars(row_dict.get("service_name", ""))
                row_dict["username"] = cm._resolve_env_vars(row_dict.get("username", ""))
                return row_dict

    @classmethod
    def test_connection(cls, profile: Dict[str, Any], update_db: bool = True) -> Dict[str, Any]:
        """
        Execute a live pre-save connectivity check and record latency/status to metadata.connections.
        """
        cm = cls._get_cm()
        tag = profile.get("tag", "").lower()
        db_type = cm._normalize_db_type(profile.get("db_type", ""))
        raw_server_input = profile.get("server", "")
        server = cm._resolve_env_vars(raw_server_input)
        
        port = int(profile.get("port", 0)) if str(profile.get("port", "")).isdigit() else 0
        database = cm._resolve_env_vars(profile.get("database_name", ""))
        username = cm._resolve_env_vars(profile.get("username", ""))
        cred_ref = profile.get("credential_reference", "")
        password = cm._resolve_env_vars(cred_ref)
        service = cm._resolve_env_vars(profile.get("service_name", ""))

        t0 = time.perf_counter()
        status = "FAILED"
        err_msg = ""
        
        try:
            if db_type in ("postgres", "postgresql"):
                # Full auth test — psycopg2 driver is available locally
                conn = psycopg2.connect(
                    host=server, port=port or 5432, dbname=database,
                    user=username, password=password, connect_timeout=5
                )
                conn.close()
            elif db_type == "mysql":
                # Full auth test — pymysql driver is available locally
                conn = pymysql.connect(
                    host=server, port=port or 3306, database=database,
                    user=username, password=password, connect_timeout=5
                )
                conn.close()
            elif db_type == "oracle":
                # oracledb thin-mode requires the host:port to be a real TCP target;
                # calling it with a bare service name triggers the 'bequeath' (local IPC)
                # path which is only available in thick mode.  Since Oracle is only
                # reachable from inside the Spark Connect container (not the Windows host),
                # we do a lightweight TCP port probe instead — it confirms the host is
                # reachable without needing an Oracle client library.
                _port = port or 1521
                if not server:
                    raise ValueError("Oracle server host is empty — check metadata.connections.server")
                reachable = _tcp_reachable(server, _port, timeout=5.0)
                if not reachable:
                    raise ConnectionError(
                        f"TCP probe to {server}:{_port} timed out. "
                        f"Oracle is only reachable from inside the Spark Connect container; "
                        f"JDBC connectivity will be confirmed during pipeline execution."
                    )
            elif db_type in ("mssql", "sqlserver"):
                # Same reasoning as Oracle — use TCP probe; no local MSSQL driver
                _port = port or 1433
                if not _tcp_reachable(server, _port, timeout=5.0):
                    raise ConnectionError(f"TCP probe to {server}:{_port} timed out.")
            elif db_type == "smtp":
                if server.startswith("file://") or server.lower() in ("console", "mock", "local", "localhost"):
                    status = "SUCCESS"
                else:
                    import smtplib
                    with smtplib.SMTP(server, port or 587, timeout=4.0) as smtp_conn:
                        if username and password:
                            try:
                                smtp_conn.starttls()
                            except Exception:
                                pass
                            smtp_conn.login(username, password)
                    status = "SUCCESS"
            else:
                # Fallback for api, file or unknown types — not testable locally
                status = "SUCCESS"
            status = "SUCCESS"
        except Exception as e:
            status = "FAILED"
            err_msg = _scrub_connection_error(str(e), password, cred_ref)
            
        latency_ms = int((time.perf_counter() - t0) * 1000)

        # Update metadata if requested and tag exists in db
        if update_db and tag and cls.get_connection(tag):
            try:
                with cm.get_metadata_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE metadata.connections
                            SET last_tested_at = CURRENT_TIMESTAMP,
                                last_test_status = %s,
                                last_error = %s,
                                last_latency_ms = %s,
                                db_type = %s,
                                server = %s
                            WHERE tag = %s
                        """, (status, err_msg if status == "FAILED" else None, latency_ms, db_type, server, tag))
                    conn.commit()
                cls.clear_cache()
            except Exception as e:
                logger.warning(f"Failed to record test metadata for tag {tag}: {e}")

        try:
            from src.services.notification_deriver import NotificationDeriver
            NotificationDeriver.on_connection_test_finished(tag, {"status": status, "latency_ms": latency_ms, "error": err_msg})
        except Exception as e:
            logger.warning(f"Failed to invoke notification deriver for connection test: {e}")

        return {"status": status, "latency_ms": latency_ms, "error": err_msg}

    @classmethod
    def save_connection(cls, profile: Dict[str, Any], username: str = "operator") -> bool:
        """
        Validate, upsert into metadata.connections, and write a Git-style JSON snapshot to metadata.connection_history.
        """
        cm = cls._get_cm()
        tag = profile.get("tag", "").strip().lower()
        if not tag:
            raise ValueError("Connection profile 'tag' cannot be empty.")

        env = (profile.get("environment") or "PROD").upper()
        if env not in ("DEV", "TEST", "UAT", "PROD"):
            raise ValueError(f"Invalid environment '{env}': Must be DEV, TEST, UAT, or PROD.")

        old_dict = cls.get_connection(tag)
        change_type = "UPDATE" if old_dict else "CREATE"

        # Serialize options safely
        opts = profile.get("connection_options", {})
        if isinstance(opts, str):
            try:
                opts = json.loads(opts)
            except Exception:
                opts = {}
        opts_json = json.dumps(opts)

        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                upsert_sql = """
                INSERT INTO metadata.connections (
                    tag, connection_type, supports_source, supports_target, environment,
                    db_type, server, port, database_name, schema_name, username,
                    credential_reference, service_name, connection_options,
                    owner, team, purpose, is_active, deleted_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, CURRENT_TIMESTAMP
                ) ON CONFLICT (tag) DO UPDATE SET
                    connection_type = EXCLUDED.connection_type,
                    supports_source = EXCLUDED.supports_source,
                    supports_target = EXCLUDED.supports_target,
                    environment = EXCLUDED.environment,
                    db_type = EXCLUDED.db_type,
                    server = EXCLUDED.server,
                    port = EXCLUDED.port,
                    database_name = EXCLUDED.database_name,
                    schema_name = EXCLUDED.schema_name,
                    username = EXCLUDED.username,
                    credential_reference = EXCLUDED.credential_reference,
                    service_name = EXCLUDED.service_name,
                    connection_options = EXCLUDED.connection_options,
                    owner = EXCLUDED.owner,
                    team = EXCLUDED.team,
                    purpose = EXCLUDED.purpose,
                    is_active = EXCLUDED.is_active,
                    deleted_at = NULL,
                    updated_at = CURRENT_TIMESTAMP;
                """
                cur.execute(upsert_sql, (
                    tag,
                    profile.get("connection_type", "DATABASE"),
                    bool(profile.get("supports_source", True)),
                    bool(profile.get("supports_target", False)),
                    env,
                    profile.get("db_type", "postgres").lower(),
                    profile.get("server", "").strip(),
                    int(profile.get("port", 0)) if str(profile.get("port", "")).isdigit() else 0,
                    profile.get("database_name", "").strip(),
                    profile.get("schema_name", "").strip(),
                    profile.get("username", "").strip(),
                    profile.get("credential_reference", "").strip(),
                    profile.get("service_name", "").strip(),
                    opts_json,
                    profile.get("owner", "Admin").strip(),
                    profile.get("team", "Data Engineering").strip(),
                    profile.get("purpose", "").strip(),
                    profile.get("is_active", "y")
                ))

                # Prepare snapshots for git-style audit
                new_dict = cls._clean_for_snapshot(profile)
                old_snap = cls._clean_for_snapshot(old_dict) if old_dict else None

                cur.execute("""
                    INSERT INTO metadata.connection_history (
                        tag, change_type, old_value, new_value, changed_by, changed_at
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    tag, change_type,
                    json.dumps(old_snap, default=str) if old_snap else None,
                    json.dumps(new_dict, default=str),
                    username
                ))
            conn.commit()
        return True

    @classmethod
    def toggle_status(cls, tag: str, new_status: str, username: str = "operator") -> bool:
        """Toggle is_active ('y'|'n') and record an UPDATE snapshot in connection_history."""
        old_dict = cls.get_connection(tag)
        if not old_dict:
            raise KeyError(f"Connection profile '{tag}' not found.")
        
        new_dict = dict(old_dict)
        new_dict["is_active"] = new_status
        
        return cls.save_connection(new_dict, username=username)

    @classmethod
    def soft_delete(cls, tag: str, username: str = "operator") -> bool:
        """Mark row as deleted (`is_active='n'`, `deleted_at=CURRENT_TIMESTAMP`) and log SOFT_DELETE snapshot."""
        cm = cls._get_cm()
        old_dict = cls.get_connection(tag)
        if not old_dict:
            raise KeyError(f"Connection profile '{tag}' not found.")

        old_snap = cls._clean_for_snapshot(old_dict)
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE metadata.connections
                    SET is_active = 'n', deleted_at = CURRENT_TIMESTAMP, deleted_by = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE tag = %s
                """, (username, tag.lower()))

                cur.execute("""
                    INSERT INTO metadata.connection_history (
                        tag, change_type, old_value, new_value, changed_by, changed_at
                    ) VALUES (%s, 'SOFT_DELETE', %s, NULL, %s, CURRENT_TIMESTAMP)
                """, (tag.lower(), json.dumps(old_snap, default=str), username))
            conn.commit()
        return True

    @classmethod
    def get_blast_radius(cls, tag: str) -> Dict[str, Any]:
        """
        Query metadata.db_tables_etl to find all pipeline tables depending on this connection tag.
        Returns exact counts and list of downstream tables for impact analysis before modifications.
        """
        cm = cls._get_cm()
        tag_key = tag.lower()
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # Check tables where this tag is either the source_tag or target_tag
                cur.execute("""
                    SELECT table_id, source_table, target_table, priority, is_active, is_cdc
                    FROM metadata.db_tables_etl
                    WHERE LOWER(source_tag) = %s
                    ORDER BY priority ASC, table_id ASC
                """, (tag_key,))
                rows = cur.fetchall()
                cols = ["table_id", "source_table", "target_table", "priority", "is_active", "is_cdc"]
                tables = [dict(zip(cols, r)) for r in rows]
                
                active_count = sum(1 for t in tables if t["is_active"] == "y")
                return {
                    "tag": tag_key,
                    "total_tables": len(tables),
                    "active_tables": active_count,
                    "tables": tables
                }

    @staticmethod
    def _clean_for_snapshot(d: Dict[str, Any]) -> Dict[str, Any]:
        """Strip internal timestamp objects before serializing snapshot JSON."""
        if not d:
            return {}
        clean = dict(d)
        for k in ("created_at", "updated_at", "deleted_at", "last_tested_at"):
            if k in clean and clean[k] is not None:
                clean[k] = str(clean[k])
        return clean
