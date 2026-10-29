# src/load/target_schema_inspector.py
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Tuple

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TargetIdentityCapability:
    """Evidence that a target can enforce a bounded-exception identity."""

    supported: bool
    identity_mode: str
    key_columns: Tuple[str, ...]
    exception_values: Tuple[int, ...]
    evidence: Dict[str, Any]

class TargetSchemaInspector(ABC):
    @abstractmethod
    def supports_unique_key(self, schema: str, table: str, business_key_columns: Tuple[str, ...], config) -> bool:
        """
        Does the target database enforce uniqueness compatible with this business key?
        Returns True if a compatible PK or UNIQUE constraint exists.
        """
        pass

    def certify_bounded_exception_key(
        self,
        schema: str,
        table: str,
        business_key_columns: Tuple[str, ...],
        exception_values: Tuple[int, ...],
        config,
        *,
        create: bool = False,
    ) -> TargetIdentityCapability:
        """Return target support for a partial uniqueness boundary.

        Adapters override this when the target engine has a safe equivalent
        (for example a filtered/partial unique index).  Unsupported engines
        fail closed before Gold DML; the worker does not guess at dialect SQL.
        """
        return TargetIdentityCapability(
            supported=False,
            identity_mode="BOUNDED_EXCEPTION_KEY",
            key_columns=tuple(business_key_columns),
            exception_values=tuple(exception_values),
            evidence={"reason": "target adapter does not support bounded exception identity"},
        )

class PostgreSQLSchemaInspector(TargetSchemaInspector):
    def supports_unique_key(self, schema: str, table: str, business_key_columns: Tuple[str, ...], config) -> bool:
        from src.utils.db_utils import _parse_postgres_connection
        import psycopg2 as pg2
        
        # Sort for order-independent comparison if needed, but usually exact match is safer.
        # Since business_key_columns is a tuple, let's normalize to lowercase set for membership comparison.
        required_cols = {c.lower() for c in business_key_columns}
        if not required_cols:
            return False

        resolved_host, port, database = _parse_postgres_connection(config)
        schema_name = schema or 'public'
        
        query = """
        SELECT c.conname, c.contype, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE n.nspname = %s AND t.relname = %s
          AND c.contype IN ('p', 'u');
        """
        
        username = getattr(config, "username", None) or getattr(config, "user", None)
        if not username:
            raise ValueError("Target connection configuration has no username")

        conn = pg2.connect(
            host=resolved_host, port=port, dbname=database,
            user=username, password=config.password
        )
        try:
            with conn.cursor() as cur:
                cur.execute(query, (schema_name, table))
                rows = cur.fetchall()
                
                for conname, contype, condef in rows:
                    # condef looks like: PRIMARY KEY (id) or UNIQUE (mrcode, adm_no)
                    # Extract the columns between parentheses
                    if '(' in condef and ')' in condef:
                        cols_part = condef[condef.find('(')+1 : condef.find(')')]
                        cols = {c.strip().strip('"').lower() for c in cols_part.split(',')}
                        if cols == required_cols:
                            return True
            return False
        finally:
            conn.close()

    def certify_bounded_exception_key(
        self,
        schema: str,
        table: str,
        business_key_columns: Tuple[str, ...],
        exception_values: Tuple[int, ...],
        config,
        *,
        create: bool = False,
    ) -> TargetIdentityCapability:
        """Certify/create a PostgreSQL partial unique index for the normal lane."""
        from src.contracts.conditional_key import Int64ExceptionCodec
        from src.utils.db_utils import _parse_postgres_connection, quote_column_name, quote_table_name
        import psycopg2 as pg2

        keys = tuple(str(column).strip() for column in business_key_columns if str(column).strip())
        values = Int64ExceptionCodec.normalize(exception_values)
        if len(keys) != 1 or not values:
            return TargetIdentityCapability(False, "BOUNDED_EXCEPTION_KEY", keys, values, {"reason": "one key column and exceptions are required"})

        resolved_host, port, database = _parse_postgres_connection(config)
        username = getattr(config, "username", None) or getattr(config, "user", None)
        if not username:
            raise ValueError("Target connection configuration has no username")
        schema_name = schema or "public"
        material = f"{schema_name}.{table}|{keys[0]}|{','.join(str(value) for value in values)}".encode("utf-8")
        index_name = f"uq_etl_cond_{hashlib.sha256(material).hexdigest()[:16]}"
        q_target = quote_table_name(f"{schema_name}.{table}", "postgres")
        q_key = quote_column_name(keys[0], "postgres")
        q_index = quote_column_name(index_name, "postgres")
        literals = ", ".join(str(value) for value in values)
        conn = pg2.connect(host=resolved_host, port=port, dbname=database, user=username, password=config.password)
        try:
            with conn.cursor() as cur:
                # A bounded identity cannot coexist with an ordinary UNIQUE
                # or PRIMARY KEY on the same column: the exception lane is
                # deliberately allowed to contain duplicate key values.
                cur.execute(
                    """
                    SELECT pg_get_constraintdef(c.oid)
                    FROM pg_constraint c
                    JOIN pg_namespace n ON n.oid = c.connamespace
                    JOIN pg_class t ON t.oid = c.conrelid
                    WHERE n.nspname = %s AND t.relname = %s
                      AND c.contype IN ('p', 'u')
                    """,
                    (schema_name, table),
                )
                ordinary_constraints = cur.fetchall()
                required = keys[0].lower()
                has_ordinary_key = False
                for row in ordinary_constraints:
                    definition = str(row[0] if not isinstance(row, dict) else row.get("pg_get_constraintdef", ""))
                    if "(" not in definition or ")" not in definition:
                        continue
                    columns = tuple(
                        part.strip().strip('"').lower()
                        for part in definition.split("(", 1)[1].split(")", 1)[0].split(",")
                    )
                    if columns == (required,):
                        has_ordinary_key = True
                        break
                if has_ordinary_key:
                    return TargetIdentityCapability(
                        False,
                        "BOUNDED_EXCEPTION_KEY",
                        keys,
                        values,
                        {"reason": "ordinary target uniqueness conflicts with exception duplicates", "index_name": index_name},
                    )
                if create:
                    cur.execute(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS {q_index} ON {q_target} ({q_key}) "
                        f"WHERE {q_key} NOT IN ({literals})"
                    )
                cur.execute(
                    """
                    SELECT 1
                    FROM pg_index i
                    JOIN pg_class idx ON idx.oid = i.indexrelid
                    JOIN pg_class tbl ON tbl.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = tbl.relnamespace
                    WHERE n.nspname = %s AND tbl.relname = %s AND idx.relname = %s AND i.indisunique
                    """,
                    (schema_name, table, index_name),
                )
                supported = cur.fetchone() is not None
            conn.commit()
            return TargetIdentityCapability(
                supported=supported,
                identity_mode="BOUNDED_EXCEPTION_KEY",
                key_columns=keys,
                exception_values=values,
                evidence={"index_name": index_name, "engine": "postgres", "partial": True},
            )
        finally:
            conn.close()

PostgresSchemaInspector = PostgreSQLSchemaInspector


class MySQLSchemaInspector(TargetSchemaInspector):
    def supports_unique_key(self, schema: str, table: str, business_key_columns: Tuple[str, ...], config) -> bool:
        """Inspect MySQL target table for a compatible primary key or unique index."""
        import pymysql
        from collections.abc import Mapping
        required_cols = {c.lower() for c in business_key_columns}
        if not required_cols:
            return False

        from src.utils.db_utils import _parse_mysql_connection
        resolved_host, port, database = _parse_mysql_connection(config)

        if isinstance(config, Mapping):
            username = config.get("username") or config.get("user")
            password = config.get("password_resolved") or config.get("password")
        else:
            username = getattr(config, "username", None) or getattr(config, "user", None)
            password = getattr(config, "password_resolved", None) or getattr(config, "password", None)

        schema_name = schema or database

        query = """
        SELECT INDEX_NAME, COLUMN_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND NON_UNIQUE = 0
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """

        conn = pymysql.connect(
            host=resolved_host,
            port=port,
            database=database,
            user=username,
            password=password,
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(query, (schema_name, table))
                rows = cur.fetchall()
                index_cols: Dict[str, set] = {}
                for index_name, col_name in rows:
                    index_cols.setdefault(index_name, set()).add(col_name.lower())
                for cols in index_cols.values():
                    if cols == required_cols:
                        return True
            return False
        finally:
            conn.close()


class GenericSchemaInspector(TargetSchemaInspector):
    def supports_unique_key(self, schema: str, table: str, business_key_columns: Tuple[str, ...], config) -> bool:
        db_type = getattr(config, "db_type", None) or (config.get("db_type") if isinstance(config, dict) else "unknown")
        raise NotImplementedError(
            f"Constraint inspection is not implemented for {db_type}."
        )


def get_schema_inspector(config) -> TargetSchemaInspector:
    from collections.abc import Mapping
    driver = (
        getattr(config, "driver", "")
        if hasattr(config, "driver")
        else str(config.get("driver", "") if isinstance(config, Mapping) else "")
    ).lower()
    db_type = (
        getattr(config, "db_type", "")
        if hasattr(config, "db_type")
        else str(config.get("db_type", "") if isinstance(config, Mapping) else "")
    ).lower()

    if "postgresql" in driver or db_type in ("postgres", "postgresql", "postgressql"):
        return PostgreSQLSchemaInspector()
    if "mysql" in driver or db_type == "mysql":
        return MySQLSchemaInspector()
    return GenericSchemaInspector()

