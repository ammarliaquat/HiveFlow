# src/migrations/migration_runner.py
"""
Canonical migration runner.

Executes each migration file in CANONICAL_MIGRATIONS order and writes one
ledger row per successfully applied file to metadata.schema_migrations.

Used by:
  - Disposable certification test
  - Operator maintenance runbook

Never used by application startup. SchemaCertifier is read-only.

Ledger writes:
  After each successful migration, a ledger row is written with autocommit=True
  so it cannot be rolled back independently of the migration commit.
"""
from __future__ import annotations

import hashlib
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

_MIGRATIONS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "migrations")
)


def run_canonical_migrations(
    conn,
    migrations_dir: Optional[str] = None,
    stop_after: Optional[str] = None,
) -> int:
    """
    Applies each migration in CANONICAL_MIGRATIONS order to the target DB.

    The caller owns the DB connection. This function temporarily changes
    autocommit while applying migrations and always restores the original
    connection mode before returning or raising.
    """
    from src.connections.schema_certifier import CANONICAL_MIGRATIONS

    target_dir = migrations_dir or _MIGRATIONS_DIR
    original_autocommit = getattr(conn, "autocommit", False)

    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS metadata;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS metadata.schema_migrations (
                    version        VARCHAR(10)  PRIMARY KEY,
                    migration_name VARCHAR(256) NOT NULL,
                    checksum       VARCHAR(64)  NOT NULL,
                    applied_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)
        conn.autocommit = False

        applied = 0
        for filename in CANONICAL_MIGRATIONS:
            version = filename.split("_")[0]
            if stop_after and version > stop_after:
                break

            filepath = os.path.join(target_dir, filename)
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"Migration file missing: {filepath}")

            with open(filepath, "rb") as fh:
                raw = fh.read()
            normalised = raw.replace(b"\r\n", b"\n")
            checksum = hashlib.sha256(normalised).hexdigest()
            sql_text = normalised.decode("utf-8")

            log.info("Applying migration %s (%s)", version, filename)
            try:
                _apply_migration(conn, sql_text)
            except Exception as exc:
                raise RuntimeError(
                    f"Migration {version} ({filename}) failed: {exc}"
                ) from exc

            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.schema_migrations
                        (version, migration_name, checksum, applied_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (version) DO NOTHING
                    """,
                    [version, filename, checksum],
                )
            conn.autocommit = False
            applied += 1
            log.info("Migration %s applied and ledger row written.", version)

        return applied
    finally:
        conn.autocommit = original_autocommit


def _apply_migration(conn, sql_text: str) -> None:
    """Executes one migration file while preserving explicit SQL transaction scripts."""
    upper_sql = sql_text.upper()
    manages_own_transaction = "BEGIN;" in upper_sql and "COMMIT;" in upper_sql
    conn.autocommit = manages_own_transaction
    with conn.cursor() as cur:
        cur.execute(sql_text)
    if not manages_own_transaction:
        conn.commit()
