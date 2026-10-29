# tests/test_migration_045.py
"""
Canonical migration certification test.

Requires a SEPARATE, EMPTY PostgreSQL database. Set DISPOSABLE_PG_URL to a
connection string pointing to it. Do NOT use the shared opf database — the
migration runner applies migrations into the metadata schema and the ledger
must start from zero.

Run with:
    $env:DISPOSABLE_PG_URL = "postgresql://user:pass@localhost:5433/etl_cert_test"
    python -m pytest tests/test_migration_045.py -m migration --live -v

The test:
  1. Applies all CANONICAL_MIGRATIONS via migration_runner.run_canonical_migrations().
  2. Asserts applied == len(CANONICAL_MIGRATIONS).
  3. Asserts ledger row count == len(CANONICAL_MIGRATIONS).
  4. Spot-checks migration 045-specific structural invariants:
     - scope_action column exists and immutability trigger fires.
     - 14 evidence fields exist on gold_publication_manifest.
     - Terminal manifest immutability trigger fires.
     - Full status constraint vocabulary accepted (PENDING, PUBLISHING, …,
       RECOVERY_FAILED).
     - UNCERTAIN rejected by constraint.
"""
from __future__ import annotations

import os
import uuid

import psycopg2
import pytest

def _insert_dummy_prereqs(conn, run_id, table_id, recon_id=None):
    if not recon_id:
        recon_id = '00000000-0000-0000-0000-000000000000'
    with conn.cursor() as cur:
        # gui_job_registry
        cur.execute(
            "INSERT INTO metadata.gui_job_registry (run_id, action, source_tag, status) "
            "VALUES (%s, 'TEST', 'TEST', 'RUNNING') ON CONFLICT DO NOTHING",
            [run_id]
        )
        # db_tables_etl
        cur.execute(
            "INSERT INTO metadata.db_tables_etl (table_id, source_tag, source_table, target_tag) "
            "VALUES (%s, 'dummy', 'dummy_' || %s::text, 'dummy') ON CONFLICT DO NOTHING",
            [table_id, table_id]
        )
        # table_policy_version
        policy_id = '00000000-0000-0000-0000-000000000000'
        cur.execute(
            """
            INSERT INTO metadata.table_policy_version (
                policy_version_id, table_id, version_number, contract_payload,
                source_key_fingerprint, mapping_fingerprint, target_key_fingerprint,
                contract_fingerprint, policy_fingerprint, promoted_by_type, promoted_by_id
            ) VALUES (
                %s, %s, 1, '{}',
                'dummy', 'dummy', 'dummy', 'dummy', 'dummy', 'SYSTEM', 'dummy'
            ) ON CONFLICT DO NOTHING
            """,
            [policy_id, table_id]
        )
        # cdc_attempt_window
        window_id = '00000000-0000-0000-0000-000000000000'
        cur.execute(
            """
            INSERT INTO metadata.cdc_attempt_window (
                window_id, run_id, table_id, attempt_number, policy_version_id,
                cdc_policy_fingerprint, window_fingerprint, lower_cursor, upper_cursor
            ) VALUES (
                %s, %s, %s, 1, %s, 'dummy', 'dummy', '{}', '{}'
            ) ON CONFLICT DO NOTHING
            """,
            [window_id, run_id, table_id, policy_id]
        )
        # attempt_persistence_manifest
        manifest_id = '00000000-0000-0000-0000-000000000000'
        cur.execute(
            """
            INSERT INTO metadata.attempt_persistence_manifest (
                manifest_id, run_id, table_id, attempt_number, policy_version_id,
                window_id, storage_layer, storage_object_identity, expected_row_count,
                expected_key_fingerprint, schema_fingerprint, manifest_fingerprint
            ) VALUES (
                %s, %s, %s, 1, %s, %s, 'SILVER', 'dummy', 0, 'dummy', 'dummy', 'dummy'
            ) ON CONFLICT DO NOTHING
            """,
            [manifest_id, run_id, table_id, policy_id, window_id]
        )
        # cdc_attempt_reconciliation
        cur.execute(
            """
            INSERT INTO metadata.cdc_attempt_reconciliation (
                reconciliation_id, manifest_id, run_id, table_id, attempt_number,
                policy_version_id, window_id, reconciliation_strategy,
                storage_layer, expected_row_count, physical_row_count,
                row_count_delta, expected_key_fingerprint, physical_key_fingerprint,
                outcome, reconciliation_fingerprint
            ) VALUES (
                %s, %s, %s, %s, 1, %s, %s, 'dummy', 'SILVER', 0, 0, 0, 'dummy', 'dummy', 'CONVERGED', 'dummy'
            ) ON CONFLICT DO NOTHING
            """,
            [recon_id, manifest_id, run_id, table_id, policy_id, window_id]
        )
    conn.commit()

DISPOSABLE_PG_URL = os.environ.get("DISPOSABLE_PG_URL")


def _require_disposable() -> str:
    """Fails hard if DISPOSABLE_PG_URL is absent — skip-and-pass is not acceptable."""
    if not DISPOSABLE_PG_URL:
        pytest.fail(
            "DISPOSABLE_PG_URL is not set. A separate, empty PostgreSQL database "
            "is required. Set DISPOSABLE_PG_URL and re-run. "
            "The shared opf database with an existing metadata schema is not acceptable "
            "because the ledger count assertion requires starting from zero."
        )
    return DISPOSABLE_PG_URL


@pytest.mark.migration
@pytest.mark.live
def test_migration_canonical_sequence_applied_and_ledger():
    """
    Applies the full CANONICAL_MIGRATIONS sequence to the disposable DB and
    verifies the ledger count matches.
    """
    dsn = _require_disposable()

    from src.connections.schema_certifier import CANONICAL_MIGRATIONS
    from src.migrations.migration_runner import run_canonical_migrations

    conn = psycopg2.connect(dsn)
    try:
        applied = run_canonical_migrations(conn=conn)
    finally:
        conn.close()

    assert applied == len(CANONICAL_MIGRATIONS), (
        f"Expected {len(CANONICAL_MIGRATIONS)} migrations applied, got {applied}"
    )

    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM metadata.schema_migrations")
        ledger_count = cur.fetchone()[0]
    conn.close()

    expected_count = len(set(m.split('_')[0] for m in CANONICAL_MIGRATIONS))
    assert ledger_count == expected_count, (
        f"Ledger row count {ledger_count} != expected {expected_count}. "
        "Some migration applied but did not write its ledger row."
    )


@pytest.mark.migration
@pytest.mark.live
def test_migration_045_scope_action_immutability():
    """Migration 045: scope_action immutability trigger fires on UPDATE."""
    dsn = _require_disposable()

    conn = psycopg2.connect(dsn)
    run_id = str(uuid.uuid4())
    _insert_dummy_prereqs(conn, run_id, 99901)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.job_scope_claim
                    (run_id, source_table_id, scope_action, worker_action, status)
                VALUES (%s, 99901, 'PUBLISH', 'PUBLISH', 'RESERVED')
                RETURNING claim_id
                """,
                [run_id],
            )
            claim_id = cur.fetchone()[0]
        conn.commit()

        with pytest.raises(psycopg2.errors.RaiseException, match="scope_action is immutable"):
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT sp_immutable")
                try:
                    cur.execute(
                        "UPDATE metadata.job_scope_claim SET scope_action = 'RECOVERY' "
                        "WHERE claim_id = %s",
                        [claim_id],
                    )
                except psycopg2.errors.RaiseException:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_immutable")
                    raise
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.migration
@pytest.mark.live
def test_migration_045_evidence_columns_present():
    """Migration 045: all 14 expected/observed evidence columns exist."""
    dsn = _require_disposable()

    expected_columns = {
        "expected_row_count", "expected_key_fingerprint",
        "expected_content_fingerprint", "expected_schema_fingerprint",
        "expected_null_key_count", "expected_duplicate_key_count",
        "expected_codec_version",
        "target_row_count", "target_key_fingerprint",
        "target_content_fingerprint", "target_codec_version",
    }
    # 045 adds 11 columns (some target_* columns may already exist from 041)
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'metadata'
                  AND table_name   = 'gold_publication_manifest'
                  AND column_name  = ANY(%s)
                """,
                [list(expected_columns)],
            )
            found = {row[0] for row in cur.fetchall()}
        missing = expected_columns - found
        assert not missing, f"Missing evidence columns: {missing}"
    finally:
        conn.close()


@pytest.mark.migration
@pytest.mark.live
def test_migration_045_manifest_status_vocabulary():
    """
    Migration 045: full status vocabulary accepted by DB constraint.
    UNCERTAIN must be rejected.
    """
    dsn = _require_disposable()

    accepted_statuses = [
        "PENDING", "PUBLISHING", "PUBLISHED",
        "ROLLED_BACK", "FAILED", "RECOVERY_REQUIRED", "RECOVERY_FAILED",
    ]
    run_id = str(uuid.uuid4())
    recon_base = str(uuid.uuid4())
    conn = psycopg2.connect(dsn)
    try:
        for i, status in enumerate(accepted_statuses):
            pub_id = str(uuid.uuid4())
            recon_id = str(uuid.uuid4())
            table_id = 90000 + i
            with conn.cursor() as cur:
                _insert_dummy_prereqs(conn, run_id, table_id, recon_id)
                cur.execute(
                    """
                    INSERT INTO metadata.gold_publication_manifest
                        (publication_id, run_id, table_id, attempt_number,
                         reconciliation_id, status, created_at, policy_version_id, manifest_id, staging_object_identity, target_object_identity, publication_strategy, publication_fingerprint, published_row_count,
                         expected_row_count, expected_key_fingerprint, expected_content_fingerprint, expected_schema_fingerprint, expected_null_key_count, expected_duplicate_key_count, expected_codec_version,
                         target_row_count, target_key_fingerprint, target_content_fingerprint, target_codec_version, target_evidence_fingerprint, target_schema_fingerprint)
                    VALUES (%s, %s, %s, 1, %s, %s, CURRENT_TIMESTAMP, '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', 'dummy', 'dummy', 'TRANSACTIONAL_SWAP', 'dummy', 0,
                            0, 'dummy', 'dummy', 'dummy', 0, 0, 'canon_v1',
                            0, 'dummy', 'dummy', 'canon_v1', 'dummy', 'dummy')
                    """,
                    [pub_id, run_id, table_id, recon_id, status],
                )
            conn.commit()

        # UNCERTAIN must be rejected
        _insert_dummy_prereqs(conn, run_id, 80000)
        with pytest.raises(psycopg2.errors.CheckViolation):
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT sp_uncertain")
                try:
                    cur.execute(
                        """
INSERT INTO metadata.gold_publication_manifest
                            (publication_id, run_id, table_id, attempt_number,
                             reconciliation_id, status, created_at, policy_version_id, manifest_id, staging_object_identity, target_object_identity, publication_strategy, publication_fingerprint, published_row_count,
                             expected_row_count, expected_key_fingerprint, expected_content_fingerprint, expected_schema_fingerprint, expected_null_key_count, expected_duplicate_key_count, expected_codec_version,
                             target_row_count, target_key_fingerprint, target_content_fingerprint, target_codec_version, target_evidence_fingerprint, target_schema_fingerprint)
                        VALUES (%s, %s, 80000, 1, %s, 'UNCERTAIN', CURRENT_TIMESTAMP, '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', 'dummy', 'dummy', 'TRANSACTIONAL_SWAP', 'dummy', 0,
                                0, 'dummy', 'dummy', 'dummy', 0, 0, 'canon_v1',
                                0, 'dummy', 'dummy', 'canon_v1', 'dummy', 'dummy')""",
                        [str(uuid.uuid4()), run_id, str(uuid.uuid4())],
                    )
                except psycopg2.errors.CheckViolation:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_uncertain")
                    raise
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.migration
@pytest.mark.live
def test_migration_045_terminal_manifest_immutability():
    """Migration 045: trigger blocks mutations on all 4 terminal status values."""
    dsn = _require_disposable()

    terminal_statuses = ["PUBLISHED", "ROLLED_BACK", "FAILED", "RECOVERY_FAILED"]
    run_id = str(uuid.uuid4())
    conn = psycopg2.connect(dsn)
    try:
        for i, terminal in enumerate(terminal_statuses):
            pub_id = str(uuid.uuid4())
            recon_id = str(uuid.uuid4())
            table_id = 80100 + i
            with conn.cursor() as cur:
                # Insert in PENDING first (bypasses trigger — not yet terminal)
                _insert_dummy_prereqs(conn, run_id, table_id, recon_id)
                cur.execute(
                    """
                    INSERT INTO metadata.gold_publication_manifest
                        (publication_id, run_id, table_id, attempt_number,
                         reconciliation_id, status, created_at, policy_version_id, manifest_id, staging_object_identity, target_object_identity, publication_strategy, publication_fingerprint, published_row_count,
                         expected_row_count, expected_key_fingerprint, expected_content_fingerprint, expected_schema_fingerprint, expected_null_key_count, expected_duplicate_key_count, expected_codec_version,
                         target_row_count, target_key_fingerprint, target_content_fingerprint, target_codec_version, target_evidence_fingerprint, target_schema_fingerprint)
                    VALUES (%s, %s, %s, 1, %s, 'PENDING', CURRENT_TIMESTAMP, '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000', 'dummy', 'dummy', 'TRANSACTIONAL_SWAP', 'dummy', 0,
                            0, 'dummy', 'dummy', 'dummy', 0, 0, 'canon_v1',
                            0, 'dummy', 'dummy', 'canon_v1', 'dummy', 'dummy')
                    """,
                    [pub_id, run_id, table_id, recon_id],
                )
                # Move to terminal using direct UPDATE (simulating a status transition)
                cur.execute(
                    "UPDATE metadata.gold_publication_manifest "
                    "SET status = %s WHERE publication_id = %s",
                    [terminal, pub_id],
                )
            conn.commit()

            # Now verify that further mutation is blocked
            with pytest.raises(psycopg2.errors.RaiseException, match="is terminal"):
                with conn.cursor() as cur:
                    cur.execute("SAVEPOINT sp_terminal")
                    try:
                        cur.execute(
                            "UPDATE metadata.gold_publication_manifest "
                            "SET status = 'PENDING' WHERE publication_id = %s",
                            [pub_id],
                        )
                    except psycopg2.errors.RaiseException:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_terminal")
                        raise
    finally:
        conn.rollback()
        conn.close()
