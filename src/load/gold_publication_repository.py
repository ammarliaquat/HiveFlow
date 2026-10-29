# src/load/gold_publication_repository.py
"""
GoldPublicationRepository — single authority for reading and writing
metadata.gold_publication_manifest rows.

Design invariants:
- initialize_manifest() is the only path for creating a manifest row.
  It is idempotent via ON CONFLICT DO NOTHING; a second call returns
  the existing row. This satisfies at-least-once delivery with
  idempotent-storage convergence.
- record_target_evidence() is called exactly once after the target
  commit succeeds. It fails closed: raises if 0 rows updated.
- transition_status() is a fenced CAS; it fails closed if the fence
  was lost or the row is already terminal.
- All methods raise on failure — no silent swallowing.

Column note: migration 045 adds expected-evidence columns.
Until 045 is applied those columns do not exist; pass None for them.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class PublicationRecord:
    """
    Full snapshot of one metadata.gold_publication_manifest row.
    Optional fields correspond to columns added by migrations 041 and 045.
    """
    # Core identity
    publication_id: uuid.UUID
    run_id: str
    table_id: int
    attempt_number: int
    claim_id: Optional[int]
    fencing_token_digest: Optional[str]
    fencing_epoch: Optional[int]
    owner_instance_id: Optional[str]
    worker_attempt_number: Optional[int]

    # Publication target
    staging_object_identity: Optional[str]
    target_object_identity: Optional[str]
    policy_version_id: Optional[str]
    reconciliation_id: Optional[str]
    manifest_id: Optional[str]
    publication_strategy: Optional[str]

    # Row counts
    published_row_count: Optional[int]

    # Status
    status: str
    state_revision: Optional[int]
    published_at: Optional[datetime]
    created_at: Optional[datetime]

    # Evidence (post-041 columns, present in current DB)
    staging_evidence_fingerprint: Optional[str]
    target_evidence_fingerprint: Optional[str]
    target_commit_observed_at: Optional[datetime]
    target_schema_fingerprint: Optional[str]
    target_null_key_count: Optional[int]
    target_duplicate_key_count: Optional[int]
    evidence_codec_version: Optional[str]
    fencing_revision: Optional[int]
    last_recovery_at: Optional[datetime]
    cleanup_status: Optional[str]
    cleanup_error: Optional[str]
    publication_fingerprint: Optional[str]


class GoldPublicationRepository:
    """
    Repository for metadata.gold_publication_manifest.
    All methods raise on failure — no warn-and-continue paths.
    """

    def __init__(self, conn_manager) -> None:
        self._conn_manager = conn_manager

    # ──────────────────────────────────────────────────────────────────────
    # Initialize (submission / first-attempt path)
    # ──────────────────────────────────────────────────────────────────────

    def initialize_manifest(
        self,
        *,
        run_id: str,
        table_id: int,
        attempt_number: int,
        claim_id: int,
        fencing_token_digest: str,
        fencing_epoch: int,
        owner_instance_id: str,
        worker_attempt_number: int,
        staging_object_identity: str,
        target_object_identity: str,
        policy_version_id: str,
        reconciliation_id: str,
        manifest_id: str,
        publication_strategy: str,
    ) -> PublicationRecord:
        """
        Inserts a new PENDING manifest row.
        ON CONFLICT (run_id, table_id, attempt_number) DO NOTHING so that
        retries are safe; returns the existing row on conflict.

        Fails closed: raises RuntimeError if the INSERT and SELECT both
        return nothing (should not happen under normal DB connectivity).
        """
        publication_id = uuid.uuid4()
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO metadata.gold_publication_manifest (
                        publication_id, run_id, table_id, attempt_number,
                        claim_id, fencing_token_digest, fencing_epoch,
                        owner_instance_id, worker_attempt_number,
                        staging_object_identity, target_object_identity,
                        policy_version_id, reconciliation_id, manifest_id,
                        publication_strategy, status, state_revision, created_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, 'PENDING', 1, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (run_id, table_id, attempt_number) DO NOTHING
                    RETURNING publication_id
                    """,
                    [
                        str(publication_id), run_id, table_id, attempt_number,
                        claim_id, fencing_token_digest, fencing_epoch,
                        owner_instance_id, worker_attempt_number,
                        staging_object_identity, target_object_identity,
                        str(policy_version_id), str(reconciliation_id), str(manifest_id),
                        publication_strategy,
                    ],
                )
                inserted = cur.fetchone()
                if inserted:
                    actual_pub_id = uuid.UUID(str(inserted[0]))
                else:
                    # Conflict — row already exists; retrieve it
                    cur.execute(
                        """
                        SELECT publication_id FROM metadata.gold_publication_manifest
                        WHERE run_id = %s AND table_id = %s AND attempt_number = %s
                        """,
                        [run_id, table_id, attempt_number],
                    )
                    existing = cur.fetchone()
                    if not existing:
                        raise RuntimeError(
                            "GoldPublicationRepository.initialize_manifest: "
                            "INSERT conflicted but existing row not found — "
                            f"run_id={run_id} table_id={table_id} attempt={attempt_number}"
                        )
                    actual_pub_id = uuid.UUID(str(existing[0]))
            conn.commit()

        return self.fetch_publication_attempt(actual_pub_id)

    # ──────────────────────────────────────────────────────────────────────
    # Record observed target evidence (post-commit path)
    # ──────────────────────────────────────────────────────────────────────

    def record_target_evidence(
        self,
        *,
        publication_id: uuid.UUID,
        fencing_token_digest: str,
        target_evidence_fingerprint: str,
        target_schema_fingerprint: Optional[str],
        target_null_key_count: Optional[int],
        target_duplicate_key_count: Optional[int],
        evidence_codec_version: Optional[str],
        published_row_count: int,
    ) -> None:
        """
        Writes observed target evidence and row count after the target commit.
        Fenced by fencing_token_digest; fails closed if fence is lost.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.gold_publication_manifest
                    SET target_evidence_fingerprint  = %s,
                        target_schema_fingerprint    = %s,
                        target_null_key_count        = %s,
                        target_duplicate_key_count   = %s,
                        evidence_codec_version       = %s,
                        published_row_count          = %s,
                        target_commit_observed_at    = CURRENT_TIMESTAMP,
                        state_revision               = COALESCE(state_revision, 0) + 1
                    WHERE publication_id        = %s
                      AND fencing_token_digest  = %s
                      AND status NOT IN ('PUBLISHED', 'ROLLED_BACK', 'FAILED', 'RECOVERY_FAILED')
                    RETURNING publication_id
                    """,
                    [
                        target_evidence_fingerprint,
                        target_schema_fingerprint,
                        target_null_key_count,
                        target_duplicate_key_count,
                        evidence_codec_version,
                        published_row_count,
                        str(publication_id),
                        fencing_token_digest,
                    ],
                )
                updated = cur.fetchone()
                if not updated:
                    raise RuntimeError(
                        "GoldPublicationRepository.record_target_evidence: "
                        "0 rows updated — fence evicted or manifest already terminal. "
                        f"publication_id={publication_id}"
                    )
            conn.commit()

    # ──────────────────────────────────────────────────────────────────────
    # Status transition (fenced CAS)
    # ──────────────────────────────────────────────────────────────────────

    def transition_status(
        self,
        *,
        publication_id: uuid.UUID,
        from_status: str,
        to_status: str,
        fencing_token_digest: str,
    ) -> None:
        """
        Fenced CAS status transition. Fails closed if 0 rows updated.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.gold_publication_manifest
                    SET status         = %s,
                        published_at   = CASE WHEN %s = 'PUBLISHED' THEN CURRENT_TIMESTAMP ELSE published_at END,
                        state_revision = COALESCE(state_revision, 0) + 1
                    WHERE publication_id       = %s
                      AND status               = %s
                      AND fencing_token_digest = %s
                    RETURNING publication_id
                    """,
                    [to_status, to_status, str(publication_id), from_status, fencing_token_digest],
                )
                updated = cur.fetchone()
                if not updated:
                    raise RuntimeError(
                        "GoldPublicationRepository.transition_status: "
                        f"CAS {from_status}→{to_status} failed for "
                        f"publication_id={publication_id} — fence evicted or status mismatch"
                    )
            conn.commit()

    # ──────────────────────────────────────────────────────────────────────
    # Read
    # ──────────────────────────────────────────────────────────────────────

    def fetch_publication_attempt(
        self,
        publication_id: uuid.UUID,
    ) -> Optional[PublicationRecord]:
        """
        Fetches all columns for a single publication attempt.
        Returns None if not found.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        publication_id, run_id, table_id, attempt_number,
                        claim_id, fencing_token_digest, fencing_epoch,
                        owner_instance_id, worker_attempt_number,
                        staging_object_identity, target_object_identity,
                        policy_version_id, reconciliation_id, manifest_id,
                        publication_strategy, published_row_count,
                        status, state_revision, published_at, created_at,
                        staging_evidence_fingerprint, target_evidence_fingerprint,
                        target_commit_observed_at, target_schema_fingerprint,
                        target_null_key_count, target_duplicate_key_count,
                        evidence_codec_version, fencing_revision,
                        last_recovery_at, cleanup_status, cleanup_error,
                        publication_fingerprint
                    FROM metadata.gold_publication_manifest
                    WHERE publication_id = %s
                    """,
                    [str(publication_id)],
                )
                row = cur.fetchone()

        if not row:
            return None

        return PublicationRecord(
            publication_id=uuid.UUID(str(row[0])),
            run_id=str(row[1]),
            table_id=int(row[2]),
            attempt_number=int(row[3]),
            claim_id=int(row[4]) if row[4] is not None else None,
            fencing_token_digest=row[5],
            fencing_epoch=int(row[6]) if row[6] is not None else None,
            owner_instance_id=row[7],
            worker_attempt_number=int(row[8]) if row[8] is not None else None,
            staging_object_identity=row[9],
            target_object_identity=row[10],
            policy_version_id=str(row[11]) if row[11] is not None else None,
            reconciliation_id=str(row[12]) if row[12] is not None else None,
            manifest_id=str(row[13]) if row[13] is not None else None,
            publication_strategy=row[14],
            published_row_count=int(row[15]) if row[15] is not None else None,
            status=str(row[16]),
            state_revision=int(row[17]) if row[17] is not None else None,
            published_at=row[18],
            created_at=row[19],
            staging_evidence_fingerprint=row[20],
            target_evidence_fingerprint=row[21],
            target_commit_observed_at=row[22],
            target_schema_fingerprint=row[23],
            target_null_key_count=int(row[24]) if row[24] is not None else None,
            target_duplicate_key_count=int(row[25]) if row[25] is not None else None,
            evidence_codec_version=row[26],
            fencing_revision=int(row[27]) if row[27] is not None else None,
            last_recovery_at=row[28],
            cleanup_status=row[29],
            cleanup_error=row[30],
            publication_fingerprint=row[31],
        )
