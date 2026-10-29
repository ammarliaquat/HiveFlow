# src/orchestrator/claim_manager.py
"""
ScopeClaimManager - single authority for all metadata.job_scope_claim mutations.

All INSERT/UPDATE on job_scope_claim must go through this class.
ExecutionService calls reserve_scope_claims(); RecoveryDispatcher calls
acquire_recovery_claim(). No other code embeds claim SQL.

Schema note (pre-migration-045):
  Column is named 'action'. After 045 applies it becomes 'scope_action'.
  The constant _ACTION_COLUMN below is the single place to update after the
  maintenance-window migration.

Constraint requirements enforced by the DB (from migrations 042-044):
  RESERVED:         fencing_token_digest IS NULL, publication_deadline IS NULL
  ACTIVE:           fencing_token_digest ~ '^[a-f0-9]{64}$', released_at IS NULL,
                    owner_instance_id NOT NULL, attempt_number > 0,
                    worker_attempt_number > 0, fencing_epoch > 0,
                    lease_expires_at NOT NULL
  PUBLISHING:       same as ACTIVE + publication_deadline NOT NULL
  RECOVERY_REQUIRED: fencing_token_digest regex, released_at IS NULL
  RELEASED/REVOKED/EXPIRED: released_at NOT NULL
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import List, Optional

from src.contracts.claim_models import ExecutionFence
from src.contracts.runtime_settings import RuntimeSettings

log = logging.getLogger(__name__)

# -- Single column-name constant: updated to scope_action after migration 045 --
_ACTION_COLUMN = "scope_action"   # renamed from 'action' by migration 045 step 3


def _hash_raw_token(raw_token: str) -> str:
    """Returns the SHA-256 hex digest of a raw fencing token.
    This is the only value persisted to the database."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _new_raw_token() -> str:
    """Generates a cryptographically secure random fencing token.
    64 lowercase hex chars. Never persisted - only its digest is stored."""
    return secrets.token_hex(32)


class ScopeClaimManager:
    """
    Authoritative owner of all job_scope_claim state transitions.
    Uses raw psycopg2 connections from ConnectionManager.
    """

    def __init__(self, conn_manager) -> None:
        self._conn_manager = conn_manager

    # -------------------------------------------------------------------------
    # Reserve (submission path - called by ExecutionService inside its tx)
    # -------------------------------------------------------------------------

    def reserve_scope_claims(
        self,
        conn,                         # existing psycopg2 connection (inside submission tx)
        run_id: str,
        table_ids: List[int],
        scope_action: str,
    ) -> None:
        """
        Inserts one RESERVED claim per table_id inside the caller's transaction.
        Inserts in sorted table_id order to prevent deadlocks with concurrent submissions.
        Does NOT commit - the caller owns the transaction boundary.
        """
        with conn.cursor() as cur:
            self.reserve_scope_claims_cursor(cur, run_id, table_ids, scope_action)
        log.debug(
            "ScopeClaimManager: reserved %d claim(s) for run_id=%s", len(table_ids), run_id
        )

    @staticmethod
    def reserve_scope_claims_cursor(
        cur,                          # existing open psycopg2 cursor (caller owns tx)
        run_id: str,
        table_ids: List[int],
        scope_action: str,
    ) -> None:
        """
        Inserts RESERVED claims using a caller-provided open cursor.
        Does NOT commit - the caller owns the transaction boundary.
        Use this when ExecutionService already has an open psycopg2 cursor.
        """
        for tid in sorted(table_ids):
            cur.execute(
                f"""
                INSERT INTO metadata.job_scope_claim
                    (run_id, source_table_id, {_ACTION_COLUMN}, status, created_at)
                VALUES (%s, %s, %s, 'RESERVED', CURRENT_TIMESTAMP)
                """,
                [run_id, tid, scope_action],
            )

    # -------------------------------------------------------------------------
    # Activate (worker path - transitions RESERVED -> ACTIVE)
    # -------------------------------------------------------------------------

    def activate_scope_claim(
        self,
        claim_id: int,
        owner_instance_id: str,
        runtime_settings: RuntimeSettings,
    ) -> ExecutionFence:
        """
        CAS: RESERVED -> ACTIVE.
        Returns an ExecutionFence. Raises RuntimeError if the CAS fails (stolen/missing claim).
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # Step 1: fetch current claim for token computation
                cur.execute(
                    """
                    SELECT claim_id, run_id, source_table_id, {ac},
                           COALESCE(attempt_number, 1),
                           COALESCE(fencing_epoch, 0)
                    FROM metadata.job_scope_claim
                    WHERE claim_id = %s AND status = 'RESERVED'
                    FOR UPDATE
                    """.format(ac=_ACTION_COLUMN),
                    [claim_id],
                )
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(
                        f"ScopeClaimManager: claim_id={claim_id} not found in RESERVED state - "
                        "possible concurrent acquisition"
                    )
                cid, run_id, table_id, scope_action, attempt_num, cur_epoch = row

                new_worker_attempt = attempt_num
                new_epoch = cur_epoch + 1
                raw_token = _new_raw_token()
                token_digest = _hash_raw_token(raw_token)
                expires_at = (
                    datetime.now(timezone.utc).timestamp()
                    + runtime_settings.dispatch_lease_timeout_sec
                )

                # Step 2: CAS update
                cur.execute(
                    """
                    UPDATE metadata.job_scope_claim
                    SET status                = 'ACTIVE',
                        fencing_token_digest  = %s,
                        fencing_epoch         = %s,
                        owner_instance_id     = %s,
                        attempt_number        = %s,
                        worker_attempt_number = %s,
                        lease_revision        = COALESCE(lease_revision, 0) + 1,
                        lease_expires_at      = to_timestamp(%s),
                        last_heartbeat        = CURRENT_TIMESTAMP,
                        released_at           = NULL
                    WHERE claim_id = %s AND status = 'RESERVED'
                    RETURNING claim_id, run_id, source_table_id, {ac},
                              lease_revision, fencing_epoch, fencing_token_digest,
                              owner_instance_id, worker_attempt_number, lease_expires_at
                    """.format(ac=_ACTION_COLUMN),
                    [token_digest, new_epoch, owner_instance_id, attempt_num,
                     new_worker_attempt, expires_at, cid],
                )
                updated = cur.fetchone()
                if not updated:
                    raise RuntimeError(
                        f"ScopeClaimManager: RESERVED->ACTIVE CAS failed for claim_id={claim_id}"
                    )
            conn.commit()

        return ExecutionFence(
            run_id=str(updated[1]),
            table_id=int(updated[2]),
            claim_id=int(updated[0]),
            scope_action=str(updated[3]),
            lease_revision=int(updated[4]),
            fencing_epoch=int(updated[5]),
            fencing_token_digest=str(updated[6]),
            raw_token=raw_token,              # held in caller memory only
            owner_instance_id=str(updated[7]),
            worker_attempt_number=int(updated[8]),
            lease_expires_at=updated[9],
        )

    # -------------------------------------------------------------------------
    # Heartbeat
    # -------------------------------------------------------------------------

    def heartbeat_claim(
        self,
        claim_id: int,
        raw_token: str,                   # raw token; hashed internally
        runtime_settings: RuntimeSettings,
    ) -> bool:
        """
        Renews lease for an ACTIVE claim. Returns False if the fence was lost.
        raw_token is hashed to digest internally - never stored or logged.
        """
        token_digest = _hash_raw_token(raw_token)
        expires_at = (
            datetime.now(timezone.utc).timestamp()
            + runtime_settings.worker_heartbeat_timeout_sec
        )
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.job_scope_claim
                    SET last_heartbeat   = CURRENT_TIMESTAMP,
                        lease_expires_at = to_timestamp(%s),
                        heartbeat_version = COALESCE(heartbeat_version, 0) + 1
                    WHERE claim_id            = %s
                      AND fencing_token_digest = %s
                      AND status               = 'ACTIVE'
                      AND released_at          IS NULL
                    RETURNING claim_id
                    """,
                    [expires_at, claim_id, token_digest],
                )
                claimed = cur.fetchone() is not None
            conn.commit()
        if not claimed:
            log.warning(
                "ScopeClaimManager: heartbeat lost for claim_id=%s (fence evicted)", claim_id
            )
        return claimed

    # -------------------------------------------------------------------------
    # Advance to PUBLISHING
    # -------------------------------------------------------------------------

    def advance_to_publishing(
        self,
        claim_id: int,
        raw_token: str,                   # raw token; hashed internally
        publication_deadline: datetime,
    ) -> bool:
        """
        CAS: ACTIVE -> PUBLISHING. Sets publication_deadline.
        Called before target DML - before the target lock is acquired.
        Returns False if fence was lost (caller must abort; no DML attempted).
        raw_token is hashed to digest internally - never stored or logged.
        """
        token_digest = _hash_raw_token(raw_token)
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.job_scope_claim
                    SET status               = 'PUBLISHING',
                        publication_deadline = %s
                    WHERE claim_id            = %s
                      AND fencing_token_digest = %s
                      AND status               = 'ACTIVE'
                    RETURNING claim_id
                    """,
                    [publication_deadline, claim_id, token_digest],
                )
                claimed = cur.fetchone() is not None
            conn.commit()
        return claimed

    # -------------------------------------------------------------------------
    # Release (normal completion)
    # -------------------------------------------------------------------------

    def release_claim(
        self,
        claim_id: int,
        raw_token: str,                   # raw token; hashed internally
    ) -> None:
        """
        Transitions ACTIVE -> RELEASED. Sets released_at.
        No-ops if already released (idempotent on double-call).

        Must NOT be called for PUBLISHING or RECOVERY_REQUIRED claims after a
        PostCommitUncertainError. Those are handled by mark_recovery_required()
        or reaper_expire_publishing(), respectively.

        raw_token is hashed to digest internally - never stored or logged.
        """
        token_digest = _hash_raw_token(raw_token)
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.job_scope_claim
                    SET status      = 'RELEASED',
                        released_at = CURRENT_TIMESTAMP
                    WHERE claim_id            = %s
                      AND fencing_token_digest = %s
                      AND status NOT IN ('RELEASED', 'REVOKED', 'EXPIRED',
                                         'PUBLISHING', 'RECOVERY_REQUIRED')
                    """,
                    [claim_id, token_digest],
                )
            conn.commit()

    # -------------------------------------------------------------------------
    # Revoke (operator / timeout path)
    # -------------------------------------------------------------------------

    def revoke_claim(self, claim_id: int) -> None:
        """
        Forces a claim to REVOKED regardless of fence token (operator/reaper use only).
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.job_scope_claim
                    SET status      = 'REVOKED',
                        released_at = CURRENT_TIMESTAMP
                    WHERE claim_id = %s
                      AND status NOT IN ('RELEASED', 'REVOKED', 'EXPIRED')
                    """,
                    [claim_id],
                )
            conn.commit()

    # -------------------------------------------------------------------------
    # Recovery acquisition (RecoveryDispatcher path)
    # -------------------------------------------------------------------------

    def acquire_recovery_claim(
        self,
        publication_id: str,
        owner_instance_id: str,
        runtime_settings: RuntimeSettings,
    ) -> ExecutionFence:
        """
        Finds a RECOVERY_REQUIRED claim for the given publication_id,
        locks it under SKIP LOCKED, and transitions it RECOVERY_REQUIRED -> ACTIVE.

        All identity (run_id, table_id, epoch) is derived from the locked row.
        No caller-provided epoch or contract is accepted.

        Returns ExecutionFence with raw_token - caller must not persist raw_token.
        Raises RuntimeError if no eligible claim is found (already taken or absent).
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Lock a single RECOVERY_REQUIRED row, skipping any already locked
                cur.execute(
                    f"""
                    SELECT c.claim_id, c.run_id, c.source_table_id, c.{_ACTION_COLUMN},
                           COALESCE(c.attempt_number, 1),
                           COALESCE(c.fencing_epoch, 0),
                           COALESCE(c.worker_attempt_number, 0)
                    FROM metadata.job_scope_claim c
                    JOIN metadata.gold_publication_manifest m
                      ON m.claim_id = c.claim_id
                    WHERE m.publication_id = %s
                      AND c.status = 'RECOVERY_REQUIRED'
                      AND c.released_at IS NULL
                    LIMIT 1
                    FOR UPDATE OF c SKIP LOCKED
                    """,
                    [publication_id],
                )
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(
                        f"ScopeClaimManager: no eligible RECOVERY_REQUIRED claim for "
                        f"publication_id={publication_id} (already taken or absent)"
                    )
                cid, run_id, table_id, scope_action, attempt_num, cur_epoch, cur_w_attempt = row

                # 2. Derive new fence values entirely from the locked row
                new_w_attempt = cur_w_attempt + 1
                new_epoch = cur_epoch + 1
                raw_token = _new_raw_token()
                token_digest = _hash_raw_token(raw_token)
                expires_at = (
                    datetime.now(timezone.utc).timestamp()
                    + runtime_settings.dispatch_lease_timeout_sec
                )

                # 3. CAS: RECOVERY_REQUIRED -> ACTIVE
                cur.execute(
                    f"""
                    UPDATE metadata.job_scope_claim
                    SET status                = 'ACTIVE',
                        fencing_token_digest  = %s,
                        fencing_epoch         = %s,
                        owner_instance_id     = %s,
                        worker_attempt_number = %s,
                        lease_revision        = COALESCE(lease_revision, 0) + 1,
                        lease_expires_at      = to_timestamp(%s),
                        last_heartbeat        = CURRENT_TIMESTAMP,
                        released_at           = NULL
                    WHERE claim_id = %s AND status = 'RECOVERY_REQUIRED'
                    RETURNING claim_id, run_id, source_table_id, {_ACTION_COLUMN},
                              lease_revision, fencing_epoch, fencing_token_digest,
                              owner_instance_id, worker_attempt_number, lease_expires_at
                    """,
                    [token_digest, new_epoch, owner_instance_id, new_w_attempt,
                     expires_at, cid],
                )
                updated = cur.fetchone()
                if not updated:
                    raise RuntimeError(
                        f"ScopeClaimManager: RECOVERY_REQUIRED->ACTIVE CAS failed for "
                        f"claim_id={cid} - possible concurrent recovery"
                    )
            conn.commit()

        log.info(
            "ScopeClaimManager: recovery claim acquired claim_id=%s run_id=%s epoch=%s",
            updated[0], updated[1], updated[5],
        )
        return ExecutionFence(
            run_id=str(updated[1]),
            table_id=int(updated[2]),
            claim_id=int(updated[0]),
            scope_action=str(updated[3]),
            lease_revision=int(updated[4]),
            fencing_epoch=int(updated[5]),
            fencing_token_digest=str(updated[6]),
            raw_token=raw_token,              # held in caller memory only
            owner_instance_id=str(updated[7]),
            worker_attempt_number=int(updated[8]),
            lease_expires_at=updated[9],
        )

    # -------------------------------------------------------------------------
    # Post-commit uncertainty handling
    # -------------------------------------------------------------------------

    def mark_recovery_required(
        self,
        claim_id: int,
        raw_token: str,
    ) -> None:
        """
        CAS: PUBLISHING -> RECOVERY_REQUIRED.

        Called by the worker when target commit is acknowledged but the
        post-commit metadata write fails (PostCommitUncertainError).
        The token guard proves this caller owns the PUBLISHING claim.

        Does NOT release the claim - the reaper/recovery path takes over.
        raw_token is hashed to digest internally - never stored or logged.
        """
        token_digest = _hash_raw_token(raw_token)
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.job_scope_claim
                    SET status = 'RECOVERY_REQUIRED'
                    WHERE claim_id            = %s
                      AND fencing_token_digest = %s
                      AND status               = 'PUBLISHING'
                      AND released_at          IS NULL
                    RETURNING claim_id
                    """,
                    [claim_id, token_digest],
                )
                updated = cur.fetchone()
            conn.commit()
        if not updated:
            log.warning(
                "ScopeClaimManager: mark_recovery_required found no PUBLISHING claim_id=%s "
                "(fence evicted or already transitioned)", claim_id
            )

    def reaper_expire_publishing(
        self,
        claim_id: int,
    ) -> None:
        """
        Reaper path: transitions an expired PUBLISHING claim -> RECOVERY_REQUIRED.
        No token - the reaper cannot hold the raw token.
        Only acts if lease_expires_at < now() to prevent premature expiry.
        Does NOT release the claim; recovery must inspect the target independently.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE metadata.job_scope_claim
                    SET status = 'RECOVERY_REQUIRED'
                    WHERE claim_id        = %s
                      AND status          = 'PUBLISHING'
                      AND lease_expires_at < CURRENT_TIMESTAMP
                      AND released_at     IS NULL
                    RETURNING claim_id
                    """,
                    [claim_id],
                )
                updated = cur.fetchone()
            conn.commit()
        if updated:
            log.info(
                "ScopeClaimManager: reaper_expire_publishing transitioned claim_id=%s "
                "to RECOVERY_REQUIRED", claim_id
            )
        else:
            log.debug(
                "ScopeClaimManager: reaper_expire_publishing no-op for claim_id=%s "
                "(not expired or already transitioned)", claim_id
            )
