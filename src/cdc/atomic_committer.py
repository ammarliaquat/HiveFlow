# src/cdc/atomic_committer.py
"""
OBS-01 — Atomic Cursor Advancement and Execution Success Certification.

Sole authority for transitioning a governed CDC attempt to terminal SUCCESS
and advancing the committed cursor watermark in metadata.table_cdc_state.

Invariants:
1. Shared advisory lock with META-01 to eliminate promotion/commit race conditions.
2. Direct database lineage resolution (trusts zero caller-supplied memory objects).
3. Exact batch_log_id binding (UNIQUE per commit event).
4. Exact replay handled before RUNNING state checks.
5. Strict canonical lock order to prevent deadlocks.
6. CAS verification on state_version and expected lower cursor L.
7. Explicit modes: BOOTSTRAP, INCREMENTAL, EMPTY.
8. Single transaction atomicity: cursor commit + batch SUCCESS + commit event.
"""

from datetime import datetime
import json
from typing import Any, Dict, Optional, Union
import uuid

from src.connections.connection_manager import ConnectionManager
from src.metadata.policy_models import (
    CDCCursor,
    CDCCommitMode,
    CDCCommitResult,
)
from src.exceptions import (
    CDCCommitFailedError,
    CDCStateCommitConflictError,
    CDCCommitReplayConflictError,
    JobStateInvalidError,
    BatchStateInvalidError,
    GovernedPolicyBindingMissingError,
    PolicyVersionSupersededError,
    ReconciliationFailedError,
    ReconciliationOutcomeInvalidError,
    ReconciliationNotAuthoritativeError,
    AttemptIdentityMismatchError,
)


class AtomicCDCCommitter:
    """
    Authoritative service executing atomic CDC watermark advancement and success certification.
    """

    def __init__(self, conn_manager: ConnectionManager):
        self._conn_manager = conn_manager

    def commit_attempt_watermark_and_success(
        self,
        batch_log_id: int,
        reconciliation_id: Union[uuid.UUID, str],
        run_id: Optional[str] = None,
        expected_state_version: Optional[int] = None,
    ) -> CDCCommitResult:
        """
        Atomically commits the CDC watermark and marks the batch attempt SUCCESS.
        Consumes identities directly from the metadata database.
        """
        recon_uuid = uuid.UUID(str(reconciliation_id))

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # ─── Step 1: Pre-fetch attempt's table_id to acquire META-01 advisory lock first!
                cur.execute("""
                    SELECT COALESCE(pv.table_id, t.table_id)
                    FROM metadata.batch_log b
                    LEFT JOIN metadata.table_policy_version pv ON pv.policy_version_id = b.policy_version_id
                    LEFT JOIN metadata.db_tables_etl t ON t.source_table = b.source_table
                    WHERE b.id = %s
                """, [batch_log_id])
                row = cur.fetchone()
                if not row or row[0] is None:
                    raise BatchStateInvalidError(f"Batch log ID {batch_log_id} not found or table unresolved.")
                table_id = row[0]

                # ─── Canonical Lock 1: pg_advisory_xact_lock(hashtext('table_policy:' || table_id))
                cur.execute("SELECT pg_advisory_xact_lock(hashtext('table_policy:' || %s))", [str(table_id)])

                # ─── Step 2: Check for existing commit on this exact batch_log_id (Exact Replay Idempotency)
                cur.execute("""
                    SELECT commit_id, run_id, table_id, attempt_number, policy_version_id,
                           reconciliation_id, manifest_id, window_id, previous_cursor,
                           committed_cursor, previous_state_version, new_state_version,
                           policy_fingerprint, window_fingerprint, manifest_fingerprint,
                           reconciliation_fingerprint, commit_mode, committed_at
                    FROM metadata.cdc_commit_event
                    WHERE batch_log_id = %s
                """, [batch_log_id])
                existing_commit = cur.fetchone()
                if existing_commit:
                    (e_cid, e_run_id, e_tid, e_att, e_pvid, e_recid, e_mid, e_wid,
                     e_prev_c, e_comm_c, e_prev_ver, e_new_ver, e_pfp, e_wfp, e_mfp,
                     e_rfp, e_cmode, e_cat) = existing_commit

                    if (
                        str(e_recid) != str(recon_uuid)
                        or (run_id and str(e_run_id) != str(run_id))
                        or e_tid != table_id
                    ):
                        raise CDCCommitReplayConflictError(
                            f"Existing commit on batch_log_id {batch_log_id} conflicts with requested reconciliation {recon_uuid}."
                        )

                    c_cursor = CDCCursor.from_dict(e_comm_c if isinstance(e_comm_c, dict) else json.loads(e_comm_c))
                    p_cursor = CDCCursor.from_dict(e_prev_c if isinstance(e_prev_c, dict) else json.loads(e_prev_c)) if e_prev_c else None
                    return CDCCommitResult(
                        commit_id=uuid.UUID(str(e_cid)),
                        batch_log_id=batch_log_id,
                        run_id=str(e_run_id),
                        table_id=e_tid,
                        attempt_number=e_att,
                        policy_version_id=str(e_pvid),
                        reconciliation_id=uuid.UUID(str(e_recid)),
                        committed_cursor=c_cursor,
                        new_state_version=e_new_ver,
                        commit_mode=CDCCommitMode(e_cmode),
                        previous_cursor=p_cursor,
                        previous_state_version=e_prev_ver,
                        already_committed=True,
                        committed_at=e_cat,
                    )

                # ─── Canonical Lock 2: Lock and verify parent job in gui_job_registry
                cur.execute("""
                    SELECT run_id, attempt_number, policy_version_id, status, target_table
                    FROM metadata.batch_log
                    WHERE id = %s
                    FOR UPDATE
                """, [batch_log_id])
                b_row = cur.fetchone()
                if not b_row:
                    raise BatchStateInvalidError(f"Batch log ID {batch_log_id} not found.")
                b_run_id, b_attempt_number, b_policy_version_id, b_status, b_target_table = b_row

                if (
                    b_policy_version_id is None
                    or not str(b_policy_version_id).strip()
                    or str(b_policy_version_id).strip().lower() in {"none", "null"}
                ):
                    raise GovernedPolicyBindingMissingError(
                        f"Batch log {batch_log_id} has no immutable frozen policy version; "
                        "refusing CDC commit rather than comparing it to the current policy."
                    )

                if run_id and str(b_run_id) != str(run_id):
                    raise AttemptIdentityMismatchError(f"Caller run_id {run_id} does not match batch_log run_id {b_run_id}.")

                cur.execute("SELECT status FROM metadata.gui_job_registry WHERE run_id = %s FOR SHARE", [b_run_id])
                j_row = cur.fetchone()
                if not j_row or j_row[0] != "RUNNING":
                    job_st = j_row[0] if j_row else "NOT_FOUND"
                    raise JobStateInvalidError(f"Parent job {b_run_id} is in status '{job_st}', must be 'RUNNING'.")

                # ─── Canonical Lock 3: Verify batch_log is RUNNING
                if b_status != "RUNNING":
                    raise BatchStateInvalidError(f"Batch log {batch_log_id} is in status '{b_status}', must be 'RUNNING'.")

                # ─── Canonical Lock 4: Lock/verify active policy in db_tables_etl
                cur.execute("""
                    SELECT current_policy_version_id, target_table, target_schema
                    FROM metadata.db_tables_etl
                    WHERE table_id = %s
                    FOR UPDATE
                """, [table_id])
                meta_row = cur.fetchone()
                if not meta_row or not meta_row[0]:
                    raise PolicyVersionSupersededError(f"Table ID {table_id} has no active promoted policy version.")
                curr_pvid = str(meta_row[0])
                if curr_pvid != str(b_policy_version_id):
                    raise PolicyVersionSupersededError(
                        f"Policy version {b_policy_version_id} was superseded by {curr_pvid} for table {table_id}."
                    )

                # Verify promoted policy version record & fingerprint
                cur.execute("""
                    SELECT policy_fingerprint, contract_payload
                    FROM metadata.table_policy_version
                    WHERE policy_version_id = %s
                """, [curr_pvid])
                pol_row = cur.fetchone()
                if not pol_row:
                    raise PolicyVersionSupersededError(f"Policy version record {curr_pvid} not found.")
                policy_fingerprint = pol_row[0]

                # ─── Step 5: Read & verify authoritative reconciliation, manifest, and window
                cur.execute("""
                    SELECT reconciliation_id, manifest_id, run_id, table_id, attempt_number,
                           policy_version_id, window_id, reconciliation_strategy, storage_layer,
                           is_commit_authoritative, outcome, failure_code,
                           reconciliation_fingerprint
                    FROM metadata.cdc_attempt_reconciliation
                    WHERE reconciliation_id = %s
                    FOR UPDATE
                """, [str(recon_uuid)])
                r_row = cur.fetchone()
                if not r_row:
                    raise ReconciliationFailedError(f"Reconciliation record {recon_uuid} not found.")

                (r_id, r_mid, r_run_id, r_tid, r_att, r_pvid, r_wid, r_strat,
                 r_layer, r_auth, r_outcome, r_fcode, r_rfp) = r_row

                # Verify complete identity chain
                if (
                    str(r_run_id) != str(b_run_id)
                    or r_tid != table_id
                    or r_att != b_attempt_number
                    or str(r_pvid) != curr_pvid
                ):
                    raise AttemptIdentityMismatchError(
                        f"Reconciliation record {recon_uuid} does not match attempt identity: "
                        f"run={r_run_id} vs {b_run_id}, table={r_tid} vs {table_id}, attempt={r_att} vs {b_attempt_number}."
                    )

                if not r_auth:
                    raise ReconciliationNotAuthoritativeError(
                        f"Reconciliation record {recon_uuid} for layer '{r_layer}' is not commit-authoritative."
                    )

                if r_outcome not in ("CONVERGED", "EMPTY_VERIFIED"):
                    raise ReconciliationOutcomeInvalidError(
                        f"Reconciliation outcome is '{r_outcome}' (failure_code: {r_fcode}). Cannot commit."
                    )

                # Load Manifest
                cur.execute("""
                    SELECT manifest_id, storage_layer, storage_object_identity,
                           expected_row_count, expected_key_fingerprint, schema_fingerprint,
                           manifest_fingerprint
                    FROM metadata.attempt_persistence_manifest
                    WHERE manifest_id = %s
                """, [str(r_mid)])
                m_row = cur.fetchone()
                if not m_row:
                    raise ReconciliationFailedError(f"Persistence manifest {r_mid} not found.")
                m_id, m_layer, m_dest, m_exp_count, m_ek_fp, m_sch_fp, m_mfp = m_row

                # Load CDC Window
                cur.execute("""
                    SELECT window_id, cdc_policy_fingerprint, window_fingerprint,
                           lower_cursor, upper_cursor
                    FROM metadata.cdc_attempt_window
                    WHERE window_id = %s
                """, [str(r_wid)])
                w_row = cur.fetchone()
                if not w_row:
                    raise ReconciliationFailedError(f"CDC attempt window {r_wid} not found.")
                w_id, w_cdc_pfp, w_wfp, w_lower_dict, w_upper_dict = w_row

                lower_dict = w_lower_dict if isinstance(w_lower_dict, dict) else (json.loads(w_lower_dict) if w_lower_dict else None)
                upper_dict = w_upper_dict if isinstance(w_upper_dict, dict) else json.loads(w_upper_dict)
                lower_cursor = CDCCursor.from_dict(lower_dict) if lower_dict else None
                upper_cursor = CDCCursor.from_dict(upper_dict)

                # ─── Step 6: Determine Commit Mode & Execute Cursor CAS
                cur.execute("""
                    SELECT committed_cursor, state_version, cdc_policy_fingerprint
                    FROM metadata.table_cdc_state
                    WHERE table_id = %s AND policy_version_id = %s
                    FOR UPDATE
                """, [table_id, curr_pvid])
                state_row = cur.fetchone()

                if expected_state_version is not None:
                    observed_state_version = state_row[1] if state_row else None
                    if observed_state_version != int(expected_state_version):
                        raise CDCStateCommitConflictError(
                            "CAS conflict: CDC state_version changed before commit "
                            f"(expected {expected_state_version}, observed {observed_state_version})."
                        )

                is_empty_run = (r_outcome == "EMPTY_VERIFIED" or (lower_cursor and lower_cursor.values[0] == upper_cursor.values[0]))

                if is_empty_run:
                    commit_mode = CDCCommitMode.EMPTY
                    if not state_row:
                        prev_state_ver = None
                        new_state_ver = 1
                        prev_cursor_dict = None
                        # Insert initial empty state
                        cur.execute("""
                            INSERT INTO metadata.table_cdc_state (
                                table_id, policy_version_id, cdc_policy_fingerprint,
                                committed_cursor, committed_run_id, committed_attempt_number,
                                committed_window_fingerprint, committed_at, state_version
                            ) VALUES (
                                %s, %s, %s, %s::jsonb, %s, %s, %s, CURRENT_TIMESTAMP, 1
                            ) ON CONFLICT (table_id, policy_version_id) DO NOTHING
                        """, [
                            table_id, curr_pvid, w_cdc_pfp, json.dumps(upper_dict),
                            str(b_run_id), b_attempt_number, w_wfp
                        ])
                    else:
                        curr_state_cursor, curr_state_ver, curr_state_pfp = state_row
                        prev_state_ver = curr_state_ver
                        new_state_ver = curr_state_ver  # Does NOT increment state_version for empty!
                        prev_cursor_dict = curr_state_cursor if isinstance(curr_state_cursor, dict) else json.loads(curr_state_cursor)
                elif not state_row:
                    # BOOTSTRAP MODE
                    commit_mode = CDCCommitMode.BOOTSTRAP
                    prev_state_ver = None
                    new_state_ver = 1
                    prev_cursor_dict = None

                    cur.execute("""
                        INSERT INTO metadata.table_cdc_state (
                            table_id, policy_version_id, cdc_policy_fingerprint,
                            committed_cursor, committed_run_id, committed_attempt_number,
                            committed_window_fingerprint, committed_at, state_version
                        ) VALUES (
                            %s, %s, %s, %s::jsonb, %s, %s, %s, CURRENT_TIMESTAMP, 1
                        ) ON CONFLICT (table_id, policy_version_id) DO NOTHING
                        RETURNING state_version
                    """, [
                        table_id, curr_pvid, w_cdc_pfp, json.dumps(upper_dict),
                        str(b_run_id), b_attempt_number, w_wfp
                    ])
                    inserted = cur.fetchone()
                    if not inserted:
                        raise CDCStateCommitConflictError(
                            f"Concurrent bootstrap commit conflict on table {table_id}, policy {curr_pvid}."
                        )
                else:
                    # INCREMENTAL MODE
                    commit_mode = CDCCommitMode.INCREMENTAL
                    curr_state_cursor, curr_state_ver, curr_state_pfp = state_row
                    prev_state_ver = curr_state_ver
                    new_state_ver = curr_state_ver + 1
                    prev_cursor_dict = curr_state_cursor if isinstance(curr_state_cursor, dict) else json.loads(curr_state_cursor)

                    # CAS Verification: committed_cursor == lower_cursor
                    if not lower_cursor or prev_cursor_dict["values"] != list(lower_cursor.values):
                        raise CDCStateCommitConflictError(
                            f"CAS conflict: committed cursor in DB {prev_cursor_dict} does not match expected lower cursor {lower_dict}."
                        )

                    cur.execute("""
                        UPDATE metadata.table_cdc_state
                        SET committed_cursor = %s::jsonb,
                            committed_run_id = %s,
                            committed_attempt_number = %s,
                            committed_window_fingerprint = %s,
                            committed_at = CURRENT_TIMESTAMP,
                            state_version = state_version + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE table_id = %s AND policy_version_id = %s AND state_version = %s
                        RETURNING state_version
                    """, [
                        json.dumps(upper_dict), str(b_run_id), b_attempt_number, w_wfp,
                        table_id, curr_pvid, curr_state_ver
                    ])
                    updated = cur.fetchone()
                    if not updated:
                        raise CDCStateCommitConflictError(
                            f"CAS conflict: state_version {curr_state_ver} changed concurrently on table {table_id}."
                        )

                # ─── Step 7: Atomically mark batch_log SUCCESS
                cur.execute("""
                    UPDATE metadata.batch_log
                    SET status = 'SUCCESS',
                        end_time = CURRENT_TIMESTAMP,
                        target_count = %s,
                        processed_count = %s
                    WHERE id = %s
                """, [m_exp_count, m_exp_count, batch_log_id])

                # ─── Step 8: Insert immutable commit event
                commit_id = uuid.uuid4()
                cur.execute("""
                    INSERT INTO metadata.cdc_commit_event (
                        commit_id, batch_log_id, run_id, table_id, attempt_number,
                        policy_version_id, reconciliation_id, manifest_id, window_id,
                        previous_cursor, committed_cursor, previous_state_version,
                        new_state_version, policy_fingerprint, window_fingerprint,
                        manifest_fingerprint, reconciliation_fingerprint, commit_mode,
                        committed_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                    )
                """, [
                    str(commit_id), batch_log_id, str(b_run_id), table_id, b_attempt_number,
                    curr_pvid, str(recon_uuid), str(r_mid), str(r_wid),
                    json.dumps(prev_cursor_dict) if prev_cursor_dict else None,
                    json.dumps(upper_dict), prev_state_ver, new_state_ver,
                    policy_fingerprint, w_wfp, m_mfp, r_rfp, commit_mode.value
                ])

            conn.commit()

        return CDCCommitResult(
            commit_id=commit_id,
            batch_log_id=batch_log_id,
            run_id=str(b_run_id),
            table_id=table_id,
            attempt_number=b_attempt_number,
            policy_version_id=curr_pvid,
            reconciliation_id=recon_uuid,
            committed_cursor=upper_cursor,
            new_state_version=new_state_ver,
            commit_mode=commit_mode,
            previous_cursor=lower_cursor,
            previous_state_version=prev_state_ver,
            already_committed=False,
            committed_at=datetime.utcnow(),
        )
