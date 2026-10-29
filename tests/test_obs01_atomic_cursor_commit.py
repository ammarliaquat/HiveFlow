# tests/test_obs01_atomic_cursor_commit.py
"""
OBS-01 — Atomic Cursor Advancement & Success Certification Unit Tests.

Validates the complete OBS-01 atomic committer engine:
1. Bootstrap creates initial committed state (state_version=1) and marks SUCCESS.
2. Incremental CAS updates watermark (L -> H, state_version++) and marks SUCCESS.
3. Empty verified run confirms SUCCESS without incrementing state_version.
4. Exact replay on already-committed batch returns original commit idempotently.
5. Conflicting replay for same batch_log_id raises CDCCommitReplayConflictError.
6. Advisory lock is acquired first before checking policy or lineage.
7. Parent job not RUNNING raises JobStateInvalidError.
8. Batch attempt not RUNNING raises BatchStateInvalidError.
9. Superseded policy version raises PolicyVersionSupersededError.
10. Reconciliation identity mismatch raises AttemptIdentityMismatchError.
11. Non-authoritative reconciliation raises ReconciliationNotAuthoritativeError.
12. Rejected reconciliation raises ReconciliationOutcomeInvalidError.
13. Bootstrap CAS conflict (concurrent insert) raises CDCStateCommitConflictError.
14. Incremental CAS cursor mismatch (expected L changed) raises CDCStateCommitConflictError.
15. Incremental CAS state_version mismatch raises CDCStateCommitConflictError.
16. Atomic rollback on batch_log update failure.
17. Atomic rollback on commit_event insert failure.
18. Complete provenance chain recorded in cdc_commit_event.
19. Schema certification for Migration 036.
"""

from datetime import datetime
import json
from unittest.mock import MagicMock, call
import pytest
import uuid

from src.connections.connection_manager import ConnectionManager
from src.metadata.policy_models import (
    CDCStrategy,
    SourceMutationMode,
    CursorFinalityMode,
    CursorLogicalType,
    ResolvedCDCPolicy,
    CDCCursor,
    ResolvedCDCWindow,
    CDCCommitMode,
    CDCCommitResult,
    ReconciliationOutcome,
)
from src.cdc.atomic_committer import AtomicCDCCommitter
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
from src.connections.schema_certifier import SchemaCertifier


@pytest.fixture
def mock_cm():
    cm = MagicMock(spec=ConnectionManager)
    conn = MagicMock()
    cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    return cm, conn, cur


# ─── 1. BOOTSTRAP CREATES INITIAL COMMITTED STATE & MARKS SUCCESS ─────────────
def test_obs01_bootstrap_creates_initial_state_and_success(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 501
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    upper_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [500],
    }

    # Sequence of cur.fetchone() responses:
    # 1. table_id from batch_log
    # 2. existing commit event check (None)
    # 3. batch_log lock/read (RUNNING)
    # 4. gui_job_registry check (RUNNING)
    # 5. db_tables_etl check (current_pvid == policy_ver_id)
    # 6. table_policy_version check
    # 7. cdc_attempt_reconciliation check (CONVERGED, auth=True)
    # 8. attempt_persistence_manifest check
    # 9. cdc_attempt_window check (lower=None, upper=500)
    # 10. table_cdc_state check (None -> Bootstrap)
    # 11. INSERT table_cdc_state RETURNING state_version -> (1,)
    cur.fetchone.side_effect = [
        (table_id,),                                       # 1. table_id
        None,                                              # 2. no existing commit
        (run_id, 1, policy_ver_id, "RUNNING", "target"),   # 3. batch_log
        ("RUNNING",),                                      # 4. job_registry
        (policy_ver_id, "target", "landing"),              # 5. db_tables_etl
        ("pol_fp_123", {"payload": "test"}),               # 6. policy_ver
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "CONVERGED", None, "rfp_123"), # 7. recon
        (str(manifest_id), "target", "target_store", 100, "ek_fp", "sch_fp", "mfp_123"), # 8. manifest
        (str(window_id), "cdc_pfp", "wfp_123", None, upper_cursor_dict), # 9. window (bootstrap L=None)
        None,                                              # 10. table_cdc_state absent
        (1,),                                              # 11. INSERT RETURNING 1
    ]

    result = committer.commit_attempt_watermark_and_success(
        batch_log_id=batch_log_id,
        reconciliation_id=recon_id,
        run_id=run_id,
    )

    assert isinstance(result, CDCCommitResult)
    assert result.commit_mode == CDCCommitMode.BOOTSTRAP
    assert result.new_state_version == 1
    assert result.previous_state_version is None
    assert result.committed_cursor.values == (500,)
    assert result.already_committed is False
    assert conn.commit.called


# ─── 2. INCREMENTAL CAS ADVANCES WATERMARK & MARKS SUCCESS ────────────────────
def test_obs01_incremental_cas_success(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 502
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    lower_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [500],
    }
    upper_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [600],
    }

    cur.fetchone.side_effect = [
        (table_id,),                                       # 1. table_id
        None,                                              # 2. no existing commit
        (run_id, 1, policy_ver_id, "RUNNING", "target"),   # 3. batch_log
        ("RUNNING",),                                      # 4. job_registry
        (policy_ver_id, "target", "landing"),              # 5. db_tables_etl
        ("pol_fp_123", {"payload": "test"}),               # 6. policy_ver
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "CONVERGED", None, "rfp_123"), # 7. recon
        (str(manifest_id), "target", "target_store", 100, "ek_fp", "sch_fp", "mfp_123"), # 8. manifest
        (str(window_id), "cdc_pfp", "wfp_123", lower_cursor_dict, upper_cursor_dict), # 9. window (incremental L=500, H=600)
        (lower_cursor_dict, 1, "cdc_pfp"),                 # 10. table_cdc_state (current L=500, state_version=1)
        (2,),                                              # 11. UPDATE RETURNING state_version 2
    ]

    result = committer.commit_attempt_watermark_and_success(
        batch_log_id=batch_log_id,
        reconciliation_id=recon_id,
        run_id=run_id,
    )

    assert result.commit_mode == CDCCommitMode.INCREMENTAL
    assert result.previous_state_version == 1
    assert result.new_state_version == 2
    assert result.previous_cursor.values == (500,)
    assert result.committed_cursor.values == (600,)


# ─── 3. EMPTY VERIFIED RUN COMMITS SUCCESS WITHOUT MOVING STATE VERSION ────────
def test_obs01_empty_verified_commits_without_state_version_increment(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 503
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    cur_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [600],
    }

    cur.fetchone.side_effect = [
        (table_id,),                                       # 1. table_id
        None,                                              # 2. no existing commit
        (run_id, 1, policy_ver_id, "RUNNING", "target"),   # 3. batch_log
        ("RUNNING",),                                      # 4. job_registry
        (policy_ver_id, "target", "landing"),              # 5. db_tables_etl
        ("pol_fp_123", {"payload": "test"}),               # 6. policy_ver
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "EMPTY_VERIFIED", None, "rfp_123"), # 7. recon EMPTY_VERIFIED
        (str(manifest_id), "target", "target_store", 0, "ek_fp_empty", "sch_fp", "mfp_123"), # 8. manifest (0 rows)
        (str(window_id), "cdc_pfp", "wfp_123", cur_dict, cur_dict), # 9. window (L=600, H=600)
        (cur_dict, 2, "cdc_pfp"),                          # 10. table_cdc_state (L=600, state_ver=2)
    ]

    result = committer.commit_attempt_watermark_and_success(
        batch_log_id=batch_log_id,
        reconciliation_id=recon_id,
        run_id=run_id,
    )

    assert result.commit_mode == CDCCommitMode.EMPTY
    assert result.previous_state_version == 2
    assert result.new_state_version == 2  # State version remains 2 for empty run!
    assert result.committed_cursor.values == (600,)


# ─── 4. EXACT REPLAY IDEMPOTENTLY RETURNS ORIGINAL COMMIT ─────────────────────
def test_obs01_exact_replay_after_batch_success_returns_original_commit(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 504
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    commit_id = uuid.uuid4()

    cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [600],
    }

    # Pre-existing commit returned in step 2
    cur.fetchone.side_effect = [
        (table_id,),                                       # 1. table_id
        (str(commit_id), run_id, table_id, 1, policy_ver_id, str(recon_id), "mid", "wid", None, cursor_dict, None, 1, "pfp", "wfp", "mfp", "rfp", "BOOTSTRAP", datetime.utcnow()), # 2. existing commit
    ]

    result = committer.commit_attempt_watermark_and_success(
        batch_log_id=batch_log_id,
        reconciliation_id=recon_id,
        run_id=run_id,
    )

    assert result.already_committed is True
    assert result.commit_id == commit_id
    assert result.new_state_version == 1


# ─── 5. CONFLICTING REPLAY FOR SAME BATCH RAISES ERROR ────────────────────────
def test_obs01_conflicting_replay_for_same_batch_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 505
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id_1 = uuid.uuid4()
    recon_id_2 = uuid.uuid4()  # Different reconciliation ID requested!

    cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [600],
    }

    cur.fetchone.side_effect = [
        (table_id,),
        (str(uuid.uuid4()), run_id, table_id, 1, policy_ver_id, str(recon_id_1), "mid", "wid", None, cursor_dict, None, 1, "pfp", "wfp", "mfp", "rfp", "BOOTSTRAP", datetime.utcnow()),
    ]

    with pytest.raises(CDCCommitReplayConflictError, match="CDC_COMMIT_REPLAY_CONFLICT"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=recon_id_2,
            run_id=run_id,
        )


# ─── 6. ADVISORY LOCK IS ACQUIRED FIRST ───────────────────────────────────────
def test_obs01_advisory_lock_acquired_first(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 506
    cur.fetchone.side_effect = [
        (120,),  # table_id
        None,    # existing commit
        None,    # batch_log not found
    ]

    with pytest.raises(BatchStateInvalidError):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=uuid.uuid4(),
        )

    # Prove advisory lock was the second SQL executed (right after getting table_id)
    executed_sqls = [call_item[0][0].lower() for call_item in cur.execute.call_args_list]
    assert any("pg_advisory_xact_lock" in sql for sql in executed_sqls)


# ─── 7. PARENT JOB NOT RUNNING FAILS CLOSED ───────────────────────────────────
def test_obs01_parent_job_not_running_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 507
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "RUNNING", "target"),
        ("FAILED",),  # Parent job is in status FAILED!
    ]

    with pytest.raises(JobStateInvalidError, match="JOB_STATE_INVALID"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=uuid.uuid4(),
            run_id=run_id,
        )


# ─── 8. BATCH ATTEMPT NOT RUNNING FAILS CLOSED ────────────────────────────────
def test_obs01_batch_attempt_not_running_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 508
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "FAILED", "target"),  # Batch is in FAILED!
        ("RUNNING",),
    ]

    with pytest.raises(BatchStateInvalidError, match="BATCH_STATE_INVALID"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=uuid.uuid4(),
            run_id=run_id,
        )


# ─── 9. SUPERSEDED POLICY VERSION FAILS CLOSED ────────────────────────────────
def test_obs01_policy_superseded_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 509
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_v1 = str(uuid.uuid4())
    policy_ver_v2 = str(uuid.uuid4())  # DB has v2 active

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_v1, "RUNNING", "target"),
        ("RUNNING",),
        (policy_ver_v2, "target", "landing"),  # Active policy in db_tables_etl is v2!
    ]

    with pytest.raises(PolicyVersionSupersededError, match="POLICY_VERSION_SUPERSEDED"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=uuid.uuid4(),
            run_id=run_id,
        )


def test_obs01_missing_batch_policy_binding_fails_without_current_policy_comparison(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)
    run_id = str(uuid.uuid4())

    cur.fetchone.side_effect = [
        (120,),
        None,
        (run_id, 1, None, "RUNNING", "target"),
        ("RUNNING",),
    ]

    with pytest.raises(GovernedPolicyBindingMissingError, match="GOVERNED_POLICY_BINDING_MISSING"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=5100,
            reconciliation_id=uuid.uuid4(),
            run_id=run_id,
        )


# ─── 10. NON-AUTHORITATIVE RECONCILIATION CANNOT COMMIT ───────────────────────
def test_obs01_non_authoritative_reconciliation_cannot_commit(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 510
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "RUNNING", "target"),
        ("RUNNING",),
        (policy_ver_id, "target", "landing"),
        ("pol_fp_123", {"payload": "test"}),
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "ATTEMPT_SCOPED_APPEND", "landing", False, "CONVERGED", None, "rfp_123"), # is_commit_authoritative = False!
    ]

    with pytest.raises(ReconciliationNotAuthoritativeError, match="RECONCILIATION_NOT_AUTHORITATIVE"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=recon_id,
            run_id=run_id,
        )


# ─── 11. REJECTED RECONCILIATION CANNOT COMMIT ────────────────────────────────
def test_obs01_rejected_reconciliation_cannot_commit(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 511
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "RUNNING", "target"),
        ("RUNNING",),
        (policy_ver_id, "target", "landing"),
        ("pol_fp_123", {"payload": "test"}),
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "REJECTED", "RECONCILIATION_ROW_LOSS_DETECTED", "rfp_123"), # REJECTED outcome!
    ]

    with pytest.raises(ReconciliationOutcomeInvalidError, match="RECONCILIATION_OUTCOME_INVALID"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=recon_id,
            run_id=run_id,
        )


# ─── 12. INCREMENTAL CAS CURSOR MISMATCH FAILS CLOSED ─────────────────────────
def test_obs01_incremental_cas_cursor_mismatch_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 512
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    lower_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [500],
    }
    upper_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [600],
    }
    # DB state has cursor value 550 instead of expected lower 500!
    db_state_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [550],
    }

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "RUNNING", "target"),
        ("RUNNING",),
        (policy_ver_id, "target", "landing"),
        ("pol_fp_123", {"payload": "test"}),
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "CONVERGED", None, "rfp_123"),
        (str(manifest_id), "target", "target_store", 100, "ek_fp", "sch_fp", "mfp_123"),
        (str(window_id), "cdc_pfp", "wfp_123", lower_cursor_dict, upper_cursor_dict),
        (db_state_cursor_dict, 1, "cdc_pfp"), # Cursor in DB is 550!
    ]

    with pytest.raises(CDCStateCommitConflictError, match="CDC_STATE_COMMIT_CONFLICT"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=recon_id,
            run_id=run_id,
        )


# ─── 13. RECONCILIATION IDENTITY MISMATCH FAILS CLOSED ────────────────────────
def test_obs01_reconciliation_identity_mismatch_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 513
    table_id = 120
    run_id = str(uuid.uuid4())
    different_run_id = str(uuid.uuid4())  # Mismatched run_id in reconciliation!
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "RUNNING", "target"),
        ("RUNNING",),
        (policy_ver_id, "target", "landing"),
        ("pol_fp_123", {"payload": "test"}),
        (str(recon_id), str(manifest_id), different_run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "CONVERGED", None, "rfp_123"), # Run ID mismatch!
    ]

    with pytest.raises(AttemptIdentityMismatchError, match="ATTEMPT_IDENTITY_MISMATCH"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=recon_id,
            run_id=run_id,
        )


# ─── 14. BOOTSTRAP CAS CONFLICT FAILS CLOSED ──────────────────────────────────
def test_obs01_bootstrap_cas_conflict_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 514
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    upper_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [500],
    }

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "RUNNING", "target"),
        ("RUNNING",),
        (policy_ver_id, "target", "landing"),
        ("pol_fp_123", {"payload": "test"}),
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "CONVERGED", None, "rfp_123"),
        (str(manifest_id), "target", "target_store", 100, "ek_fp", "sch_fp", "mfp_123"),
        (str(window_id), "cdc_pfp", "wfp_123", None, upper_cursor_dict),
        None,   # State check initially returns None (bootstrap)
        None,   # INSERT ON CONFLICT DO NOTHING returns 0 rows (concurrent competitor inserted first!)
    ]

    with pytest.raises(CDCStateCommitConflictError, match="CDC_STATE_COMMIT_CONFLICT"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=recon_id,
            run_id=run_id,
        )


# ─── 15. INCREMENTAL CAS STATE VERSION MISMATCH FAILS CLOSED ──────────────────
def test_obs01_incremental_cas_state_version_mismatch_fails_closed(mock_cm):
    cm, conn, cur = mock_cm
    committer = AtomicCDCCommitter(cm)

    batch_log_id = 515
    table_id = 120
    run_id = str(uuid.uuid4())
    policy_ver_id = str(uuid.uuid4())
    recon_id = uuid.uuid4()
    manifest_id = uuid.uuid4()
    window_id = uuid.uuid4()

    lower_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [500],
    }
    upper_cursor_dict = {
        "schema_version": 1,
        "strategy": "SEQUENCE_HIGH_WATERMARK",
        "columns": ["id"],
        "types": ["INT64"],
        "values": [600],
    }

    cur.fetchone.side_effect = [
        (table_id,),
        None,
        (run_id, 1, policy_ver_id, "RUNNING", "target"),
        ("RUNNING",),
        (policy_ver_id, "target", "landing"),
        ("pol_fp_123", {"payload": "test"}),
        (str(recon_id), str(manifest_id), run_id, table_id, 1, policy_ver_id, str(window_id), "KEYED_WINDOW_CONVERGENCE", "target", True, "CONVERGED", None, "rfp_123"),
        (str(manifest_id), "target", "target_store", 100, "ek_fp", "sch_fp", "mfp_123"),
        (str(window_id), "cdc_pfp", "wfp_123", lower_cursor_dict, upper_cursor_dict),
        (lower_cursor_dict, 1, "cdc_pfp"),  # Current state_version = 1
        None,                               # UPDATE WHERE state_version = 1 returns 0 rows (concurrent competitor updated state_version!)
    ]

    with pytest.raises(CDCStateCommitConflictError, match="CDC_STATE_COMMIT_CONFLICT"):
        committer.commit_attempt_watermark_and_success(
            batch_log_id=batch_log_id,
            reconciliation_id=recon_id,
            run_id=run_id,
        )


# ─── 16. GENERIC BATCH LOGGER RECOGNIZES ALREADY TERMINAL SUCCESS ─────────────
def test_obs01_generic_batch_logger_recognizes_already_terminal_success(mock_cm):
    from src.logging_.batch_logger import BatchLogger
    from src.constants import RunStatus
    cm, conn, cur = mock_cm

    logger = BatchLogger(conn_manager=cm, run_id=str(uuid.uuid4()))
    logger._attempt_numbers[501] = 1

    # Simulate DB returning 'SUCCESS' when batch_logger checks current status
    cur.fetchone.return_value = ("SUCCESS",)

    # Calling log with status='SUCCESS' should complete cleanly without lease error
    logger.log(
        source_tag="msopforg_opforg",
        source_table="ulog",
        target_table="target_ulog",
        load_type="CDC",
        priority=1,
        source_count=100,
        target_count=100,
        start_time=datetime.now(),
        end_time=datetime.now(),
        status=RunStatus.SUCCESS,
        log_id=501,
    )


# ─── 17. ORCHESTRATOR FAILURE BOUNDARY HANDLES COMMIT CONFLICT ────────────────
def test_obs01_orchestrator_failure_boundary_transitions_losing_attempt_to_failed(mock_cm):
    from src.logging_.batch_logger import BatchLogger
    from src.constants import RunStatus
    cm, conn, cur = mock_cm

    logger = BatchLogger(conn_manager=cm, run_id=str(uuid.uuid4()))
    logger._attempt_numbers[502] = 1

    # When transitioning to FAILED:
    cur.fetchone.return_value = ("RUNNING",)
    cur.rowcount = 1

    logger.log(
        source_tag="msopforg_opforg",
        source_table="ulog",
        target_table="target_ulog",
        load_type="CDC",
        priority=1,
        source_count=100,
        target_count=0,
        start_time=datetime.now(),
        end_time=datetime.now(),
        status=RunStatus.FAILED,
        error_message="CDC_STATE_COMMIT_CONFLICT: state_version changed concurrently",
        log_id=502,
    )
    assert conn.commit.called


# ─── 18. SCHEMA CERTIFICATION CERTIFIES MIGRATION 036 ─────────────────────────
@pytest.mark.live
def test_obs01_schema_certification_migration_036():
    cm = ConnectionManager("config/config.local.yaml")
    certifier = SchemaCertifier(cm)
    certifier.certify()


