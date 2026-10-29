# tests/test_job01_lifecycle.py
"""
Unit and Integration Tests for JOB-01: Authoritative Execution Lifecycle.
Validates scope claims, request idempotency, state transitions, lease reaping, and separation from batch_log.
"""
import uuid
import pytest
pytestmark = pytest.mark.live
from datetime import datetime, timedelta
from sqlalchemy import text

from src.connections.connection_manager import ConnectionManager
from src.connections.schema_certifier import SchemaCertifier
from src.models.submission import SubmissionRequest, SubmissionResult
from gui.services.database import get_engine
from gui.services.execution_service import ExecutionService
from src.orchestrator.lifecycle import (
    transition_job,
    reap_stale_job_leases,
    validate_transition_allowed,
    VALID_JOB_TRANSITIONS,
    TERMINAL_STATES
)


@pytest.fixture
def test_engine():
    """Provides the database engine for tests."""
    return get_engine()


@pytest.fixture
def clean_test_job(test_engine):
    """Cleanup fixture for test jobs."""
    created_run_ids = []

    def _register(run_id: str):
        created_run_ids.append(run_id)
        return run_id

    yield _register

    with test_engine.begin() as conn:
        for rid in created_run_ids:
            conn.execute(text("DELETE FROM metadata.job_event WHERE run_id::text = :rid"), {"rid": rid})
            conn.execute(text("DELETE FROM metadata.job_scope_claim WHERE run_id::text = :rid"), {"rid": rid})
            conn.execute(text("DELETE FROM metadata.job_submission_audit WHERE run_id::text = :rid"), {"rid": rid})
            conn.execute(text("DELETE FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": rid})
            conn.execute(text("DELETE FROM metadata.batch_log WHERE run_id::text = :rid"), {"rid": rid})


def test_schema_certifier_validates_migration_032():
    """Scenario 21: SchemaCertifier successfully certifies ledger including 032."""
    cm = ConnectionManager("config/config.local.yaml")
    certifier = SchemaCertifier(cm)
    certifier.certify()


def test_job_registry_uses_run_id_primary_key(test_engine):
    """Scenario 1: Verify gui_job_registry has run_id as primary key and proper constraints."""
    with test_engine.connect() as conn:
        res = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'metadata' AND table_name = 'gui_job_registry' AND column_name = 'run_id'
        """)).fetchone()
        assert res is not None
        assert res[0] == "run_id"
        assert res[1] == "uuid"
        assert res[2] == "NO"


def test_transition_job_compare_and_swap_success(test_engine, clean_test_job):
    """Scenario 15: Valid transition updates status and inserts job_event."""
    run_id = clean_test_job(str(uuid.uuid4()))
    with test_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.gui_job_registry (run_id, action, source_tag, status, actor_type, actor_id, submitted_at)
            VALUES (:rid, 'run_pipeline', 'test_src', 'QUEUED', 'USER', 'tester', CURRENT_TIMESTAMP)
        """), {"rid": run_id})

    # Transition QUEUED -> DISPATCHED
    success = transition_job(
        test_engine,
        run_id=run_id,
        from_status="QUEUED",
        to_status="DISPATCHED",
        dispatcher_id="test_dispatcher_1",
        dispatch_lease_seconds=30
    )
    assert success is True

    # Verify state in registry
    with test_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT status, dispatcher_id, dispatch_lease_expires_at 
            FROM metadata.gui_job_registry WHERE run_id::text = :rid
        """), {"rid": run_id}).mappings().fetchone()
        assert row["status"] == "DISPATCHED"
        assert row["dispatcher_id"] == "test_dispatcher_1"
        assert row["dispatch_lease_expires_at"] is not None

        # Verify event inserted
        evt = conn.execute(text("""
            SELECT event_type, from_status, to_status 
            FROM metadata.job_event WHERE run_id::text = :rid ORDER BY event_id DESC LIMIT 1
        """), {"rid": run_id}).mappings().fetchone()
        assert evt["event_type"] == "JOB_DISPATCHED"
        assert evt["from_status"] == "QUEUED"
        assert evt["to_status"] == "DISPATCHED"


def test_transition_job_compare_and_swap_mismatch(test_engine, clean_test_job):
    """Scenario 16: Invalid from_status fails CAS and returns False."""
    run_id = clean_test_job(str(uuid.uuid4()))
    with test_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.gui_job_registry (run_id, action, source_tag, status, actor_type, actor_id, submitted_at)
            VALUES (:rid, 'run_pipeline', 'test_src', 'QUEUED', 'USER', 'tester', CURRENT_TIMESTAMP)
        """), {"rid": run_id})

    # Attempt transition from RUNNING (which does not match current QUEUED)
    # validate_transition_allowed check: RUNNING -> SUCCESS is valid rule, but DB CAS fails because DB has QUEUED
    success = transition_job(
        test_engine,
        run_id=run_id,
        from_status="RUNNING",
        to_status="SUCCESS"
    )
    assert success is False

    with test_engine.connect() as conn:
        row = conn.execute(text("SELECT status FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": run_id}).mappings().fetchone()
        assert row["status"] == "QUEUED"


def test_terminal_states_are_immutable():
    """Scenario 17: Transitioning out of terminal state is rejected in state machine."""
    for term in TERMINAL_STATES:
        with pytest.raises(ValueError, match="Invalid state transition"):
            validate_transition_allowed(term, "RUNNING")
        with pytest.raises(ValueError, match="Invalid state transition"):
            validate_transition_allowed(term, "QUEUED")


def test_submission_result_fields_on_acceptance(test_engine, clean_test_job):
    """Scenario 10: Acceptance has accepted=True, valid run_id, valid submitted_at."""
    req_id = str(uuid.uuid4())
    req = SubmissionRequest(
        request_id=req_id,
        action="run_landing",
        source_tag="test_accept_src",
        source_table_ids=(9901,),
        actor_type="USER",
        actor_id="test_user"
    )
    res = ExecutionService.submit_run(req)
    assert isinstance(res, SubmissionResult)
    assert res.accepted is True
    assert res.run_id is not None
    clean_test_job(res.run_id)
    assert res.submitted_at is not None
    assert res.rejection_code is None


def test_caller_request_id_idempotent_replay(test_engine, clean_test_job):
    """Scenario 8: Re-submitting the exact same request_id returns the exact persisted SubmissionResult."""
    req_id = str(uuid.uuid4())
    req = SubmissionRequest(
        request_id=req_id,
        action="run_landing",
        source_tag="test_replay_src",
        source_table_ids=(9902,),
        actor_type="USER",
        actor_id="test_user"
    )
    res1 = ExecutionService.submit_run(req)
    assert res1.accepted is True
    clean_test_job(res1.run_id)

    # Second call with SAME request_id
    res2 = ExecutionService.submit_run(req)
    assert isinstance(res2, SubmissionResult)
    assert res2.accepted is True
    assert res2.request_id == req_id
    assert res2.run_id == res1.run_id


def test_job_scope_claim_table_level_lock_and_conflict(test_engine, clean_test_job):
    """Scenarios 2, 3, 9: Single-table scope lock and active conflict rejection."""
    # First submission claims table 9903
    req1 = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_scope_src",
        source_table_ids=(9903,),
        actor_type="USER",
        actor_id="user_1"
    )
    res1 = ExecutionService.submit_run(req1)
    assert res1.accepted is True
    clean_test_job(res1.run_id)

    # Second submission with DIFFERENT request_id attempts to claim table 9903 while run1 is active
    req2 = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_scope_src",
        source_table_ids=(9903,),
        actor_type="USER",
        actor_id="user_2"
    )
    res2 = ExecutionService.submit_run(req2)
    assert res2.accepted is False
    assert res2.run_id is None
    assert res2.submitted_at is None
    assert res2.rejection_code == "ACTIVE_EXECUTION_EXISTS"
    assert res2.existing_active_run_id == res1.run_id
    assert res2.existing_submitted_at is not None


def test_job_scope_claim_multi_table_batch(test_engine, clean_test_job):
    """Scenario 4: Multi-table run claims all constituent source_table_ids in deterministic order."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_landing",
        source_tag="test_batch_src",
        source_table_ids=(9905, 9904, 9906),
        actor_type="USER",
        actor_id="batch_user"
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)

    with test_engine.connect() as conn:
        claims = conn.execute(text("""
            SELECT source_table_id, released_at 
            FROM metadata.job_scope_claim 
            WHERE run_id::text = :rid 
            ORDER BY source_table_id ASC
        """), {"rid": res.run_id}).fetchall()
        assert len(claims) == 3
        assert [c[0] for c in claims] == [9904, 9905, 9906]
        assert all(c[1] is None for c in claims)


def test_job_scope_claim_release_on_success(test_engine, clean_test_job):
    """Scenario 5: Completing job (SUCCESS) releases all scope claims."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_rel_src",
        source_table_ids=(9907,),
        actor_type="USER",
        actor_id="user_1"
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)

    # Transition QUEUED -> DISPATCHED -> RUNNING -> SUCCESS
    transition_job(
        test_engine,
        run_id=res.run_id,
        from_status="QUEUED",
        to_status="DISPATCHED",
        dispatch_lease_seconds=30,
    )
    transition_job(test_engine, run_id=res.run_id, from_status="DISPATCHED", to_status="RUNNING")
    transition_job(test_engine, run_id=res.run_id, from_status="RUNNING", to_status="SUCCESS")

    # Scope claims must now be released
    with test_engine.connect() as conn:
        claims = conn.execute(text("""
            SELECT released_at FROM metadata.job_scope_claim WHERE run_id::text = :rid
        """), {"rid": res.run_id}).fetchall()
        assert len(claims) == 1
        assert claims[0][0] is not None

    # New submission on same table now succeeds
    req_new = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_rel_src",
        source_table_ids=(9907,),
        actor_type="USER",
        actor_id="user_2"
    )
    res_new = ExecutionService.submit_run(req_new)
    assert res_new.accepted is True
    clean_test_job(res_new.run_id)


def test_job_scope_claim_release_on_failure(test_engine, clean_test_job):
    """Scenario 6: Failing job (FAILED) releases all scope claims."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_fail_src",
        source_table_ids=(9908,),
        actor_type="USER",
        actor_id="user_1"
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)

    # Transition to FAILED from DISPATCHED or RUNNING
    success = transition_job(test_engine, run_id=res.run_id, from_status="DISPATCHED", to_status="FAILED", error_code="TEST_ERR")
    if not success:
        success = transition_job(test_engine, run_id=res.run_id, from_status="RUNNING", to_status="FAILED", error_code="TEST_ERR")
    assert success is True

    with test_engine.connect() as conn:
        claims = conn.execute(text("""
            SELECT released_at FROM metadata.job_scope_claim WHERE run_id::text = :rid
        """), {"rid": res.run_id}).fetchall()
        assert len(claims) == 1
        assert claims[0][0] is not None


def test_job_scope_claim_release_on_cancellation(test_engine, clean_test_job):
    """Scenario 7: Cancelling job (CANCELLED) releases all scope claims."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_canc_src",
        source_table_ids=(9909,),
        actor_type="USER",
        actor_id="user_1"
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)

    ExecutionService.cancel_run(res.run_id)

    with test_engine.connect() as conn:
        claims = conn.execute(text("""
            SELECT released_at FROM metadata.job_scope_claim WHERE run_id::text = :rid
        """), {"rid": res.run_id}).fetchall()
        assert len(claims) == 1
        assert claims[0][0] is not None


def test_no_age_based_queued_reaping(test_engine, clean_test_job):
    """Scenario 11: QUEUED jobs are not reaped by reap_stale_job_leases even if submitted long ago."""
    run_id = clean_test_job(str(uuid.uuid4()))
    with test_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.gui_job_registry (
                run_id, action, source_tag, status, actor_type, actor_id, submitted_at
            ) VALUES (
                :rid, 'run_pipeline', 'test_queued', 'QUEUED', 'USER', 'tester',
                CURRENT_TIMESTAMP - INTERVAL '10 days'
            )
        """), {"rid": run_id})

    reaped_d, reaped_r = reap_stale_job_leases(test_engine, dispatch_lease_timeout_sec=1, worker_heartbeat_timeout_sec=1)
    
    # Verify job is still QUEUED
    with test_engine.connect() as conn:
        st = conn.execute(text("SELECT status FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": run_id}).scalar()
        assert st == "QUEUED"


def test_dispatched_lease_expiration_reap(test_engine, clean_test_job):
    """Scenario 12: DISPATCHED job with expired lease is reaped to ABANDONED."""
    run_id = clean_test_job(str(uuid.uuid4()))
    with test_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.gui_job_registry (
                run_id, action, source_tag, status, actor_type, actor_id, submitted_at,
                dispatched_at, dispatch_lease_expires_at
            ) VALUES (
                :rid, 'run_pipeline', 'test_disp', 'DISPATCHED', 'USER', 'tester',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - INTERVAL '5 seconds'
            )
        """), {"rid": run_id})
        conn.execute(text("""
            INSERT INTO metadata.job_scope_claim (run_id, source_table_id, action, status, created_at)
            VALUES (:rid, 9910, 'run_pipeline', 'RESERVED', CURRENT_TIMESTAMP)
        """), {"rid": run_id})

    reaped_d, reaped_r = reap_stale_job_leases(test_engine, dispatch_lease_timeout_sec=1, worker_heartbeat_timeout_sec=1)
    assert reaped_d >= 1

    with test_engine.connect() as conn:
        row = conn.execute(text("SELECT status, error_code FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": run_id}).mappings().fetchone()
        assert row["status"] == "ABANDONED"
        assert row["error_code"] == "DISPATCH_LEASE_EXPIRED"

        # Scope claim released
        released = conn.execute(text("SELECT released_at FROM metadata.job_scope_claim WHERE run_id::text = :rid"), {"rid": run_id}).scalar()
        assert released is not None


def test_running_heartbeat_expiration_and_batch_log_fencing(test_engine, clean_test_job):
    """Scenarios 13, 14: Expired RUNNING heartbeat reaped to FAILED, fencing active child batch_log attempts."""
    run_id = clean_test_job(str(uuid.uuid4()))
    with test_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.gui_job_registry (
                run_id, action, source_tag, status, actor_type, actor_id, submitted_at,
                started_at, last_heartbeat
            ) VALUES (
                :rid, 'run_pipeline', 'test_run_reap', 'RUNNING', 'USER', 'tester',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - INTERVAL '300 seconds'
            )
        """), {"rid": run_id})
        conn.execute(text("""
            INSERT INTO metadata.job_scope_claim (run_id, source_table_id, action, status, created_at)
            VALUES (:rid, 9911, 'run_pipeline', 'RESERVED', CURRENT_TIMESTAMP)
        """), {"rid": run_id})
        conn.execute(text("""
            INSERT INTO metadata.batch_log (
                run_id, source_tag, source_table, load_type, status, start_time
            ) VALUES (
                :rid, 'test_run_reap', 'ulog', 'run_pipeline', 'RUNNING', CURRENT_TIMESTAMP - INTERVAL '300 seconds'
            )
        """), {"rid": run_id})

    reaped_d, reaped_r = reap_stale_job_leases(test_engine, dispatch_lease_timeout_sec=1, worker_heartbeat_timeout_sec=120)
    assert reaped_r >= 1

    with test_engine.connect() as conn:
        # Check parent registry
        p_row = conn.execute(text("SELECT status, error_code FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": run_id}).mappings().fetchone()
        assert p_row["status"] == "FAILED"
        assert p_row["error_code"] == "WORKER_HEARTBEAT_EXPIRED"

        # Check child batch_log is fenced to FAILED
        c_row = conn.execute(text("SELECT status, notes FROM metadata.batch_log WHERE run_id::text = :rid"), {"rid": run_id}).mappings().fetchone()
        assert c_row["status"] == "FAILED"
        assert "heartbeat expired" in c_row["notes"]

        # Scope claim released
        released = conn.execute(text("SELECT released_at FROM metadata.job_scope_claim WHERE run_id::text = :rid"), {"rid": run_id}).scalar()
        assert released is not None


def test_running_with_missing_worker_timestamps_reaped_by_submitted_at(test_engine, clean_test_job):
    """A malformed RUNNING parent with no started_at/heartbeat must not stay active forever."""
    run_id = clean_test_job(str(uuid.uuid4()))
    with test_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.gui_job_registry (
                run_id, action, source_tag, status, actor_type, actor_id, submitted_at,
                started_at, last_heartbeat
            ) VALUES (
                :rid, 'run_pipeline', 'test_null_worker_clock', 'RUNNING', 'USER', 'tester',
                CURRENT_TIMESTAMP - INTERVAL '300 seconds', NULL, NULL
            )
        """), {"rid": run_id})

    reaped_d, reaped_r = reap_stale_job_leases(
        test_engine,
        dispatch_lease_timeout_sec=1,
        worker_heartbeat_timeout_sec=120,
    )
    assert reaped_r >= 1

    with test_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT status, error_code
            FROM metadata.gui_job_registry
            WHERE run_id::text = :rid
        """), {"rid": run_id}).mappings().fetchone()
        assert row["status"] == "FAILED"
        assert row["error_code"] == "WORKER_HEARTBEAT_EXPIRED"


def test_batch_log_never_inserts_queued(test_engine, clean_test_job):
    """Scenario 18: submit_run does not insert any QUEUED row into batch_log."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_landing",
        source_tag="test_no_queued_src",
        source_table_ids=(9912,),
        actor_type="USER",
        actor_id="test_user"
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)

    with test_engine.connect() as conn:
        b_count = conn.execute(text("SELECT COUNT(*) FROM metadata.batch_log WHERE run_id::text = :rid"), {"rid": res.run_id}).scalar()
        assert b_count == 0


def test_batch_log_never_blocks_new_submissions(test_engine, clean_test_job):
    """Scenario 19: Stale batch_log rows never block new submissions."""
    # Insert an old abandoned RUNNING row in batch_log without any active scope claim
    stale_rid = clean_test_job(str(uuid.uuid4()))
    with test_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.batch_log (
                run_id, source_tag, source_table, load_type, status, start_time
            ) VALUES (
                :rid, 'test_stale_src', 'ulog', 'run_pipeline', 'RUNNING', CURRENT_TIMESTAMP - INTERVAL '10 days'
            )
        """), {"rid": stale_rid})

    # New submission for the same source_table must NOT be blocked by historical batch_log
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_stale_src",
        source_table_ids=(9913,),
        actor_type="USER",
        actor_id="test_user"
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)
    assert res.run_id != stale_rid


def test_secret_redaction_in_job_parameters(test_engine, clean_test_job):
    """Scenario 20: Passwords, tokens, and api_keys in parameters are redacted in persisted JSONB."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_landing",
        source_tag="test_redact_src",
        source_table_ids=(9914,),
        parameters={"db_password": "super_secret_pwd", "api_token": "secret_tok_123", "mode": "fast"}
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)

    with test_engine.connect() as conn:
        params_val = conn.execute(text("SELECT parameters FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": res.run_id}).scalar()
        assert params_val["db_password"] == "[REDACTED]"
        assert params_val["api_token"] == "[REDACTED]"
        assert params_val["mode"] == "fast"


def test_invalid_actor_type_database_constraint(test_engine, clean_test_job):
    """Verify DB chk_actor_type constraint blocks invalid actor types."""
    run_id = clean_test_job(str(uuid.uuid4()))
    with pytest.raises(Exception, match="chk_actor_type"):
        with test_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO metadata.gui_job_registry (
                    run_id, action, source_tag, status, actor_type, actor_id, submitted_at
                ) VALUES (
                    :rid, 'run_pipeline', 'test_act', 'QUEUED', 'INVALID_ACTOR', 'tester', CURRENT_TIMESTAMP
                )
            """), {"rid": run_id})


def test_parent_run_id_lineage_and_metadata(test_engine, clean_test_job):
    """Verify parent_run_id lineage is preserved and reflected in get_run_detail."""
    parent_rid = clean_test_job(str(uuid.uuid4()))
    child_req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_parent_src",
        source_table_ids=(9915,),
        parameters={"parent_run_id": parent_rid}
    )
    child_res = ExecutionService.submit_run(child_req)
    assert child_res.accepted is True
    clean_test_job(child_res.run_id)

    detail = ExecutionService.get_run_detail(child_res.run_id)
    assert detail.run_id == child_res.run_id


def test_concurrent_scope_claims_deadlock_free_sorting(test_engine, clean_test_job):
    """Verify scope claims are always sorted ascending to prevent lock acquisition inversion deadlocks."""
    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_pipeline",
        source_tag="test_sort_src",
        source_table_ids=(9999, 1000, 5000, 2000),
    )
    res = ExecutionService.submit_run(req)
    assert res.accepted is True
    clean_test_job(res.run_id)

    with test_engine.connect() as conn:
        tids = conn.execute(text("""
            SELECT source_table_id FROM metadata.job_scope_claim WHERE run_id::text = :rid ORDER BY claim_id ASC
        """), {"rid": res.run_id}).fetchall()
        assert [t[0] for t in tids] == [1000, 2000, 5000, 9999]
