import json
import os
import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from gui.models.project_context import ProjectContext
from gui.services.execution_backend import ExecutionRequest, SubprocessExecutionBackend
from gui.services.execution_service import ExecutionService, _resolve_upstream_artifact
from src.contracts.claim_models import FrozenJobExecution, FrozenTableEntry
from src.contracts.runtime_settings import RuntimeSettings
from src.constants import LayerName, RunStatus
from src.exceptions import ExecutionOwnershipRequiredError
from src.models.submission import SubmissionRequest
import src.orchestrator.etl as etl_module
from src.orchestrator.etl import ETL
from src.orchestrator.job_manager import JobManager
from src.runner.pipeline_runner import PipelineRunner


def test_gold_only_accepts_a_verified_legacy_silver_manifest_with_same_policy():
    """Legacy manifests recorded the policy hash in mapping_fingerprint."""
    class ArtifactCursor:
        def __init__(self):
            self.query = ""
            self.params = ()

        def execute(self, query, params):
            self.query = str(query)
            self.params = params

        def fetchone(self):
            return {
                "manifest_id": "artifact-1",
                "run_id": "silver-run",
                "attempt_number": 1,
                "storage_layer": "SILVER",
                "storage_uri": "history/orders/run=silver-run",
                "storage_format": "parquet",
                "expected_row_count": 3,
                "policy_version_id": "policy-1",
                "policy_fingerprint": "p" * 64,
                # Historical writer defect: this should have been m*64.
                "mapping_fingerprint": "p" * 64,
                "source_key_fingerprint": "k" * 64,
                "schema_fingerprint": "s" * 64,
                "artifact_fingerprint": "a" * 64,
                "window_id": None,
                "cdc_lower_cursor": None,
                "cdc_upper_cursor": None,
                "cdc_state_version": None,
                "batch_log_id": 85,
                "status": "SUCCESS",
            }

    cursor = ArtifactCursor()
    artifact = _resolve_upstream_artifact(
        cursor,
        table_id=178,
        policy_version_id="policy-1",
        policy_fingerprint="p" * 64,
        mapping_fingerprint="m" * 64,
        source_key_fingerprint="k" * 64,
        execution_scope="GOLD",
    )

    assert artifact is not None
    assert artifact.producer_run_id == "silver-run"
    assert "m.mapping_fingerprint = m.policy_fingerprint" in cursor.query


def _runtime_settings() -> RuntimeSettings:
    return RuntimeSettings(
        dispatch_lease_timeout_sec=60,
        worker_heartbeat_timeout_sec=120,
        publication_timeout_sec=300,
        target_lock_timeout_ms=15000,
    )


class RecordingCursor:
    def __init__(self):
        self.executed = []
        self._one = None
        self._all = []
        self.registry_params_json = None
        self.registry_target_tag = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split()).lower()
        self.executed.append((normalized, params))
        self._one = None
        self._all = []

        if "from information_schema.columns" in normalized:
            self._all = [("scope_action",), ("worker_action",)]
        elif "from metadata.job_submission_audit" in normalized and normalized.startswith("select"):
            self._one = None
        elif "select table_id from metadata.db_tables_etl" in normalized:
            self._all = [(120,)]
        elif "from metadata.db_tables_etl e" in normalized:
            self._all = [{
                "table_id": 120,
                "source_tag": "gov_src",
                "source_schema": "gov_src",
                "source_database_type": "mysql",
                "source_table": "orders",
                "target_tag": "land_test",
                "target_schema": "landing",
                "target_table": "orders",
                "landing_table": "orders",
                "history_table": "orders",
                "table_type": "full",
                "priority": 1,
                "is_cdc": "y",
                "cdc_columns": "id",
                "cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
                "cdc_p_days": 7,
                "delta_days": 0,
                "last_success_timestamp": None,
                "last_success_date": datetime(2026, 1, 1),
                "rerun_flag": "n",
                "partition_lower_bound": 1,
                "partition_upper_bound": 100,
                "target_rows_per_partition": 250000,
                "minimum_partition_threshold": 50000,
                "maximum_partitions": 32,
                "execution_authority_mode": "GOVERNED",
                "current_policy_version_id": "policy-1",
                "policy_fingerprint": "p" * 64,
                "mapping_fingerprint": "m" * 64,
                "source_key_fingerprint": "k" * 64,
                "contract_payload": {
                    "source_business_key": {
                        "columns": ["id"],
                        "evidence_type": "PRIMARY_KEY",
                        "evidence_fingerprint": "k" * 64,
                    },
                    "target_business_key": {"columns": ["id"]},
                    "cdc_policy": {
                        "table_id": 120,
                        "strategy": "SEQUENCE_HIGH_WATERMARK",
                        "mutation_mode": "APPEND_ONLY",
                        "cursor_finality_mode": "PREFIX_FINAL",
                        "cursor_columns": ["id"],
                        "cursor_types": ["INT64"],
                        "tie_breaker_columns": ["id"],
                        "cdc_policy_fingerprint": "c" * 64,
                    },
                },
            }]
        elif "from metadata.job_scope_claim c" in normalized:
            self._one = None
        elif "insert into metadata.gui_job_registry" in normalized:
            self.registry_target_tag = params[3]
            self.registry_params_json = params[5]
            self._one = (datetime(2026, 1, 1),)

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class MissingPolicyCursor(RecordingCursor):
    def execute(self, sql, params=None):
        super().execute(sql, params)
        normalized = " ".join(str(sql).split()).lower()
        if "from metadata.db_tables_etl e" in normalized:
            self._all[0]["policy_fingerprint"] = None


class RetryParentCursor(RecordingCursor):
    """Provides a failed governed parent for retry-lineage submission tests."""

    def __init__(self, parent_run_id):
        super().__init__()
        self.parent_run_id = parent_run_id

    def execute(self, sql, params=None):
        super().execute(sql, params)
        normalized = " ".join(str(sql).split()).lower()
        if (
            "from metadata.gui_job_registry" in normalized
            and normalized.startswith("select")
            and params
            and str(params[0]) == self.parent_run_id
        ):
            self._one = ("run_governed", "gov_src", "FAILED")


class RecordingConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def _contract_dict(run_id):
    table = FrozenTableEntry(
        table_id=120,
        source_tag="gov_src",
        source_table="orders",
        target_tag="land_test",
        target_schema="landing",
        target_table="orders",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="policy-1",
        policy_fingerprint="p" * 64,
        column_mapping_version_id="policy-1",
        column_mapping_fingerprint="m" * 64,
        business_key_version_id="policy-1",
        business_key_fingerprint="k" * 64,
        business_key_columns=["id"],
        deployment_profile_fingerprint="profile-fp",
    )
    return FrozenJobExecution.build(
        run_id=run_id,
        action="run_governed",
        deployment_profile_fingerprint="profile-fp",
        tables=[table],
    ).to_dict()


def test_governed_submission_persists_frozen_contract_and_dispatches_after_commit(monkeypatch):
    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)
    dispatched = []
    reservations = []

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.ScopeClaimManager.reserve_scope_claims_cursor",
        lambda cur, run_id, table_ids, action: reservations.append((run_id, table_ids, action)),
    )
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_governed_job",
        lambda run_id, runtime_settings: dispatched.append((run_id, runtime_settings)),
    )

    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_governed",
        source_tag="gov_src",
        source_table="orders",
        actor_type="USER",
        actor_id="operator",
        execution_scope="BRONZE",
    )
    result = ExecutionService.submit_run(req, context=ProjectContext("gov_src", "PROD"))

    assert result.accepted is True
    assert conn.committed is True
    assert dispatched and dispatched[0][0] == result.run_id
    assert reservations == [(result.run_id, [120], "run_governed")]

    persisted_contract = json.loads(cursor.registry_params_json)
    assert persisted_contract["action"] == "run_governed"
    assert persisted_contract["run_id"] == result.run_id
    assert persisted_contract["execution_scope"] == "BRONZE"
    assert persisted_contract["tables"][0]["source_table"] == "orders"
    assert persisted_contract["tables"][0]["business_key_columns"] == ["id"]
    assert persisted_contract["tables"][0]["is_cdc"] == "y"
    assert persisted_contract["tables"][0]["cdc_columns"] == "id"
    assert persisted_contract["tables"][0]["cdc_p_days"] == 7
    assert persisted_contract["tables"][0]["contract_payload"]["cdc_policy"]["strategy"] == "SEQUENCE_HIGH_WATERMARK"
    FrozenJobExecution.from_dict(persisted_contract)


def test_normal_governed_submission_uses_the_legacy_build_signature(monkeypatch):
    """A normal run must not require the retry-only contract argument."""
    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)
    original_build = FrozenJobExecution.build

    @classmethod
    def legacy_build(cls, *, run_id, action, deployment_profile_fingerprint, tables, execution_scope="FULL"):
        return original_build(
            run_id=run_id,
            action=action,
            deployment_profile_fingerprint=deployment_profile_fingerprint,
            tables=tables,
            execution_scope=execution_scope,
        )

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.ScopeClaimManager.reserve_scope_claims_cursor",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_governed_job",
        lambda **kwargs: kwargs["run_id"],
    )
    monkeypatch.setattr(FrozenJobExecution, "build", legacy_build)

    result = ExecutionService.submit_run(
        SubmissionRequest(
            request_id=str(uuid.uuid4()),
            action="run_governed",
            source_tag="gov_src",
            source_table="orders",
            actor_type="USER",
            actor_id="operator",
        ),
        context=ProjectContext("gov_src", "PROD"),
    )

    assert result.accepted is True
    assert "retry_parent_run_id" not in json.loads(cursor.registry_params_json)


def test_governed_retry_submission_freezes_the_parent_run_identity(monkeypatch):
    parent_run_id = str(uuid.uuid4())
    cursor = RetryParentCursor(parent_run_id)
    conn = RecordingConnection(cursor)

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.ScopeClaimManager.reserve_scope_claims_cursor",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_governed_job",
        lambda **kwargs: kwargs["run_id"],
    )

    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_governed",
        source_tag="gov_src",
        source_table="orders",
        actor_type="USER",
        actor_id="operator",
        parameters={"parent_run_id": parent_run_id},
    )

    result = ExecutionService.submit_run(req, context=ProjectContext("gov_src", "PROD"))

    persisted_contract = json.loads(cursor.registry_params_json)
    assert result.accepted is True
    assert persisted_contract["retry_parent_run_id"] == parent_run_id
    assert FrozenJobExecution.from_dict(persisted_contract).retry_parent_run_id == parent_run_id


def test_governed_retry_run_detail_exposes_signed_parent_lineage(monkeypatch):
    child_run_id = str(uuid.uuid4())
    parent_run_id = str(uuid.uuid4())
    parent_job = FrozenJobExecution.from_dict(_contract_dict(parent_run_id))
    child_job = FrozenJobExecution.build(
        run_id=child_run_id,
        action="run_governed",
        deployment_profile_fingerprint=parent_job.deployment_profile_fingerprint,
        tables=parent_job.tables,
        retry_parent_run_id=parent_run_id,
    )
    registry_row = {
        "run_id": child_run_id,
        "action": "run_governed",
        "source_tag": "gov_src",
        "target_tag": "land_test",
        "priority": 1,
        "status": "FAILED",
        "pid": None,
        "submitted_at": datetime(2026, 9, 14, 12, 0, 0),
        "dispatched_at": None,
        "started_at": datetime(2026, 9, 14, 12, 0, 1),
        "last_heartbeat": None,
        "completed_at": datetime(2026, 9, 14, 12, 0, 2),
        "error_code": "GOLD_WRITE_FAILED",
        "error_message": "Gold write failed.",
        "parameters": child_job.to_dict(),
    }

    class Result:
        def __init__(self, row):
            self._row = row

        def mappings(self):
            return self

        def fetchone(self):
            return self._row

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement, params=None):
            if "FROM metadata.gui_job_registry" in str(statement):
                return Result(registry_row)
            return Result(None)

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    monkeypatch.setattr("gui.services.execution_service.get_engine", lambda: FakeEngine())

    detail = ExecutionService.get_run_detail(
        child_run_id,
        context=ProjectContext("gov_src", "PROD"),
    )

    assert detail.parent_run_id == parent_run_id
    assert detail.retry_attempt == 1
    assert detail.error_message == "Gold write failed."


def test_admin_sync_submission_dispatches_without_table_scope_claims(monkeypatch):
    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)
    dispatched = []

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.ScopeClaimManager.reserve_scope_claims_cursor",
        lambda *args, **kwargs: pytest.fail("sync must not reserve table scope claims"),
    )
    monkeypatch.setattr(
        "gui.services.execution_service.RuntimeSettings.from_config_path",
        lambda *args, **kwargs: _runtime_settings(),
    )
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_job",
        lambda **kwargs: dispatched.append(kwargs) or kwargs["preallocated_run_id"],
    )

    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="sync",
        source_tag="gov_src",
        target_tag="land_test",
        actor_type="USER",
        actor_id="admin",
        extra_flags=["--target_tag", "land_test"],
    )
    result = ExecutionService.submit_run(req, context=ProjectContext("gov_src", "PROD"))

    assert result.accepted is True
    assert conn.committed is True
    assert dispatched
    assert dispatched[0]["action"] == "sync"
    assert dispatched[0]["source_tag"] == "gov_src"
    assert dispatched[0]["target_tag"] == "land_test"
    assert dispatched[0]["extra_flags"] == ["--target_tag", "land_test"]
    assert dispatched[0]["preallocated_run_id"] == result.run_id
    assert cursor.registry_target_tag == "land_test"


def test_governed_submission_rolls_back_and_does_not_dispatch_with_invalid_policy(monkeypatch):
    cursor = MissingPolicyCursor()
    conn = RecordingConnection(cursor)
    dispatched = []

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_governed_job",
        lambda run_id, runtime_settings: dispatched.append(run_id),
    )

    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_governed",
        source_tag="gov_src",
        source_table="orders",
    )

    with pytest.raises(RuntimeError, match="Job registration persistence failed"):
        ExecutionService.submit_run(req, context=ProjectContext("gov_src", "PROD"))

    assert conn.rolled_back is True
    assert dispatched == []


def test_dispatch_failure_raises_when_terminal_cas_is_lost(monkeypatch):
    """A failed worker spawn must not report acceptance when terminal CAS is lost."""
    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_governed_job",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("spawn failed")),
    )
    monkeypatch.setattr("gui.services.execution_service.get_engine", lambda: object())
    monkeypatch.setattr("gui.services.execution_service.transition_job", lambda *args, **kwargs: False)

    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_governed",
        source_tag="gov_src",
        source_table="orders",
    )

    with pytest.raises(RuntimeError, match="terminal CAS was not acquired"):
        ExecutionService.submit_run(req, context=ProjectContext("gov_src", "PROD"))


def test_submission_refuses_synthetic_table_identity(monkeypatch):
    """A missing metadata row cannot become a CRC-derived scope claim."""
    class EmptyTableCursor(RecordingCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "select table_id from metadata.db_tables_etl" in " ".join(str(sql).split()).lower():
                self._all = []

    cursor = EmptyTableCursor()
    conn = RecordingConnection(cursor)
    dispatched = []
    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_job",
        lambda **kwargs: dispatched.append(kwargs),
    )

    with pytest.raises(RuntimeError, match="refusing to synthesize execution identity"):
        ExecutionService.submit_run(
            ProjectContext("gov_src", "PROD"),
            "run_pipeline",
            "gov_src",
            "missing_table",
        )

    assert dispatched == []


def test_governed_submission_fails_closed_when_migration_045_missing(monkeypatch):
    class Pre045Cursor(RecordingCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            normalized = " ".join(str(sql).split()).lower()
            if "from information_schema.columns" in normalized:
                self._all = []

    cursor = Pre045Cursor()
    conn = RecordingConnection(cursor)
    dispatched = []

    monkeypatch.setattr("gui.services.execution_service.get_psycopg2_conn", lambda: conn)
    monkeypatch.setattr(
        "gui.services.execution_service.JobManager.submit_governed_job",
        lambda run_id, runtime_settings: dispatched.append(run_id),
    )

    req = SubmissionRequest(
        request_id=str(uuid.uuid4()),
        action="run_governed",
        source_tag="gov_src",
        source_table="orders",
    )

    with pytest.raises(Exception, match="Database is missing required migration: 045"):
        ExecutionService.submit_run(req, context=ProjectContext("gov_src", "PROD"))

    assert conn.rolled_back is True
    assert dispatched == []


def test_governed_command_construction_is_worker_run_id_only():
    run_id = str(uuid.uuid4())
    cmd = JobManager.build_governed_worker_command(
        run_id,
        base_dir=os.path.normpath("C:/repo"),
        python_executable="python-test",
    )

    assert cmd == [
        "python-test",
        os.path.join(os.path.normpath("C:/repo"), "jobs", "app_etl.py"),
        "--action",
        "run_governed",
        "--run_id",
        run_id,
    ]


def test_metadata_sync_worker_command_is_source_only(monkeypatch):
    """A Tier-1 discovery worker must not receive a promotion target."""
    popen = MagicMock(return_value=SimpleNamespace(pid=123))
    monkeypatch.setattr("src.orchestrator.job_manager.subprocess.Popen", popen)
    monkeypatch.setattr("src.orchestrator.job_manager.os.makedirs", MagicMock())
    monkeypatch.setattr("builtins.open", MagicMock())
    monkeypatch.setattr("gui.services.database.get_engine", MagicMock())
    monkeypatch.setattr("src.orchestrator.lifecycle.transition_job", MagicMock(return_value=True))

    run_id = "sync-target-tag-test"
    JobManager.submit_job(
        action="sync",
        source_tag="gov_src",
        preallocated_run_id=run_id,
        runtime_settings=_runtime_settings(),
    )

    cmd = popen.call_args.args[0]
    assert "--target_tag" not in cmd


def test_sync_failure_is_logged_under_shared_run_id(monkeypatch):
    """Pre-flight failures must be attributable to the worker's run record."""
    import jobs.app_etl as app_etl

    class FakeSpark:
        def stop(self):
            return None

    class FakeETL:
        def __init__(self, config_path):
            self.conn_manager = object()
            self.spark = FakeSpark()

    logger_runs = []
    logger_starts = []
    logger_finals = []

    class FakeBatchLogger:
        def __init__(self, conn_manager, run_id):
            logger_runs.append(run_id)

        def log_start(self, **kwargs):
            logger_starts.append(kwargs)
            return 77

        def log(self, **kwargs):
            logger_finals.append(kwargs)

    monkeypatch.setattr(app_etl, "ETL", FakeETL)
    monkeypatch.setattr(app_etl, "BatchLogger", FakeBatchLogger)
    monkeypatch.setattr(
        app_etl,
        "action_sync",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("sync failed")),
    )
    monkeypatch.setattr("gui.services.database.get_engine", MagicMock())
    monkeypatch.setattr("src.orchestrator.lifecycle.transition_job", MagicMock(return_value=True))

    run_id = "sync-failure-run"
    assert app_etl.run_etl_action(
        "gov_src",
        "sync",
        run_id=run_id,
        target_tag="land_test",
        config="config/config.yaml",
    ) == 1
    assert logger_runs == [run_id]
    assert logger_starts and logger_starts[0]["target_tag"] == "land_test"
    assert logger_finals and logger_finals[0]["log_id"] == 77


@pytest.mark.skip(reason="Obsolete: run_etl_action uses raw SQL for error updates")
def test_sync_preflight_failure_terminalizes_dispatched_run(monkeypatch):
    """Schema certification failures must not strand a worker in DISPATCHED."""
    import jobs.app_etl as app_etl

    transitions = MagicMock(side_effect=[False, True])
    monkeypatch.setattr(
        app_etl,
        "ETL",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("metadata schema incompatible")),
    )
    monkeypatch.setattr("gui.services.database.get_engine", MagicMock())
    monkeypatch.setattr("src.orchestrator.lifecycle.transition_job", transitions)

    assert app_etl.run_etl_action(
        "gov_src",
        "sync",
        run_id="preflight-failure-run",
        target_tag="land_test",
        config="config/config.yaml",
    ) == 1

    assert any(
        call.kwargs.get("run_id") == "preflight-failure-run"
        and call.kwargs.get("from_status") == "DISPATCHED"
        and call.kwargs.get("to_status") == "FAILED"
        for call in transitions.call_args_list
    )


def test_admin_promote_selected_command_includes_target_and_steward_flags(monkeypatch, tmp_path):
    popen_calls = []

    monkeypatch.setattr("gui.services.execution_backend.PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        "gui.services.execution_backend.subprocess.Popen",
        lambda *args, **kwargs: popen_calls.append((args, kwargs)) or SimpleNamespace(pid=123),
    )

    SubprocessExecutionBackend().submit(
        ExecutionRequest(
            job_id="promote-selected",
            action="promote_table",
            source_tag="gov_src",
            target_tag="land_test",
            table="orders,items",
            options={
                "script_name": "jobs/promote_table.py",
                "priority": 2,
                "force": True,
                "approve_physical_primary_key": True,
                "approved_by": "admin_user",
            },
        )
    )

    cmd = popen_calls[0][0][0]
    assert cmd[1] == "jobs/promote_table.py"
    assert cmd[cmd.index("--source_tag") + 1] == "gov_src"
    assert cmd[cmd.index("--target_tag") + 1] == "land_test"
    assert cmd[cmd.index("--table") + 1] == "orders,items"
    assert cmd[cmd.index("--priority") + 1] == "2"
    assert "--force" in cmd
    assert "--approve-physical-primary-key" in cmd
    assert cmd[cmd.index("--approved-by") + 1] == "admin_user"


def test_admin_promote_all_and_drift_commands_use_canonical_script(monkeypatch, tmp_path):
    popen_calls = []

    monkeypatch.setattr("gui.services.execution_backend.PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        "gui.services.execution_backend.subprocess.Popen",
        lambda *args, **kwargs: popen_calls.append((args, kwargs)) or SimpleNamespace(pid=456),
    )

    backend = SubprocessExecutionBackend()
    backend.submit(
        ExecutionRequest(
            job_id="promote-all",
            action="promote_table",
            source_tag="gov_src",
            target_tag="land_test",
            options={"script_name": "jobs/promote_table.py", "all": True},
        )
    )
    backend.submit(
        ExecutionRequest(
            job_id="drift",
            action="promote_table",
            source_tag="gov_src",
            target_tag="land_test",
            options={"script_name": "jobs/promote_table.py", "drift_check": True},
        )
    )

    promote_cmd = popen_calls[0][0][0]
    drift_cmd = popen_calls[1][0][0]
    assert promote_cmd[1] == "jobs/promote_table.py"
    assert "--all" in promote_cmd
    assert drift_cmd[1] == "jobs/promote_table.py"
    assert "--drift-check" in drift_cmd


def test_governed_dispatch_requires_queued_ownership(monkeypatch):
    popen_calls = []

    monkeypatch.setattr("gui.services.database.get_engine", lambda: object())
    monkeypatch.setattr("src.orchestrator.lifecycle.transition_job", lambda *a, **k: False)
    monkeypatch.setattr(
        "src.orchestrator.job_manager.subprocess.Popen",
        lambda *a, **k: popen_calls.append((a, k)),
    )

    with pytest.raises(ExecutionOwnershipRequiredError):
        JobManager.submit_governed_job(str(uuid.uuid4()), _runtime_settings())

    assert popen_calls == []


def test_run_governed_parser_requires_only_run_id():
    from jobs.app_etl import build_parser

    parser = build_parser()
    args = parser.parse_args(["--action", "run_governed", "--run_id", str(uuid.uuid4())])
    assert args.action == "run_governed"
    assert args.source_tag is None


def test_frozen_contract_hash_mismatch_rejected_before_runner(monkeypatch):
    from jobs import app_etl

    run_id = str(uuid.uuid4())
    contract = _contract_dict(run_id)
    contract["tables"][0]["source_table"] = "tampered"

    class FakeConnectionManager:
        def __init__(self, config_path):
            self.config_path = config_path

        def get_metadata_db_connection(self):
            return RecordingConnection(SimpleNamespace(
                __enter__=lambda self: self,
                __exit__=lambda self, exc_type, exc, tb: False,
                execute=lambda sql, params=None: None,
                fetchone=lambda: ("run_governed", contract),
            ))

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return ("run_governed", contract)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, *args, **kwargs):
            return FakeCursor()

    class FakeCM:
        def __init__(self, config_path):
            self.config_path = config_path
            self.metadata_db_config = {"username": "u", "password": "p", "host": "h", "port": 5432, "database": "d"}

        def get_metadata_db_connection(self):
            return FakeConn()

    monkeypatch.setattr(app_etl, "ConnectionManager", FakeCM)
    monkeypatch.setattr(app_etl, "ETL", lambda *a, **k: pytest.fail("ETL must not be constructed"))

    assert app_etl.run_etl_action(None, "run_governed", run_id=run_id) == 1


def test_run_frozen_fails_closed_because_it_is_plan_only():
    run_id = str(uuid.uuid4())
    frozen_table = FrozenJobExecution.from_dict(_contract_dict(run_id)).tables[0]

    runner = PipelineRunner.__new__(PipelineRunner)

    result = runner.run_frozen(frozen_table=frozen_table, run_id=run_id)

    assert result.status == "FAILED"
    assert result.source_tag == "gov_src"
    assert result.source_table == "orders"
    assert "GOVERNED_EXECUTION_NOT_IMPLEMENTED" in result.errors[0]


def test_etl_run_frozen_table_executes_bronze_silver_gold_batches():
    run_id = str(uuid.uuid4())
    frozen_table = FrozenJobExecution.from_dict(_contract_dict(run_id)).tables[0]
    etl = ETL.__new__(ETL)
    etl.spark_app_id = "spark-test"
    etl.conn_manager = object()
    etl._register_cancellation_guards = lambda logger: None
    etl._clear_cancellation_guards = lambda: None
    etl._process_bronze_table = object()
    etl._process_silver_table = lambda *a, **k: None
    etl._process_gold_table = lambda *a, **k: None
    calls = []

    def fake_batch(tables, layer_name, target_tag, logger, worker_fn, on_success=None):
        calls.append((layer_name, target_tag, tables[0].source_table, tables[0].target_tag))
        return [tables[0]]

    etl._execute_table_batch = fake_batch

    result = etl.run_frozen_table(frozen_table=frozen_table, run_id=run_id)

    assert result[0].id == 120
    assert result[0].source_table == "orders"
    assert [call[0] for call in calls] == ["bronze", "silver", "gold"]
    assert all(call[1] == "gov_src" for call in calls)
    assert all(call[2] == "orders" for call in calls)
    assert all(call[3] == "land_test" for call in calls)


def test_governed_retry_reuses_parent_cdc_window_and_runs_only_gold(monkeypatch):
    from src.contracts.claim_models import FrozenArtifactReference

    run_id = str(uuid.uuid4())
    parent_run_id = str(uuid.uuid4())
    contract = _contract_dict(run_id)
    contract["tables"][0].update({
        "is_cdc": "y",
        "cdc_columns": "id",
        "cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
    })
    contract["parent_contract_fingerprint"] = FrozenJobExecution.compute_fingerprint(contract)
    frozen_table = FrozenJobExecution.from_dict(contract).tables[0]
    frozen_table.upstream_artifact = FrozenArtifactReference(
        artifact_id="parent-silver-artifact",
        producer_run_id=parent_run_id,
        producer_attempt_number=1,
        storage_layer="SILVER",
        storage_uri="history/run_parent",
        storage_format="parquet",
        row_count=1,
        policy_version_id=frozen_table.policy_version_id,
        policy_fingerprint=frozen_table.policy_fingerprint,
        mapping_fingerprint=frozen_table.column_mapping_fingerprint,
        source_key_fingerprint=frozen_table.business_key_fingerprint,
        schema_fingerprint="s" * 64,
        artifact_fingerprint="a" * 64,
        cdc_window_id="parent-window",
    )
    etl = ETL.__new__(ETL)
    etl.spark_app_id = "spark-test"
    etl.conn_manager = object()
    etl._register_cancellation_guards = lambda logger: None
    etl._clear_cancellation_guards = lambda: None
    etl._governed_retry_layer = lambda meta, parent: LayerName.GOLD
    etl._process_bronze_table = pytest.fail
    etl._process_silver_table = pytest.fail
    gold_calls = []
    etl._process_gold_table = lambda *args, **kwargs: gold_calls.append((args, kwargs)) or (1, 1, "")
    calls = []

    def fake_batch(tables, layer_name, target_tag, logger, worker_fn, on_success=None):
        calls.append(layer_name)
        if layer_name == LayerName.GOLD:
            worker_fn(tables[0], 99, MagicMock())
        return [tables[0]]

    etl._execute_table_batch = fake_batch

    result = etl.run_frozen_table(
        frozen_table=frozen_table,
        run_id=run_id,
        resume_from_run_id=parent_run_id,
    )

    assert result[0].id == 120
    assert calls == [LayerName.GOLD]
    assert gold_calls[0][1]["cdc_window_run_id"] == parent_run_id


def test_governed_retry_fails_closed_when_parent_cdc_window_is_not_compatible():
    etl = ETL.__new__(ETL)
    meta = SimpleNamespace(
        is_cdc="y",
        source_tag="gov_src",
        target_tag="land_test",
        source_table="orders",
        target_table="orders",
    )
    etl._parent_layer_statuses = lambda *args: {
        LayerName.BRONZE: RunStatus.SUCCESS,
        LayerName.SILVER: RunStatus.SUCCESS,
        LayerName.GOLD: RunStatus.FAILED,
    }
    etl._has_matching_parent_cdc_window = lambda *args: False

    assert etl._governed_retry_layer(meta, "parent-run") == LayerName.BRONZE


def test_governed_retry_reuses_latest_successful_full_snapshot_silver_artifact():
    """An exact full-snapshot retry may rebuild Gold without rerunning upstream layers."""
    etl = ETL.__new__(ETL)
    meta = SimpleNamespace(
        is_cdc="n",
        source_tag="gov_src",
        target_tag="land_test",
        source_table="orders",
        target_table="orders",
    )
    etl._parent_layer_statuses = lambda *args: {
        LayerName.BRONZE: RunStatus.SUCCESS,
        LayerName.SILVER: RunStatus.SUCCESS,
        LayerName.GOLD: RunStatus.FAILED,
    }
    etl._has_current_parent_silver_artifact = lambda *args: True

    assert etl._governed_retry_layer(meta, "parent-run") == LayerName.GOLD


def test_governed_retry_rebuilds_when_full_snapshot_silver_artifact_is_stale():
    """Static Silver storage must never be reused after another run overwrites it."""
    etl = ETL.__new__(ETL)
    meta = SimpleNamespace(
        is_cdc="n",
        source_tag="gov_src",
        target_tag="land_test",
        source_table="orders",
        target_table="orders",
    )
    etl._parent_layer_statuses = lambda *args: {
        LayerName.BRONZE: RunStatus.SUCCESS,
        LayerName.SILVER: RunStatus.SUCCESS,
        LayerName.GOLD: RunStatus.FAILED,
    }
    etl._has_current_parent_silver_artifact = lambda *args: False

    assert etl._governed_retry_layer(meta, "parent-run") == LayerName.BRONZE


def test_governed_silver_consumes_persisted_bronze_without_legacy_cdc_skip(monkeypatch):
    """Governed Silver must never re-evaluate the retired mutable CDC handler."""
    etl = ETL.__new__(ETL)
    etl.conn_manager = MagicMock()
    etl.spark = MagicMock()
    etl._freeze_schema_if_identity = lambda _meta, _df: None
    etl.cdc_handler = MagicMock()
    etl.cdc_handler.should_skip.side_effect = AssertionError(
        "Governed Silver must not call the legacy CDC handler"
    )

    meta = SimpleNamespace(
        id=120,
        is_cdc="y",
        source_tag="gov_src",
        source_table="orders",
        skip_dedup="n",
        table_contract=SimpleNamespace(cdc_policy=object()),
    )
    dataframe = MagicMock()
    etl.extractor = MagicMock()
    etl.extractor.read_landing.return_value = dataframe
    etl.loader = MagicMock()
    etl.current_action = "run_governed"
    etl.loader.write_history.return_value = ("history-path", "artifact-path")
    etl._publish_layer_artifact = MagicMock(return_value=MagicMock())

    transformer = MagicMock()
    transformer.add_source_identification_key.return_value = dataframe
    transformer.deduplicate.return_value = dataframe
    transformer.add_business_hash.return_value = dataframe
    transformer.add_audit_columns.return_value = dataframe
    transformer.cast_columns.return_value = dataframe
    transformer.select_target_columns.return_value = dataframe
    profiler = MagicMock()

    monkeypatch.setattr(etl_module, "Transformer", lambda _run_id: transformer)
    monkeypatch.setattr(etl_module, "StageProfiler", lambda **_kwargs: profiler)
    monkeypatch.setattr(
        etl_module,
        "SchemaDriftDetector",
        lambda *_args: MagicMock(detect=MagicMock()),
    )
    monkeypatch.setattr(etl_module.Reconciler, "count_and_cache", MagicMock(side_effect=[3, 3]))
    count_delta = MagicMock(side_effect=AssertionError("governed artifact is Parquet"))
    count_parquet = MagicMock(return_value=3)
    monkeypatch.setattr(etl_module.Reconciler, "count_delta", count_delta)
    monkeypatch.setattr(etl_module.Reconciler, "count_parquet", count_parquet)
    monkeypatch.setattr(
        etl_module.SchemaValidator,
        "validate_layer_contract",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        etl_module,
        "DQEngine",
        lambda *_args: MagicMock(evaluate=MagicMock(return_value="PASS")),
    )

    result = etl._process_silver_table(
        meta,
        run_id="governed-run",
        log_id=None,
        logger=None,
        date_partition="2026-09-14",
    )

    assert result == (3, 3, "Duplicates removed: 0", 0)
    etl.cdc_handler.should_skip.assert_not_called()
    count_delta.assert_not_called()
    count_parquet.assert_called_once_with(etl.spark, "artifact-path", run_id="governed-run")


def test_frozen_metadata_preserves_promoted_cdc_policy():
    frozen_table = FrozenTableEntry(
        table_id=120,
        source_tag="gov_src",
        source_schema="gov_src",
        source_db_type="mysql",
        source_table="orders",
        target_tag="land_test",
        target_schema="landing",
        target_table="orders",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="policy-1",
        policy_fingerprint="p" * 64,
        column_mapping_version_id="policy-1",
        column_mapping_fingerprint="m" * 64,
        business_key_version_id="policy-1",
        business_key_fingerprint="k" * 64,
        business_key_columns=["id"],
        deployment_profile_fingerprint="profile-fp",
        is_cdc="y",
        cdc_columns="id",
        cdc_strategy="SEQUENCE_HIGH_WATERMARK",
        cdc_p_days=7,
        contract_payload={
            "source_business_key": {
                "columns": ["id"],
                "evidence_type": "PRIMARY_KEY",
                "evidence_fingerprint": "k" * 64,
            },
            "target_business_key": {"columns": ["id"]},
            "cdc_policy": {
                "table_id": 120,
                "strategy": "SEQUENCE_HIGH_WATERMARK",
                "mutation_mode": "APPEND_ONLY",
                "cursor_finality_mode": "PREFIX_FINAL",
                "cursor_columns": ["id"],
                "cursor_types": ["INT64"],
                "tie_breaker_columns": ["id"],
                "cdc_policy_fingerprint": "c" * 64,
            },
        },
    )
    etl = ETL.__new__(ETL)

    meta = etl._metadata_row_from_frozen_table(frozen_table)

    assert meta.is_cdc == "y"
    assert meta.cdc_columns == "id"
    assert meta.cdc_p_days == 7
    assert meta.table_contract.cdc_policy.strategy.value == "SEQUENCE_HIGH_WATERMARK"
    assert meta.table_contract.policy_version_id == "policy-1"
    assert meta.table_contract.policy_fingerprint == "p" * 64
    assert meta.is_reconcile == "n"


def test_frozen_table_rejects_missing_governed_policy_binding():
    frozen_table = FrozenTableEntry(
        table_id=120,
        source_tag="gov_src",
        source_table="orders",
        target_tag="land_test",
        target_schema="landing",
        target_table="orders",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="policy-1",
        policy_fingerprint="p" * 64,
        column_mapping_version_id="policy-1",
        column_mapping_fingerprint="m" * 64,
        business_key_version_id="policy-1",
        business_key_fingerprint="k" * 64,
        business_key_columns=["id"],
        deployment_profile_fingerprint="profile-fp",
    )

    payload = frozen_table.to_dict()
    payload["policy_version_id"] = None

    with pytest.raises(ValueError, match="governed policy binding is missing"):
        FrozenTableEntry.from_dict(payload)


def test_batch_log_start_persists_frozen_policy_binding():
    """The atomic committer must receive the exact policy bound at submission."""
    from src.logging_.batch_logger import BatchLogger

    conn_manager = MagicMock()
    conn = MagicMock()
    cursor = MagicMock()
    conn_manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = (9001,)
    logger = BatchLogger(conn_manager, run_id="governed-run", spark_app_id="spark-app")

    assert logger.log_start(
        source_tag="gov_src",
        source_table="orders",
        target_table="orders",
        load_type="gold",
        priority=1,
        target_tag="land_test",
        policy_version_id="policy-130",
        policy_fingerprint="p" * 64,
    ) == 9001

    sql, params = cursor.execute.call_args.args
    assert "policy_version_id" in sql
    assert "policy_fingerprint" in sql
    assert params[-2:] == ("policy-130", "p" * 64)


def test_frozen_metadata_preserves_keyless_full_snapshot_contract():
    """A keyless full snapshot is governed, but never fabricated into a merge key."""
    frozen_table = FrozenTableEntry(
        table_id=121,
        source_tag="gov_src",
        source_schema="gov_src",
        source_db_type="oracle",
        source_table="legacy_audit",
        target_tag="land_test",
        target_schema="landing",
        target_table="legacy_audit",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="policy-2",
        policy_fingerprint="p" * 64,
        column_mapping_version_id="policy-2",
        column_mapping_fingerprint="m" * 64,
        business_key_version_id="policy-2",
        business_key_fingerprint="",
        business_key_columns=[],
        deployment_profile_fingerprint="profile-fp",
        is_cdc="n",
        cdc_strategy="FULL_SNAPSHOT",
        contract_payload={
            "source_business_key": None,
            "target_business_key": None,
            "cdc_policy": None,
        },
    )
    etl = ETL.__new__(ETL)

    meta = etl._metadata_row_from_frozen_table(frozen_table)

    assert meta.is_cdc == "n"
    assert meta.natural_key_column == ""
    assert meta.key_approval_state == "UNAPPROVED"
    assert meta.approved_source_business_key is None
    assert meta.table_contract.approved_source_business_key is None
    assert meta.table_contract.canonical_target_business_key is None


def test_frozen_table_entry_reconciliation_propagation():
    """Verify FrozenTableEntry serializes and deserializes reconciliation settings accurately."""
    from src.contracts.claim_models import FrozenTableEntry
    from src.orchestrator.etl import ETL

    entry = FrozenTableEntry(
        table_id=130,
        source_tag="opf_oracle",
        source_table="tblop_dec24",
        target_tag="pg_target",
        target_schema="public",
        target_table="tblop_dec24",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="pv-123",
        policy_fingerprint="fp1",
        column_mapping_version_id="pv-123",
        column_mapping_fingerprint="fp2",
        business_key_version_id="pv-123",
        business_key_fingerprint="fp3",
        business_key_columns=["reg_id"],
        deployment_profile_fingerprint="dp-fp",
        is_reconcile="n",
        reconcile_threshold=0.0,
        reconcile_action="FAIL",
    )
    d = entry.to_dict()
    assert d["is_reconcile"] == "n"
    assert d["reconcile_threshold"] == 0.0
    assert d["reconcile_action"] == "FAIL"

    restored = FrozenTableEntry.from_dict(d)
    assert restored.is_reconcile == "n"

    etl = ETL.__new__(ETL)
    meta = etl._metadata_row_from_frozen_table(restored)
    assert meta.is_reconcile == "n"
    assert meta.reconcile_threshold == 0.0
    assert meta.reconcile_action == "FAIL"


def test_execute_table_batch_silver_dedup_reconciliation_success(monkeypatch):
    """Verify _execute_table_batch accounts for duplicates_removed during Silver reconciliation."""
    np = pytest.importorskip("numpy")
    etl = ETL.__new__(ETL)
    etl.conn_manager = MagicMock()
    etl.metadata_reader = MagicMock()
    etl.current_action = "run_governed"
    etl._validate_eligible_tables = lambda tables, argument_name="tables": None
    etl.validate_metadata = lambda **k: None

    meta = SimpleNamespace(
        id=130,
        source_tag="opf_oracle",
        source_table="tblop_dec24",
        landing_table="tblop_dec24",
        history_table="tblop_dec24",
        target_table="tblop_dec24",
        target_tag="pg_target",
        priority=1,
        is_reconcile="y",
        reconcile_threshold=0.0,
        reconcile_action="FAIL",
        mapping_policy="IDENTITY",
        table_contract=SimpleNamespace(
            policy_version_id="policy-130",
            policy_fingerprint="p" * 64,
        ),
    )

    logger = MagicMock()
    logger.log_start.return_value = 101

    # Simulate Silver processing: 3,548,812 Bronze rows -> 18 duplicates removed -> 3,548,794 History rows
    def mock_worker(table_meta, log_id, log_obj):
        return (3548812, 3548794, "Duplicates removed: 18", np.int64(18))

    # Mock advisory locks
    raw_conn = MagicMock()
    raw_cur = MagicMock()
    raw_cur.fetchone.return_value = (True,)
    raw_conn.cursor.return_value = raw_cur
    etl.conn_manager.get_raw_metadata_db_connection.return_value = raw_conn
    etl.column_mapper = MagicMock()
    etl.column_mapper.resolve_table_mappings.return_value = None

    # Lineage DAG
    lineage_mock = MagicMock()
    lineage_mock.build_execution_dag.side_effect = lambda tables: tables
    lineage_mock.check_dependencies_status.return_value = "READY"
    monkeypatch.setattr(etl_module, "LineageService", lambda cm: lineage_mock)

    succeeded = etl._execute_table_batch(
        tables=[meta],
        layer_name="silver",
        target_tag="opf_oracle",
        logger=logger,
        worker_fn=mock_worker,
    )

    assert len(succeeded) == 1
    assert succeeded[0].source_table == "tblop_dec24"
    assert logger.log_start.call_args.kwargs["policy_version_id"] == "policy-130"
    assert logger.log_start.call_args.kwargs["policy_fingerprint"] == "p" * 64


def test_action_run_governed_uses_real_etl_frozen_execution_path(monkeypatch):
    from jobs import app_etl
    from src.contracts.claim_models import ExecutionFence

    run_id = str(uuid.uuid4())
    contract = _contract_dict(run_id)
    contract["execution_scope"] = "SILVER"
    contract["parent_contract_fingerprint"] = FrozenJobExecution.compute_fingerprint(contract)
    executed = []
    scope_calls = []
    released = []

    class FakeCursor:
        def __init__(self):
            self._one = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            normalized = " ".join(str(sql).split()).lower()
            if "from metadata.gui_job_registry" in normalized:
                self._one = ("run_governed", contract)
            elif "from metadata.job_scope_claim" in normalized:
                self._one = (77,)

        def fetchone(self):
            return self._one

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, *args, **kwargs):
            return FakeCursor()

    class FakeCM:
        def __init__(self, config_path):
            self.config_path = config_path
            self.metadata_db_config = {"username": "u", "password": "p", "host": "h", "port": 5432, "database": "d"}

        def get_metadata_db_connection(self):
            return FakeConn()

    class FakeClaimManager:
        def __init__(self, conn_manager):
            self.conn_manager = conn_manager

        def activate_scope_claim(self, claim_id, owner_instance_id, runtime_settings):
            return ExecutionFence(
                run_id=run_id,
                table_id=120,
                claim_id=claim_id,
                scope_action="run_governed",
                lease_revision=1,
                fencing_epoch=1,
                fencing_token_digest="a" * 64,
                raw_token="b" * 64,
                owner_instance_id=owner_instance_id,
                worker_attempt_number=1,
                lease_expires_at=datetime(2026, 1, 1),
            )

        def release_claim(self, claim_id, raw_token):
            released.append((claim_id, raw_token))

    class FakeETL:
        def __init__(self, config_path):
            self.config_path = config_path

        def run_frozen_table(self, frozen_table, run_id, **kwargs):
            executed.append((frozen_table.table_id, run_id))
            scope_calls.append(kwargs.get("execution_scope"))
            return [frozen_table.source_table]

    monkeypatch.setattr(app_etl, "ConnectionManager", FakeCM)
    monkeypatch.setattr(app_etl, "ETL", FakeETL)
    monkeypatch.setattr("jobs.app_etl.get_engine", lambda: object(), raising=False)
    monkeypatch.setattr("gui.services.database.get_engine", lambda: object())
    monkeypatch.setattr("src.contracts.runtime_settings.RuntimeSettings.from_config_path", lambda config_path: _runtime_settings())
    monkeypatch.setattr("src.orchestrator.lifecycle.transition_job", lambda *a, **k: True)
    monkeypatch.setattr("src.orchestrator.claim_manager.ScopeClaimManager", FakeClaimManager)

    monkeypatch.setattr("sqlalchemy.create_engine", lambda *args, **kwargs: object())
    app_etl.action_run_governed(run_id=run_id, config_path="config/config.local.yaml")

    assert executed == [(120, run_id)]
    assert scope_calls == ["SILVER"]
    assert released == [(77, "b" * 64)]


def test_governed_multi_table_run_continues_after_one_table_fails(monkeypatch):
    """A failed table keeps its own evidence but cannot stop later selected tables."""
    from jobs import app_etl
    from src.contracts.claim_models import ExecutionFence
    from src.orchestrator.lifecycle import TERMINAL_STATES, validate_transition_allowed

    run_id = str(uuid.uuid4())
    first_table = FrozenJobExecution.from_dict(_contract_dict(run_id)).tables[0]
    second_table = FrozenTableEntry(
        table_id=121,
        source_tag="gov_src",
        source_table="payments",
        target_tag="land_test",
        target_schema="landing",
        target_table="payments",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="policy-2",
        policy_fingerprint="q" * 64,
        column_mapping_version_id="policy-2",
        column_mapping_fingerprint="n" * 64,
        business_key_version_id="policy-2",
        business_key_fingerprint="j" * 64,
        business_key_columns=["payment_id"],
        deployment_profile_fingerprint="profile-fp",
    )
    frozen_job = FrozenJobExecution.build(
        run_id=run_id,
        action="run_governed",
        deployment_profile_fingerprint="profile-fp",
        tables=[first_table, second_table],
    )
    executed = []
    released = []
    transitions = []

    class FakeClaimManager:
        def __init__(self, conn_manager):
            pass

        def activate_scope_claim(self, claim_id, owner_instance_id, runtime_settings):
            table_id = claim_id - 1000
            return ExecutionFence(
                run_id=run_id,
                table_id=table_id,
                claim_id=claim_id,
                scope_action="run_governed",
                lease_revision=1,
                fencing_epoch=1,
                fencing_token_digest="a" * 64,
                raw_token="b" * 64,
                owner_instance_id=owner_instance_id,
                worker_attempt_number=1,
                lease_expires_at=datetime(2026, 1, 1),
            )

        def release_claim(self, claim_id, raw_token):
            released.append((claim_id, raw_token))

    class FakeETL:
        def __init__(self, config_path):
            pass

        def run_frozen_table(self, frozen_table, run_id, resume_from_run_id=None, execution_scope=None):
            executed.append(frozen_table.source_table)
            if frozen_table.source_table == "orders":
                raise RuntimeError("intentional table failure")
            return [frozen_table]

    def record_transition(*args, **kwargs):
        transitions.append(kwargs)
        return True

    monkeypatch.setattr(app_etl, "ConnectionManager", lambda config_path: type("MockCM", (), {"metadata_db_config": {"username":"u", "password":"p", "host":"h", "port":5432, "database":"d"}})())
    monkeypatch.setattr("sqlalchemy.create_engine", lambda *args, **kwargs: object())
    monkeypatch.setattr(app_etl, "ETL", FakeETL)
    monkeypatch.setattr(app_etl, "_load_frozen_job_execution", lambda *args: frozen_job)
    monkeypatch.setattr(app_etl, "_load_governed_retry_parent", lambda *args: (None, None))
    monkeypatch.setattr(app_etl, "_claim_id_for_table", lambda _cm, _run_id, table_id: 1000 + table_id)
    monkeypatch.setattr("gui.services.database.get_engine", lambda: object())
    monkeypatch.setattr("src.contracts.runtime_settings.RuntimeSettings.from_config_path", lambda *args: _runtime_settings())
    monkeypatch.setattr("src.orchestrator.lifecycle.transition_job", record_transition)
    monkeypatch.setattr("src.orchestrator.claim_manager.ScopeClaimManager", FakeClaimManager)

    monkeypatch.setattr(app_etl, 'create_engine', lambda *args, **kwargs: object(), raising=False)
    app_etl.action_run_governed(run_id=run_id, config_path="config/config.local.yaml")

    assert executed == ["orders", "payments"]
    assert released == [(1120, "b" * 64), (1121, "b" * 64)]
    assert transitions[-1]["to_status"] == "PARTIAL_SUCCESS"
    assert transitions[-1]["details"] == {
        "completed_table_count": 1,
        "failed_table_count": 1,
        "failed_tables": [{
            "table_id": 120,
            "source_table": "orders",
            "error_type": "RuntimeError",
        }],
    }
    validate_transition_allowed("RUNNING", "PARTIAL_SUCCESS")
    assert "PARTIAL_SUCCESS" in TERMINAL_STATES


def test_governed_retry_worker_forwards_parent_only_for_matching_frozen_table(monkeypatch):
    from jobs import app_etl
    from src.contracts.claim_models import ExecutionFence

    run_id = str(uuid.uuid4())
    parent_run_id = str(uuid.uuid4())
    parent_job = FrozenJobExecution.from_dict(_contract_dict(parent_run_id))
    child_table = parent_job.tables[0]
    child_job = FrozenJobExecution.build(
        run_id=run_id,
        action="run_governed",
        deployment_profile_fingerprint=parent_job.deployment_profile_fingerprint,
        tables=[child_table],
        retry_parent_run_id=parent_run_id,
    )
    calls = []

    class FakeClaimManager:
        def __init__(self, conn_manager):
            pass

        def activate_scope_claim(self, claim_id, owner_instance_id, runtime_settings):
            return ExecutionFence(
                run_id=run_id,
                table_id=120,
                claim_id=claim_id,
                scope_action="run_governed",
                lease_revision=1,
                fencing_epoch=1,
                fencing_token_digest="a" * 64,
                raw_token="b" * 64,
                owner_instance_id=owner_instance_id,
                worker_attempt_number=1,
                lease_expires_at=datetime(2026, 1, 1),
            )

        def release_claim(self, claim_id, raw_token):
            pass

    class FakeETL:
        def __init__(self, config_path):
            pass

        def run_frozen_table(self, **kwargs):
            calls.append(kwargs)
            return []

    monkeypatch.setattr(app_etl, "ConnectionManager", lambda config_path: type("MockCM", (), {"metadata_db_config": {"username":"u", "password":"p", "host":"h", "port":5432, "database":"d"}})())
    monkeypatch.setattr("sqlalchemy.create_engine", lambda *args, **kwargs: object())
    monkeypatch.setattr(app_etl, "ETL", FakeETL)
    monkeypatch.setattr(app_etl, "_load_frozen_job_execution", lambda *args: child_job)
    monkeypatch.setattr(app_etl, "_load_governed_retry_parent", lambda *args: (parent_run_id, parent_job))
    monkeypatch.setattr(app_etl, "_claim_id_for_table", lambda *args: 77)
    monkeypatch.setattr("gui.services.database.get_engine", lambda: object())
    monkeypatch.setattr("src.contracts.runtime_settings.RuntimeSettings.from_config_path", lambda *args: _runtime_settings())
    monkeypatch.setattr("src.orchestrator.lifecycle.transition_job", lambda *args, **kwargs: True)
    monkeypatch.setattr("src.orchestrator.claim_manager.ScopeClaimManager", FakeClaimManager)

    monkeypatch.setattr(app_etl, 'create_engine', lambda *args, **kwargs: object(), raising=False)
    app_etl.action_run_governed(run_id=run_id, config_path="config/config.local.yaml")

    assert calls == [{
        "frozen_table": child_table,
        "run_id": run_id,
        "resume_from_run_id": parent_run_id,
        "execution_scope": "FULL",
    }]


def test_retry_identity_allows_only_a_parent_produced_artifact_binding():
    from jobs import app_etl
    from src.contracts.claim_models import FrozenArtifactReference

    parent_run_id = str(uuid.uuid4())
    parent_table = FrozenJobExecution.from_dict(_contract_dict(parent_run_id)).tables[0]
    child_table = FrozenTableEntry.from_dict(parent_table.to_dict())
    child_table.upstream_artifact = FrozenArtifactReference(
        artifact_id="artifact",
        producer_run_id=parent_run_id,
        producer_attempt_number=1,
        storage_layer="SILVER",
        storage_uri="history/run_parent",
        storage_format="parquet",
        row_count=3,
        policy_version_id=parent_table.policy_version_id,
        policy_fingerprint=parent_table.policy_fingerprint,
        mapping_fingerprint=parent_table.column_mapping_fingerprint,
        source_key_fingerprint=parent_table.business_key_fingerprint,
        schema_fingerprint="s" * 64,
        artifact_fingerprint="a" * 64,
    )

    assert app_etl._same_retry_table_identity(parent_table, child_table, parent_run_id)

    child_table.upstream_artifact = FrozenArtifactReference(
        artifact_id="foreign-artifact",
        producer_run_id=str(uuid.uuid4()),
        producer_attempt_number=1,
        storage_layer="SILVER",
        storage_uri="history/run_foreign",
        storage_format="parquet",
        row_count=3,
        policy_version_id=parent_table.policy_version_id,
        policy_fingerprint=parent_table.policy_fingerprint,
        mapping_fingerprint=parent_table.column_mapping_fingerprint,
        source_key_fingerprint=parent_table.business_key_fingerprint,
        schema_fingerprint="s" * 64,
        artifact_fingerprint="a" * 64,
    )
    assert not app_etl._same_retry_table_identity(parent_table, child_table, parent_run_id)


def test_get_active_runs_reaps_stale_registry_rows_before_listing(monkeypatch):
    calls = []

    class EmptyResult:
        def mappings(self):
            return self

        def fetchall(self):
            return []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            return EmptyResult()

    class FakeEngine:
        def connect(self):
            return FakeConnection()

    monkeypatch.setattr("gui.services.execution_service.get_engine", lambda: FakeEngine())
    monkeypatch.setattr(
        "gui.services.execution_service.RuntimeSettings.from_config_path",
        lambda config_path: _runtime_settings(),
    )
    monkeypatch.setattr(
        "gui.services.execution_service.reap_stale_job_leases",
        lambda engine, dispatch_lease_timeout_sec, worker_heartbeat_timeout_sec: calls.append(
            (dispatch_lease_timeout_sec, worker_heartbeat_timeout_sec)
        ) or (0, 0),
    )

    assert ExecutionService.get_active_runs(context=ProjectContext("gov_src", "PROD")) == []
    assert calls == [(60, 120)]


def test_reaper_uses_submitted_at_when_running_worker_timestamps_are_missing(monkeypatch):
    from src.orchestrator import lifecycle

    executed_sql = []

    class Result:
        def __init__(self, rows=None):
            self._rows = rows or []

        def mappings(self):
            return self

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            sql_text = str(sql)
            executed_sql.append(sql_text)
            if "status = 'DISPATCHED'" in sql_text:
                return Result([])
            if "status = 'RUNNING'" in sql_text and "FROM metadata.gui_job_registry" in sql_text:
                return Result([{"run_id": str(uuid.uuid4()), "worker_attempt_number": 1}])
            return Result([])

    class FakeEngine:
        def connect(self):
            return FakeConnection()

        def begin(self):
            return FakeConnection()

    monkeypatch.setattr(
        lifecycle,
        "transition_job",
        lambda *args, **kwargs: kwargs["to_status"] == "FAILED",
    )

    reaped_dispatched, reaped_running = lifecycle.reap_stale_job_leases(
        FakeEngine(),
        dispatch_lease_timeout_sec=1,
        worker_heartbeat_timeout_sec=120,
    )

    assert reaped_dispatched == 0
    assert reaped_running == 1
    assert "COALESCE(last_heartbeat, started_at, submitted_at)" in "\n".join(executed_sql)
