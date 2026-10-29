"""Regression tests for post-discovery CDC policy reconciliation."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from jobs.app_etl import action_sync
from jobs.promote_table import (
    _promote_conditional_bootstrap_policy,
    _promote_full_snapshot_policy,
    build_catalog_policy_reconciliation,
    reconcile_catalog_cdc_policies,
)
from src.contracts.conditional_key import Int64ExceptionCodec
from src.exceptions import PolicyDependencyInvalidError
from src.metadata.policy_governance import PolicyGovernanceService
from src.metadata.policy_models import CDCStrategy, CursorLogicalType


def _row(**overrides):
    value = {
        "is_cdc": "n",
        "execution_authority_mode": "GOVERNED",
        "approved_key_evidence_fingerprint": "key-fp",
        "source_key_evidence_fingerprint": "key-fp",
        "discovered_cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
        "discovered_cdc_columns": "emigrant_id",
        "approved_source_business_key": ["emigrant_id"],
        "current_cdc_health_details": {},
        "discovered_cdc_source": "PROFILED",
        "discovered_cdc_health": "OK",
        "discovered_cdc_confidence": 100,
        "discovered_cdc_null_rows": 0,
        "discovered_cdc_health_details": {},
    }
    value.update(overrides)
    return value


def test_reconciler_builds_sequence_policy_for_unchanged_governed_key():
    result = build_catalog_policy_reconciliation(
        _row(),
        {"emigrant_id": {"data_type": "NUMBER", "data_precision": 38, "data_scale": 0}},
    )

    assert result["status"] == "READY"
    assert result["strategy"] == "SEQUENCE_HIGH_WATERMARK"
    assert result["cursor_columns"] == ("emigrant_id",)


def test_reconciler_normalizes_json_tier1_health_details():
    result = build_catalog_policy_reconciliation(
        _row(discovered_cdc_health_details='{"profile": "complete"}'),
        {"emigrant_id": {"data_type": "NUMBER", "data_precision": 38, "data_scale": 0}},
    )

    assert result["health_details"] == {"profile": "complete"}


def test_reconciler_accepts_complete_historic_timestamp_profile():
    """No recent writes are not a reason to disable a clean timestamp cursor."""
    result = build_catalog_policy_reconciliation(
        _row(
            discovered_cdc_strategy="TIMESTAMP_HIGH_WATERMARK",
            discovered_cdc_columns="updated_at",
            discovered_cdc_health="WARNING",
            discovered_cdc_confidence=90,
            discovered_cdc_health_details={
                "datatype_ok": True,
                "unique_key_valid": True,
                "recent_activity": False,
            },
        ),
        {
            "updated_at": {"data_type": "TIMESTAMP"},
            "emigrant_id": {"data_type": "BIGINT"},
        },
    )

    assert result["status"] == "READY"
    assert result["strategy"] == "TIMESTAMP_HIGH_WATERMARK"
    assert result["health"] == "OK"


def test_reconciler_preserves_operator_disabled_cdc():
    result = build_catalog_policy_reconciliation(
        _row(current_cdc_health_details={"operator_cdc_override": "OFF"}),
        {},
    )

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "CDC_DISABLED_BY_OPERATOR"


def test_reconciler_preserves_policy_when_key_evidence_changed():
    result = build_catalog_policy_reconciliation(
        _row(source_key_evidence_fingerprint="new-key-fp"),
        {},
    )

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "KEY_EVIDENCE_CHANGED"


def test_reconciler_does_not_downgrade_existing_policy_on_invalid_candidate():
    result = build_catalog_policy_reconciliation(
        _row(discovered_cdc_strategy="FULL_SNAPSHOT"),
        {},
    )

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "NO_EXECUTABLE_CDC_CANDIDATE"


def test_reconciler_leaves_bounded_exception_policy_to_bootstrap_lifecycle():
    result = build_catalog_policy_reconciliation(
        _row(current_cdc_health_details={"identity_mode": "BOUNDED_EXCEPTION_KEY"}),
        {},
    )

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "BOUNDED_CDC_REQUIRES_BOOTSTRAP"


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"discovered_cdc_health": "CRITICAL"}, "CDC_EVIDENCE_UNHEALTHY"),
        ({"discovered_cdc_confidence": 89}, "CDC_CONFIDENCE_BELOW_THRESHOLD"),
        ({"discovered_cdc_null_rows": 1}, "CDC_CURSOR_CONTAINS_NULLS"),
        ({"discovered_cdc_source": "PROFILE_ERROR"}, "CDC_EVIDENCE_NOT_PROFILED"),
    ],
)
def test_reconciler_rejects_unsafe_latest_tier1_evidence(overrides, reason):
    result = build_catalog_policy_reconciliation(
        _row(**overrides),
        {"emigrant_id": {"data_type": "NUMBER", "data_precision": 38, "data_scale": 0}},
    )

    assert result == {"status": "SKIPPED", "reason": reason}


class _Cursor:
    def __init__(self, governed_rows, columns):
        self.governed_rows = governed_rows
        self.columns = columns
        self.rows = []
        self.rowcount = 0
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.executions.append((sql, params))
        self.rows = self.columns if "db_columns_etl" in sql else self.governed_rows
        self.rowcount = 1 if str(sql).lstrip().upper().startswith("UPDATE") else len(self.rows)

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, governed_rows, columns):
        self.cursor_obj = _Cursor(governed_rows, columns)
        self.rollback_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self, **_):
        return self.cursor_obj

    def commit(self):
        return None

    def rollback(self):
        self.rollback_calls += 1
        return None


class _ConnectionManager:
    def __init__(self, governed_rows, columns):
        self.connection = _Connection(governed_rows, columns)

    def get_metadata_db_connection(self):
        return self.connection


def test_reconciler_supersedes_existing_policy_without_deleting_tier2_row():
    governed = [{
        "table_id": 17,
        "source_table": "orders",
        "is_cdc": "n",
        "execution_authority_mode": "GOVERNED",
        "current_policy_version_id": "v1",
        "approved_source_business_key": '["order_id"]',
        "approved_key_evidence_fingerprint": "key-fp",
        "source_key_evidence_fingerprint": "key-fp",
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_columns": "",
        "discovered_cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
        "discovered_cdc_columns": "order_id",
        "current_cdc_health_details": {},
        "discovered_cdc_source": "PROFILED",
        "discovered_cdc_health": "OK",
        "discovered_cdc_confidence": 100,
        "discovered_cdc_null_rows": 0,
        "discovered_cdc_health_details": {},
    }]
    columns = [{
        "column_name": "order_id",
        "data_type": "NUMBER",
        "data_precision": 18,
        "data_scale": 0,
    }]
    cm = _ConnectionManager(governed, columns)
    candidate_policy = object()
    candidate_contract = type("Contract", (), {"candidate_fingerprint": "candidate-fp"})()

    with patch("jobs.promote_table.PolicyGovernanceService") as service_cls:
        service = service_cls.return_value
        service.set_candidate_cdc_policy.return_value = candidate_policy
        service.build_candidate_contract.return_value = candidate_contract

        result = reconcile_catalog_cdc_policies(cm, "source_a")

    assert result["updated"] == ["orders"]
    request = service.promote_policy.call_args.args[0]
    assert request.expected_current_policy_version_id == "v1"
    assert request.expected_candidate_fingerprint == "candidate-fp"


def test_reconciler_scopes_existing_policies_to_selected_target():
    cm = _ConnectionManager([], [])

    reconcile_catalog_cdc_policies(cm, "source_a", target_tag="land_test")

    initial_sql, initial_params = cm.connection.cursor_obj.executions[0]
    assert "LOWER(e.target_tag) = LOWER(%s)" in initial_sql
    assert initial_params == ["source_a", "land_test", "land_test"]


def test_reconciler_materializes_approved_blocked_row_into_governed_policy():
    """Approved onboarding records must not remain permanently BLOCKED."""
    blocked = [{
        "table_id": 233,
        "source_table": "orders",
        "is_cdc": "n",
        "execution_authority_mode": "BLOCKED",
        "current_policy_version_id": None,
        "key_approval_state": "APPROVED",
        "approved_source_business_key": ["order_id"],
        "approved_key_evidence_fingerprint": "key-fp",
        "source_key_evidence_fingerprint": "key-fp",
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_columns": "",
        "discovered_cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
        "discovered_cdc_columns": "order_id",
        "current_cdc_health_details": {},
        "discovered_cdc_source": "PROFILED",
        "discovered_cdc_health": "OK",
        "discovered_cdc_confidence": 100,
        "discovered_cdc_null_rows": 0,
        "discovered_cdc_health_details": {},
    }]
    columns = [{
        "column_name": "order_id",
        "data_type": "NUMBER",
        "data_precision": 18,
        "data_scale": 0,
    }]
    cm = _ConnectionManager(blocked, columns)
    candidate_policy = object()
    candidate_contract = type("Contract", (), {"candidate_fingerprint": "candidate-fp"})()

    with patch("jobs.promote_table.PolicyGovernanceService") as service_cls:
        service = service_cls.return_value
        service.set_candidate_cdc_policy.return_value = candidate_policy
        service.build_candidate_contract.return_value = candidate_contract

        result = reconcile_catalog_cdc_policies(cm, "source_a")

    assert result["updated"] == ["orders"]
    request = service.promote_policy.call_args.args[0]
    assert request.expected_current_policy_version_id is None
    initial_sql = cm.connection.cursor_obj.executions[0][0]
    assert "e.execution_authority_mode = 'BLOCKED'" in initial_sql
    assert "e.key_approval_state = 'APPROVED'" in initial_sql
    metadata_update = next(
        sql for sql, _params in cm.connection.cursor_obj.executions
        if str(sql).lstrip().upper().startswith("UPDATE")
        and "cdc_source = %s" in sql
    )
    assert "NULLIF(cdc_health_details::text, '')::jsonb" in metadata_update


def test_reconciler_materializes_blocked_row_as_governed_snapshot_when_cdc_is_unsafe():
    """Unsafe CDC must not exclude an approved dataset from execution scope."""
    blocked = [_row(
        table_id=234,
        source_table="reference_data",
        execution_authority_mode="BLOCKED",
        current_policy_version_id=None,
        key_approval_state="APPROVED",
        discovered_cdc_health="WARNING",
        discovered_cdc_confidence=84,
        discovered_cdc_null_rows=12,
    )]
    cm = _ConnectionManager(blocked, [])

    with patch(
        "jobs.promote_table._promote_full_snapshot_policy", return_value=True
    ) as promote_snapshot:
        result = reconcile_catalog_cdc_policies(cm, "source_a")

    assert result["updated"] == ["reference_data"]
    assert result["failed"] == []
    promote_snapshot.assert_called_once_with(
        cm,
        234,
        "SYSTEM_DISCOVERY",
        "Latest Tier-1 CDC evidence is not executable; governed full snapshot selected.",
    )


def test_reconciler_rolls_back_one_failure_and_continues_to_next_table():
    first = _row(
        table_id=301,
        source_table="first",
        current_policy_version_id="v1",
        approved_source_business_key=["order_id"],
        cdc_strategy="FULL_SNAPSHOT",
        cdc_columns="",
        discovered_cdc_columns="order_id",
    )
    second = _row(
        table_id=302,
        source_table="second",
        current_policy_version_id="v2",
        approved_source_business_key=["order_id"],
        cdc_strategy="FULL_SNAPSHOT",
        cdc_columns="",
        discovered_cdc_columns="order_id",
    )
    columns = [{
        "column_name": "order_id",
        "data_type": "NUMBER",
        "data_precision": 18,
        "data_scale": 0,
    }]
    cm = _ConnectionManager([first, second], columns)
    candidate_contract = type("Contract", (), {"candidate_fingerprint": "candidate-fp"})()

    with patch("jobs.promote_table.PolicyGovernanceService") as service_cls:
        service = service_cls.return_value
        service.set_candidate_cdc_policy.side_effect = [RuntimeError("first failed"), object()]
        service.build_candidate_contract.return_value = candidate_contract

        result = reconcile_catalog_cdc_policies(cm, "source_a")

    assert result["failed"] == [{"table": "first", "error": "first failed"}]
    assert result["updated"] == ["second"]
    assert cm.connection.rollback_calls == 1


def test_reconciler_upgrades_old_unapproved_row_to_conditional_bootstrap():
    governed = [{
        "table_id": 178,
        "source_table": "tblop_dec24",
        "source_table_master_key": 1234,
        "is_cdc": "n",
        "execution_authority_mode": "GOVERNED",
        "current_policy_version_id": "full-v1",
        "key_approval_state": "UNAPPROVED",
        "approved_source_business_key": None,
        "approved_key_evidence_fingerprint": None,
        "source_key_evidence_fingerprint": "key-fp",
        "source_key_candidate_columns": ["reg_id"],
        "source_key_evidence_type": "PROFILED_CONDITIONAL_KEY",
        "source_key_source": "PROFILED_CONDITIONAL",
        "source_key_confidence": 95,
        "source_key_candidate_nullable": False,
        "source_key_candidate_compatible": True,
        "source_duplicate_count": 18,
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_columns": "",
        "current_cdc_health_details": {
            "reason": "No approved business key available for CDC; governed full snapshot selected."
        },
        "discovered_cdc_strategy": "FULL_SNAPSHOT",
        "discovered_cdc_columns": "reg_id",
        "discovered_cdc_source": "PROFILED_CONDITIONAL",
        "discovered_cdc_health": "WARNING",
        "discovered_cdc_confidence": 95,
        "discovered_cdc_null_rows": 0,
        "discovered_cdc_health_details": {
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
            "activation_state": "BOOTSTRAP_REQUIRED",
            "exception_values": [0],
            "exception_values_payload": {
                "codec_version": "exception-key-v1",
                "logical_type": "INT64",
                "values": [0],
            },
            "exception_values_fingerprint": Int64ExceptionCodec.fingerprint([0]),
        },
    }]
    cm = _ConnectionManager(governed, [])

    with patch(
        "jobs.promote_table._promote_conditional_bootstrap_policy",
        return_value=True,
    ) as bootstrap:
        result = reconcile_catalog_cdc_policies(cm, "opf_oracle", target_tag="land_test")

    assert result["updated"] == ["tblop_dec24"]
    bootstrap.assert_called_once_with(
        cm,
        178,
        "SYSTEM_DISCOVERY",
        "Latest Tier-1 evidence qualified a bounded-exception identity; governed bootstrap required.",
        expected_current_policy_version_id="full-v1",
    )
    approval_updates = [
        sql for sql, _params in cm.connection.cursor_obj.executions
        if str(sql).lstrip().upper().startswith("UPDATE")
        and "key_approval_state = 'APPROVED'" in sql
    ]
    assert len(approval_updates) == 1


def test_reconciler_retries_conditional_bootstrap_after_approval_was_committed():
    fingerprint = Int64ExceptionCodec.fingerprint([0])
    governed = [{
        "table_id": 178,
        "source_table": "tblop_dec24",
        "source_table_master_key": 1234,
        "is_cdc": "n",
        "execution_authority_mode": "GOVERNED",
        "current_policy_version_id": "full-v1",
        "key_approval_state": "APPROVED",
        "approved_source_business_key": ["reg_id"],
        "approved_key_evidence_fingerprint": "key-fp",
        "source_key_evidence_fingerprint": "key-fp",
        "source_key_candidate_columns": ["reg_id"],
        "source_key_evidence_type": "PROFILED_CONDITIONAL_KEY",
        "source_key_source": "PROFILED_CONDITIONAL",
        "source_key_confidence": 95,
        "source_key_candidate_nullable": False,
        "source_key_candidate_compatible": True,
        "source_duplicate_count": 18,
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_columns": "",
        "current_cdc_health_details": {"reason": "stale full snapshot"},
        "discovered_cdc_strategy": "FULL_SNAPSHOT",
        "discovered_cdc_columns": "reg_id",
        "discovered_cdc_source": "PROFILED_CONDITIONAL",
        "discovered_cdc_health": "WARNING",
        "discovered_cdc_confidence": 95,
        "discovered_cdc_null_rows": 0,
        "discovered_cdc_health_details": {
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
            "activation_state": "BOOTSTRAP_REQUIRED",
            "exception_values": [0],
            "exception_values_payload": {
                "codec_version": "exception-key-v1",
                "logical_type": "INT64",
                "values": [0],
            },
            "exception_values_fingerprint": fingerprint,
        },
    }]
    cm = _ConnectionManager(governed, [])

    with patch(
        "jobs.promote_table._promote_conditional_bootstrap_policy",
        return_value=True,
    ) as bootstrap:
        result = reconcile_catalog_cdc_policies(cm, "opf_oracle", target_tag="land_test")

    assert result["updated"] == ["tblop_dec24"]
    bootstrap.assert_called_once()
    evidence_updates = [
        sql for sql, _params in cm.connection.cursor_obj.executions
        if "Conditional evidence refresh" not in sql
        and "approved_key_evidence_fingerprint = %s" in sql
    ]
    assert len(evidence_updates) == 1


def test_reconciler_materializes_blocked_conditional_subscription_without_policy_fence():
    """A newly promoted target has no prior policy version to fence against."""
    fingerprint = Int64ExceptionCodec.fingerprint([0])
    blocked = [{
        "table_id": 348,
        "source_table": "tblop_dec24",
        "source_table_master_key": 1234,
        "is_cdc": "n",
        "execution_authority_mode": "BLOCKED",
        "current_policy_version_id": None,
        "key_approval_state": "APPROVED",
        "approved_source_business_key": ["reg_id"],
        "approved_key_evidence_fingerprint": "key-fp",
        "source_key_evidence_fingerprint": "key-fp",
        "source_key_candidate_columns": ["reg_id"],
        "source_key_evidence_type": "PROFILED_CONDITIONAL_KEY",
        "source_key_source": "PROFILED_CONDITIONAL",
        "source_key_confidence": 95,
        "source_key_candidate_nullable": False,
        "source_key_candidate_compatible": True,
        "source_duplicate_count": 18,
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_columns": "",
        "current_cdc_health_details": {"identity_mode": "BOUNDED_EXCEPTION_KEY"},
        "discovered_cdc_strategy": "FULL_SNAPSHOT",
        "discovered_cdc_columns": "reg_id",
        "discovered_cdc_source": "PROFILED_CONDITIONAL",
        "discovered_cdc_health": "WARNING",
        "discovered_cdc_confidence": 95,
        "discovered_cdc_null_rows": 0,
        "discovered_cdc_health_details": {
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
            "activation_state": "BOOTSTRAP_REQUIRED",
            "exception_values": [0],
            "exception_values_payload": {
                "codec_version": "exception-key-v1",
                "logical_type": "INT64",
                "values": [0],
            },
            "exception_values_fingerprint": fingerprint,
        },
    }]
    cm = _ConnectionManager(blocked, [])

    with patch(
        "jobs.promote_table._promote_conditional_bootstrap_policy",
        return_value=True,
    ) as bootstrap:
        result = reconcile_catalog_cdc_policies(cm, "opf_oracle", target_tag="land_n")

    assert result["updated"] == ["tblop_dec24"]
    assert bootstrap.call_args.kwargs["expected_current_policy_version_id"] is None
    evidence_refresh = next(
        sql for sql, _params in cm.connection.cursor_obj.executions
        if "approved_key_evidence_fingerprint = %s" in sql
    )
    assert "current_policy_version_id IS NOT DISTINCT FROM %s" in evidence_refresh


def test_reconciler_never_refreshes_operator_disabled_conditional_policy():
    governed = [{
        "table_id": 178,
        "source_table": "tblop_dec24",
        "source_table_master_key": 1234,
        "execution_authority_mode": "GOVERNED",
        "current_policy_version_id": "full-v1",
        "key_approval_state": "UNAPPROVED",
        "source_key_evidence_type": "PROFILED_CONDITIONAL_KEY",
        "current_cdc_health_details": {"operator_cdc_override": "OFF"},
        "discovered_cdc_health_details": {"identity_mode": "BOUNDED_EXCEPTION_KEY"},
    }]
    cm = _ConnectionManager(governed, [])

    with patch("jobs.promote_table._promote_conditional_bootstrap_policy") as bootstrap:
        result = reconcile_catalog_cdc_policies(cm, "opf_oracle", target_tag="land_test")

    assert result["skipped"] == [{
        "table": "tblop_dec24",
        "reason": "CDC_DISABLED_BY_OPERATOR",
    }]
    bootstrap.assert_not_called()


def test_conditional_bootstrap_metadata_update_is_policy_version_fenced():
    cm = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.return_value = {
        "cdc_health_details": {
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
            "exception_values": [0],
        }
    }
    cur.rowcount = 1

    with patch("jobs.promote_table.PolicyGovernanceService") as service_cls:
        service_cls.return_value.build_candidate_contract.return_value = MagicMock(
            candidate_fingerprint="bootstrap-fp"
        )
        assert _promote_conditional_bootstrap_policy(
            cm,
            178,
            "SYSTEM_DISCOVERY",
            "bootstrap",
            expected_current_policy_version_id="full-v1",
        ) is True

    update_calls = [
        call for call in cur.execute.call_args_list
        if str(call.args[0]).lstrip().upper().startswith("UPDATE")
    ]
    assert len(update_calls) == 1
    assert "current_policy_version_id = %s" in update_calls[0].args[0]
    assert update_calls[0].args[1][-1] == "full-v1"


def test_action_sync_reports_policy_reconciliation_failures_without_failing_tier1_discovery(capsys):
    etl = MagicMock()
    crawler = MagicMock()
    crawler.stats = {
        "tables_discovered": 1,
        "tables_inserted": 0,
        "tables_deactivated": 0,
        "columns_discovered": 2,
        "columns_inserted": 0,
        "columns_deactivated": 0,
    }

    with patch("jobs.discover_and_promote.DiscoveryCrawler", return_value=crawler), patch(
        "jobs.promote_table.reconcile_catalog_cdc_policies",
        return_value={
            "updated": [],
            "unchanged": [],
            "skipped": [],
            "failed": [{"table": "orders", "error": "policy drift"}],
        },
    ):
        action_sync(etl, "source_a", False, False, False)

    assert "Policy reconciliation warnings after Tier-1 discovery: orders" in capsys.readouterr().out


def test_policy_promotion_rechecks_operator_off_marker_under_row_lock():
    candidate = SimpleNamespace(
        cdc_policy=SimpleNamespace(
            strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            cursor_types=(CursorLogicalType.INT64,),
        )
    )
    locked_row = {
        "cdc_health": "OK",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_null_rows": 0,
        "cdc_health_details": {"operator_cdc_override": "OFF"},
    }

    with pytest.raises(PolicyDependencyInvalidError, match="explicitly disabled"):
        PolicyGovernanceService(None)._validate_profiled_cdc_activation(candidate, locked_row)
