import hashlib
import json

import pytest

from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec
from src.contracts.claim_models import FrozenJobExecution, FrozenTableEntry
from gui.services.execution_service import _materialize_governed_contract
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyCandidate,
    KeyEvidenceType,
    build_promotion_key_contract,
    resolve_key_candidate,
)
from src.metadata.policy_governance import _approved_source_key_from_policy_row


def _table(**kwargs):
    values = {
        "table_id": 1,
        "source_tag": "source",
        "source_table": "orders",
        "target_tag": "target",
        "target_schema": "landing",
        "target_table": "orders",
        "priority": 1,
        "authority_mode": "GOVERNED",
        "policy_version_id": "policy-1",
        "policy_fingerprint": "p" * 64,
        "column_mapping_version_id": "mapping-1",
        "column_mapping_fingerprint": "m" * 64,
        "business_key_version_id": "key-1",
        "business_key_fingerprint": "k" * 64,
        "business_key_columns": ["id"],
        "deployment_profile_fingerprint": "d" * 64,
    }
    values.update(kwargs)
    return FrozenTableEntry(**values)


def test_int64_exception_codec_is_typed_canonical_and_deduplicated():
    payload = Int64ExceptionCodec.encode([3, 0, 3])
    assert payload == {
        "codec_version": "exception-key-v1",
        "logical_type": "INT64",
        "values": [0, 3],
    }
    assert Int64ExceptionCodec.decode(payload) == (0, 3)
    expected = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()
    assert Int64ExceptionCodec.fingerprint(payload) == expected


@pytest.mark.parametrize("value", [True, 1.0, "1", None, 2**63])
def test_int64_exception_codec_rejects_coercive_or_out_of_range_values(value):
    with pytest.raises(ValueError):
        Int64ExceptionCodec.encode([value])


def test_conditional_key_candidate_and_approval_preserve_identity_evidence():
    candidate = KeyCandidate(
        columns=("id",),
        evidence_type=KeyEvidenceType.PROFILED_CONDITIONAL_KEY,
        confidence=95,
        nullable=False,
        compatible=True,
        evidence_fingerprint="source-fingerprint",
        identity_mode=IdentityMode.BOUNDED_EXCEPTION_KEY,
        exception_values=(0,),
    )
    resolution = resolve_key_candidate(candidate)
    assert resolution.approval_state.value == "APPROVED"
    assert resolution.approved_key.identity_mode is IdentityMode.BOUNDED_EXCEPTION_KEY
    assert resolution.approved_key.exception_values == (0,)


def test_promotion_contract_reads_typed_conditional_payload():
    contract = build_promotion_key_contract(
        {
            "table_master_key": 7,
            "key_candidate_columns": ["id"],
            "key_evidence_type": "PROFILED_CONDITIONAL_KEY",
            "key_confidence": 95,
            "key_candidate_nullable": False,
            "key_candidate_compatible": True,
            "key_evidence_fingerprint": "source-fingerprint",
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
            "exception_values_payload": Int64ExceptionCodec.encode([0]),
        },
        approve_physical_primary_key=True,
    )
    assert contract.identity_mode is IdentityMode.BOUNDED_EXCEPTION_KEY
    assert contract.exception_values_payload["values"] == [0]
    assert contract.exception_values_fingerprint == Int64ExceptionCodec.fingerprint([0])


def test_locked_policy_row_reads_conditional_identity_from_its_own_column_shape():
    """Promotion's lock query has CDC details after five CDC policy columns."""
    locked_row = (
        178, "EXPLICIT", "opf_oracle", "tblop_dec24", "land_test", "landing", "tblop_dec24",
        None, ["reg_id"], "PROFILED_CONDITIONAL_KEY", "evidence-fingerprint", "admin",
        "2026-09-17T15:00:00+00:00", 1, False, True, "evidence-fingerprint",
        "FULL_SNAPSHOT", "PROFILED_CONDITIONAL", 95, "OK", 0,
        {"identity_mode": "BOUNDED_EXCEPTION_KEY", "exception_values": [0]},
    )

    approved_key = _approved_source_key_from_policy_row(178, locked_row)

    assert approved_key.identity_mode is IdentityMode.BOUNDED_EXCEPTION_KEY
    assert approved_key.exception_values == (0,)


def test_frozen_table_v2_round_trip_conditional_identity():
    table = _table(
        identity_mode="BOUNDED_EXCEPTION_KEY",
        conditional_exception_values_payload=Int64ExceptionCodec.encode([0]),
        row_hash_codec_version="row-hash-v2",
    )
    restored = FrozenTableEntry.from_dict(table.to_dict())
    assert restored.identity_mode == IdentityMode.BOUNDED_EXCEPTION_KEY.value
    assert restored.conditional_exception_values_payload["values"] == [0]
    assert restored.conditional_exception_fingerprint == Int64ExceptionCodec.fingerprint([0])


def test_legacy_frozen_job_v1_decodes_keyless_as_keyless():
    table = _table(business_key_columns=[])
    contract = FrozenJobExecution.build(
        run_id="run-1",
        action="run_governed",
        deployment_profile_fingerprint="d" * 64,
        tables=[table],
    ).to_dict()
    contract["contract_version"] = "frozen_job_execution.v1"
    # Simulate a payload written before conditional-identity fields and the
    # execution-scope field existed.
    contract.pop("execution_scope")
    for field_name in (
        "identity_mode",
        "conditional_exception_values_payload",
        "conditional_exception_fingerprint",
        "frozen_capability_evidence",
        "row_hash_codec_version",
    ):
        contract["tables"][0].pop(field_name, None)
    contract["parent_contract_fingerprint"] = FrozenJobExecution.compute_fingerprint(contract)
    restored = FrozenJobExecution.from_dict(contract)
    assert restored.contract_version == "frozen_job_execution.v1"
    assert restored.tables[0].identity_mode == IdentityMode.KEYLESS.value
    assert restored.to_dict() == contract


def test_frozen_table_rejects_conditional_payload_fingerprint_drift():
    payload = _table(
        identity_mode="BOUNDED_EXCEPTION_KEY",
        conditional_exception_values_payload=Int64ExceptionCodec.encode([0]),
    ).to_dict()
    payload["conditional_exception_fingerprint"] = "wrong"
    with pytest.raises(ValueError, match="exception fingerprint"):
        FrozenTableEntry.from_dict(payload)


def test_governed_submission_freezes_conditional_identity_from_policy_payload():
    payload = {
        "schema_version": 1,
        "table_id": 21,
        "mapping_policy": "EXPLICIT",
        "source_business_key": {
            "columns": ["id"],
            "evidence_type": "PROFILED_CONDITIONAL_KEY",
            "evidence_fingerprint": "k" * 64,
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
            "exception_values_payload": Int64ExceptionCodec.encode([0]),
            "exception_values_fingerprint": Int64ExceptionCodec.fingerprint([0]),
        },
        "target_business_key": {
            "columns": ["id"],
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
        },
        "mappings": [],
        "cdc_policy": {
            "table_id": 21,
            "strategy": "FULL_SNAPSHOT",
            "mutation_mode": "APPEND_ONLY",
            "cursor_finality_mode": "PREFIX_FINAL",
            "cursor_columns": [],
            "cursor_types": [],
            "tie_breaker_columns": [],
            "cdc_policy_fingerprint": "c" * 64,
            "identity_mode": "BOUNDED_EXCEPTION_KEY",
            "exception_values_payload": Int64ExceptionCodec.encode([0]),
            "exception_values_fingerprint": Int64ExceptionCodec.fingerprint([0]),
            "activation_state": "BOOTSTRAP_REQUIRED",
        },
    }
    row = {
        "table_id": 21,
        "source_tag": "source",
        "source_table": "events",
        "target_tag": "target",
        "target_schema": "landing",
        "target_table": "events",
        "priority": 1,
        "execution_authority_mode": "GOVERNED",
        "current_policy_version_id": "policy-21",
        "policy_fingerprint": "p" * 64,
        "mapping_fingerprint": "m" * 64,
        "source_key_fingerprint": "k" * 64,
        "contract_payload": payload,
        "source_schema": "public",
        "source_database_type": "postgres",
        "landing_table": "events",
        "history_table": "events",
        "table_type": "full",
        "is_cdc": "n",
        "cdc_columns": None,
        "cdc_strategy": "FULL_SNAPSHOT",
        "cdc_p_days": 0,
        "delta_days": 0,
        "last_success_timestamp": None,
        "last_success_date": None,
        "rerun_flag": "n",
        "partition_lower_bound": None,
        "partition_upper_bound": None,
        "target_rows_per_partition": 250000,
        "minimum_partition_threshold": 50000,
        "maximum_partitions": 32,
        "is_reconcile": "n",
        "reconcile_threshold": 0.0,
        "reconcile_action": "FAIL",
    }

    class Cursor:
        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return [row]

        def fetchone(self):
            return None

    frozen = _materialize_governed_contract(
        Cursor(),
        run_id="run-21",
        source_tag="source",
        table_ids=[21],
        action="run_governed",
    )

    assert frozen.tables[0].identity_mode == IdentityMode.BOUNDED_EXCEPTION_KEY.value
    assert frozen.tables[0].conditional_exception_values_payload["values"] == [0]
    assert frozen.tables[0].contract_payload["cdc_policy"]["activation_state"] == "BOOTSTRAP_REQUIRED"
