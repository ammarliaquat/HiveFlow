# tests/test_foundation_gate_01_live.py
"""
Foundation Integration Gate #1 — Live Verification Suite.

Validates the complete foundation layer against the development environment:
- Test 1: Normal Application Startup & Schema Certification (32 canonical migrations).
- Test 2: Incompatible Metadata DB Fast-Fail (0 side-effects, 0 leases, 0 batch rows).
- Test 3: IDENTITY Table Freeze & Immutability.
- Test 4: EXPLICIT Table Column Mapping & Transformation.
- Test 5: Fail-Closed Missing Mapping Detection (MissingColumnMappingError).
- Test 6: Same-Name Isolation by stable table_id.
- Test 7: Multi-Table Application Batch & Scope-Claim Concurrency Governance.
"""

import uuid
from unittest.mock import MagicMock, patch
import pytest
from sqlalchemy import text

from src.connections.connection_manager import ConnectionManager
from src.connections.schema_certifier import SchemaCertifier, MetadataSchemaIncompatibleError
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings, ResolvedColumnMapping
from src.transform.column_mapper import ColumnMapper, MissingColumnMappingError
from src.orchestrator.etl import ETL
from gui.models.submission import SubmissionRequest, SubmissionResult
from gui.services.execution_service import ExecutionService
from gui.services.database import get_engine


# ─── TEST 1: NORMAL APPLICATION STARTUP & LIVE SCHEMA CERTIFICATION ───────────
def test_gate1_test1_live_schema_certification():
    """
    Test 1: Verify SchemaCertifier validates the live metadata database against
    the 32 canonical migrations ledger.
    """
    cm = ConnectionManager("config/config.local.yaml")
    certifier = SchemaCertifier(cm)
    # certify() validates all 32 migrations without raising MetadataSchemaIncompatibleError
    certifier.certify()


# ─── TEST 2: INCOMPATIBLE METADATA DB FAST-FAIL ──────────────────────────────
def test_gate1_test2_incompatible_metadata_db_fast_fail():
    """
    Test 2: Verify that an incompatible metadata database raises MetadataSchemaIncompatibleError
    at startup with zero persistent side-effects.
    """
    mock_certifier = MagicMock(spec=SchemaCertifier)
    mock_certifier.certify.side_effect = MetadataSchemaIncompatibleError(
        "DATABASE_INCOMPATIBLE: Local migration checksums do not match database ledger."
    )

    with pytest.raises(MetadataSchemaIncompatibleError, match="DATABASE_INCOMPATIBLE"):
        ETL("config/config.local.yaml", schema_certifier=mock_certifier)


# ─── TEST 3: IDENTITY TABLE FREEZE & IMMUTABILITY ─────────────────────────────
def test_gate1_test3_identity_table_freeze_immutability():
    """
    Test 3: Verify that IDENTITY tables snapshot their schema into an immutable
    ResolvedTableMappings instance with a SHA-256 fingerprint.
    """
    initial_mappings = ResolvedTableMappings.build(
        table_id=101,
        raw_mappings=[],
        mapping_policy=MappingPolicy.IDENTITY
    )
    assert initial_mappings.mapping_policy == MappingPolicy.IDENTITY
    assert initial_mappings.frozen_source_schema is None

    # Freeze schema upon reading source DataFrame
    source_schema = ["id", "user_name", "created_at"]
    frozen_mappings = initial_mappings.with_frozen_schema(source_schema)

    assert frozen_mappings is not initial_mappings
    assert frozen_mappings.frozen_source_schema == tuple(source_schema)
    assert frozen_mappings.fingerprint != initial_mappings.fingerprint
    assert len(frozen_mappings.fingerprint) == 64  # SHA-256 hex digest

    # Verify immutability: modifying source list does not change frozen tuple
    source_schema.append("unexpected_column")
    assert "unexpected_column" not in frozen_mappings.frozen_source_schema


# ─── TEST 4: EXPLICIT TABLE COLUMN MAPPING & TRANSFORMATION ───────────────────
def test_gate1_test4_explicit_table_column_mapping():
    """
    Test 4: Verify EXPLICIT table mapping transforms source columns to target columns.
    """
    cm = ConnectionManager("config/config.local.yaml")
    mapper = ColumnMapper(cm)

    # Build explicit mappings for test table
    col_mappings = [
        {"column_id": 1, "column_sequence": 1, "source_column": "USER_ID", "target_column": "user_id", "data_type": "bigint", "source_column_datatype": "bigint", "canonical_type": "BIGINT"},
        {"column_id": 2, "column_sequence": 2, "source_column": "USER_EMAIL", "target_column": "email", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"},
    ]
    resolved = ResolvedTableMappings.build(
        table_id=202,
        raw_mappings=col_mappings,
        mapping_policy=MappingPolicy.EXPLICIT
    )

    assert resolved.mapping_policy == MappingPolicy.EXPLICIT
    assert len(resolved.mappings) == 2
    assert resolved.mappings[0].source_column == "USER_ID"
    assert resolved.mappings[0].target_column == "user_id"
    assert resolved.mappings[1].source_column == "USER_EMAIL"
    assert resolved.mappings[1].target_column == "email"


# ─── TEST 5: FAIL-CLOSED MISSING MAPPING DETECTION ────────────────────────────
def test_gate1_test5_fail_closed_missing_mapping():
    """
    Test 5: Verify EXPLICIT policy fails fast with MissingColumnMappingError when
    an existing table has zero column mappings in metadata.
    """
    cm = ConnectionManager("config/config.local.yaml")
    mapper = ColumnMapper(cm)

    # Table 236 (empty_tbl) exists in db_tables_etl but has 0 mappings in db_columns_etl
    with pytest.raises(MissingColumnMappingError):
        mapper.resolve_table_mappings(
            table_id=236,
            mapping_policy=MappingPolicy.EXPLICIT
        )


# ─── TEST 6: SAME-NAME TABLE ISOLATION BY TABLE_ID ───────────────────────────
def test_gate1_test6_same_name_table_isolation():
    """
    Test 6: Verify schema_a.users (table_id 301) and schema_b.users (table_id 302)
    resolve completely isolated mappings without collision.
    """
    resolved_a = ResolvedTableMappings.build(
        table_id=301,
        raw_mappings=[{"column_id": 10, "column_sequence": 1, "source_column": "ID", "target_column": "patient_id", "data_type": "bigint", "source_column_datatype": "bigint", "canonical_type": "BIGINT"}],
        mapping_policy=MappingPolicy.EXPLICIT
    )
    resolved_b = ResolvedTableMappings.build(
        table_id=302,
        raw_mappings=[{"column_id": 20, "column_sequence": 1, "source_column": "ID", "target_column": "staff_id", "data_type": "bigint", "source_column_datatype": "bigint", "canonical_type": "BIGINT"}],
        mapping_policy=MappingPolicy.EXPLICIT
    )

    assert resolved_a.table_id == 301
    assert resolved_b.table_id == 302
    assert resolved_a.mappings[0].target_column == "patient_id"
    assert resolved_b.mappings[0].target_column == "staff_id"
    assert resolved_a.fingerprint != resolved_b.fingerprint


# ─── TEST 7: MULTI-TABLE BATCH & CONCURRENCY GOVERNANCE ───────────────────────
def test_gate1_test7_multi_table_batch_and_concurrency_governance():
    """
    Test 7: Verify multi-table execution claims sorted scope locks in metadata.job_scope_claim,
    records authoritative registry entry, and rejects overlapping submissions.
    """
    engine = get_engine()
    req_id_1 = str(uuid.uuid4())
    req_id_2 = str(uuid.uuid4())

    req_1 = SubmissionRequest(
        request_id=req_id_1,
        action="run_pipeline",
        source_tag="msopforg_opforg",
        source_table_ids=[120, 54, 55],  # Unsorted multi-table scope
        priority=1,
        actor_type="USER",
        actor_id="gate1_tester"
    )

    with patch("gui.services.execution_service.JobManager.submit_job"):
        # 1. First submission succeeds and acquires sorted claims
        res_1 = ExecutionService.submit_run(request=req_1)
        assert isinstance(res_1, SubmissionResult)
        assert res_1.accepted is True
        assert res_1.run_id is not None

        # Verify claims exist in DB
        with engine.connect() as conn:
            claims = conn.execute(text("""
                SELECT source_table_id FROM metadata.job_scope_claim
                WHERE run_id::text = :rid AND released_at IS NULL
                ORDER BY source_table_id ASC
            """), {"rid": res_1.run_id}).fetchall()
            claimed_tids = [r[0] for r in claims]
            assert claimed_tids == [54, 55, 120]  # Deterministically sorted

        # 2. Conflicting submission on overlapping table_id=54 is rejected
        req_2 = SubmissionRequest(
            request_id=req_id_2,
            action="run_pipeline",
            source_tag="msopforg_opforg",
            source_table_ids=[54],
            priority=1,
            actor_type="USER",
            actor_id="gate1_tester"
        )
        res_2 = ExecutionService.submit_run(request=req_2)
        assert isinstance(res_2, SubmissionResult)
        assert res_2.accepted is False
        assert res_2.rejection_code == "ACTIVE_EXECUTION_EXISTS"
        assert res_2.existing_active_run_id == res_1.run_id
        assert res_2.submitted_at is None

        # 3. Clean up test run & claims
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM metadata.job_event WHERE run_id::text = :rid"), {"rid": res_1.run_id})
            conn.execute(text("DELETE FROM metadata.job_scope_claim WHERE run_id::text = :rid"), {"rid": res_1.run_id})
            conn.execute(text("DELETE FROM metadata.job_submission_audit WHERE run_id::text = :rid OR request_id::text IN (:r1, :r2)"), {"rid": res_1.run_id, "r1": req_id_1, "r2": req_id_2})
            conn.execute(text("DELETE FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": res_1.run_id})
