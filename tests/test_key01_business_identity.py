"""KEY-01 pure metadata tests: evidence is not execution authority."""

import pytest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from src.discovery.type_mapper import detect_business_key, detect_business_key_profiled
from src.exceptions import BusinessKeyRequiredError
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyApprovalPolicy,
    KeyApprovalState,
    KeyCandidate,
    KeyEvidenceType,
    build_promotion_key_contract,
    require_approved_business_key,
    resolve_key_candidate,
)
from src.metadata.metadata_models import MetadataRow
from src.metadata.metadata_reader import MetadataReader
from src.validation.stage0_policy import validate_stage0_execution_policy
from src.validation.stage0_policy import is_source_identity_execution_enabled
from src.exceptions import CDCPolicyNotMigratedError
from jobs.discover_and_promote import _build_discovery_queries, _effective_discovery_schema, _source_object_key


def candidate(columns, evidence, *, nullable=False, compatible=True):
    return KeyCandidate(
        columns=tuple(columns),
        evidence_type=evidence,
        confidence=100,
        nullable=nullable,
        compatible=compatible,
    )


def metadata_row(**overrides):
    values = {
        "id": 1,
        "source_tag": "source",
        "source_schema": "public",
        "source_table": "events",
        "source_db_type": "postgres",
        "landing_table": "events",
        "history_table": "events",
        "target_table": "events",
        "target_schema": "landing",
        "is_active": "y",
        "priority": 1,
        "load_type": "full",
        "is_cdc": "n",
        "cdc_columns": None,
        "cdc_p_days": 0,
        "delta_days": 0,
        "last_success_timestamp": None,
        "is_reconcile": "n",
        "reconcile_column": None,
        "reconcile_measure": "count",
        "chunk_size": 1000,
        "num_partitions": 1,
        "partition_column": None,
        "source_key_prefix": None,
        "natural_key_column": "legacy_guess",
    }
    values.update(overrides)
    return MetadataRow(**values)


def test_source_physical_primary_key_becomes_approved_business_key():
    result = resolve_key_candidate(candidate(["id"], KeyEvidenceType.PRIMARY_KEY))

    assert result.approval_state is KeyApprovalState.APPROVED
    assert result.canonical_source_business_key == ("id",)


def test_composite_physical_primary_key_preserves_source_column_order():
    result = resolve_key_candidate(
        candidate(["tenant_id", "sequence_no"], KeyEvidenceType.PRIMARY_KEY)
    )

    assert result.canonical_source_business_key == ("tenant_id", "sequence_no")


def test_unique_constraint_is_approved_source_identity():
    key = candidate(["email"], KeyEvidenceType.UNIQUE_CONSTRAINT)

    result = resolve_key_candidate(key)

    assert result.approval_state is KeyApprovalState.APPROVED
    assert result.canonical_source_business_key == ("email",)


@pytest.mark.parametrize(
    "evidence",
    [KeyEvidenceType.PROFILED_UNIQUE, KeyEvidenceType.PROFILED_COMPOSITE],
)
def test_profiled_uniqueness_is_candidate_only(evidence):
    result = resolve_key_candidate(candidate(["candidate_key"], evidence))

    assert result.candidate.columns == ("candidate_key",)
    assert result.approval_state is KeyApprovalState.UNAPPROVED
    assert result.canonical_source_business_key is None


def test_lowest_duplicate_heuristic_is_never_active():
    result = resolve_key_candidate(
        candidate(["least_bad_column"], KeyEvidenceType.PROFILED_LOWEST_DUP)
    )

    assert result.canonical_source_business_key is None


def test_id_looking_column_is_not_active_by_name():
    result = resolve_key_candidate(candidate(["id"], KeyEvidenceType.HEURISTIC))

    assert result.canonical_source_business_key is None
    assert detect_business_key(["id"], primary_keys=[]) is None


def test_first_column_fallback_is_impossible():
    assert detect_business_key(["payload", "description"], primary_keys=[]) is None


def test_unprofiled_key_like_names_are_not_persisted_as_heuristic_evidence():
    result = detect_business_key_profiled(["id", "payload"], primary_keys=[])

    assert result["columns_unique_key"] is None
    assert result["key_source"] == "NONE"
    assert result["key_confidence"] == 0
    assert result["duplicate_count"] == 0
    assert result["skip_dedup"] == "y"
    assert result["key_candidate_compatible"] is False


def test_no_trusted_key_has_no_canonical_source_identity():
    result = resolve_key_candidate(
        candidate([], KeyEvidenceType.NONE)
    )

    assert result.canonical_source_business_key is None


def test_keyed_operation_requires_an_approved_business_key():
    result = resolve_key_candidate(candidate(["id"], KeyEvidenceType.PROFILED_UNIQUE))

    with pytest.raises(BusinessKeyRequiredError, match="BUSINESS_KEY_REQUIRED"):
        require_approved_business_key(result)


def test_recrawl_candidate_change_does_not_replace_existing_approval():
    approved = ApprovedBusinessKey(
        columns=("legacy_id",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fingerprint1",
    )
    changed_candidate = KeyCandidate(
        columns=("new_id",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        confidence=100,
        evidence_fingerprint="fingerprint2",
    )

    result = resolve_key_candidate(
        changed_candidate,
        existing_approved=approved,
    )

    assert result.candidate.columns == ("new_id",)
    assert result.approval_state == KeyApprovalState.UNAPPROVED
    assert result.canonical_source_business_key is None


@pytest.mark.parametrize("invalid_field", ["nullable", "compatible"])
def test_nullable_or_incompatible_candidate_is_rejected(invalid_field):
    kwargs = {"nullable": False, "compatible": True}
    kwargs[invalid_field] = True if invalid_field == "nullable" else False
    result = resolve_key_candidate(
        candidate(["id"], KeyEvidenceType.PRIMARY_KEY, **kwargs)
    )

    assert result.approval_state is KeyApprovalState.REJECTED
    assert result.canonical_source_business_key is None


def test_metadata_row_reads_active_identity_only_from_approved_fields():
    row = metadata_row(
        key_approval_state="APPROVED",
        approved_source_business_key=["tenant_id", "event_id"],
        approved_key_evidence="PRIMARY_KEY",
        key_approved_by="steward@example.test",
        key_approved_at=datetime.now(timezone.utc),
        key_approval_revision=1,
        approved_key_evidence_fingerprint="abc123",
        current_key_evidence_fingerprint="abc123",
        key_candidate_nullable=False,
        key_candidate_compatible=True,
    )

    assert row.canonical_source_business_key == ("tenant_id", "event_id")
    assert row.canonical_business_key_columns == ("legacy_guess",)


@pytest.mark.parametrize(
    "missing_field",
    [
        "approved_key_evidence",
        "key_approved_by",
        "key_approved_at",
        "key_approval_revision",
        "approved_key_evidence_fingerprint",
        "current_key_evidence_fingerprint",
    ],
)
def test_incomplete_approval_provenance_never_becomes_authority(missing_field):
    values = {
        "key_approval_state": "APPROVED",
        "approved_source_business_key": ["id"],
        "approved_key_evidence": "PRIMARY_KEY",
        "key_approved_by": "steward@example.test",
        "key_approved_at": datetime.now(timezone.utc),
        "key_approval_revision": 1,
        "approved_key_evidence_fingerprint": "same",
        "current_key_evidence_fingerprint": "same",
        "key_candidate_nullable": False,
        "key_candidate_compatible": True,
    }
    values[missing_field] = None

    assert metadata_row(**values).canonical_source_business_key is None


def test_stale_candidate_evidence_revokes_runtime_authority():
    row = metadata_row(
        key_approval_state="APPROVED",
        approved_source_business_key=["id"],
        approved_key_evidence="PRIMARY_KEY",
        key_approved_by="steward@example.test",
        key_approved_at=datetime.now(timezone.utc),
        key_approval_revision=1,
        approved_key_evidence_fingerprint="approved",
        current_key_evidence_fingerprint="recrawled",
        key_candidate_nullable=False,
        key_candidate_compatible=True,
    )

    assert row.canonical_source_business_key is None


def test_legacy_candidate_never_populates_canonical_source_identity():
    row = metadata_row(
        key_source="PRIMARY_KEY",
        key_confidence=100,
        key_approval_state=None,
        approved_source_business_key=None,
    )

    assert row.canonical_source_business_key is None


def test_approved_source_key_does_not_unfreeze_legacy_cdc():
    row = metadata_row(
        is_cdc="y",
        key_approval_state="APPROVED",
        approved_source_business_key=["id"],
    )

    with pytest.raises(CDCPolicyNotMigratedError):
        validate_stage0_execution_policy(row)


def test_approved_source_key_does_not_enable_identity_execution_before_key02():
    assert is_source_identity_execution_enabled(metadata_row()) is False


def test_metadata_reader_separates_candidate_evidence_from_approved_identity():
    sql = MetadataReader._base_select_sql("= 'gold'")

    assert "m.key_candidate_columns" in sql
    assert "m.key_evidence_type" in sql
    assert "etl.key_approval_state" in sql
    assert "etl.approved_source_business_key" in sql
    assert "etl.key_approved_at" in sql
    assert "etl.approved_key_evidence_fingerprint" in sql
    assert "m.key_evidence_fingerprint AS current_key_evidence_fingerprint" in sql
    assert "NULL" in sql and "AS natural_key_column" in sql
    assert "etl.source_schema = m.schema_name" in sql


def test_key01_migration_has_no_legacy_approval_backfill():
    migration = Path("migrations/029_source_business_key_governance.sql").read_text(
        encoding="utf-8"
    )

    assert "approved_source_business_key JSONB" in migration
    assert "ALTER COLUMN key_source DROP DEFAULT" in migration
    assert "SET approved_source_business_key" not in migration
    assert "prevent_approved_key_mutation" in migration
    assert "is_valid_key_column_array" in migration


def test_promotion_carries_candidate_without_granting_execution_authority():
    promoted = build_promotion_key_contract(
        {
            "table_master_key": 42,
            "business_key": "id",
            "key_source": "PROFILED",
        }
    )

    assert promoted.source_table_master_key == 42
    assert promoted.candidate_columns == ("id",)
    assert promoted.approval_state is KeyApprovalState.UNAPPROVED
    assert promoted.approved_source_business_key is None


def test_explicit_physical_pk_promotion_records_complete_approval_provenance():
    approved_at = datetime.now(timezone.utc)
    promoted = build_promotion_key_contract(
        {
            "table_master_key": 42,
            "key_candidate_columns": ["tenant_id", "sequence_no"],
            "key_evidence_type": "PRIMARY_KEY",
            "key_confidence": 100,
            "key_candidate_nullable": False,
            "key_candidate_compatible": True,
            "key_evidence_fingerprint": "fingerprint",
        },
        approve_physical_primary_key=True,
        approved_by="steward@example.test",
        approved_at=approved_at,
    )

    assert promoted.approval_state is KeyApprovalState.APPROVED
    assert promoted.approved_source_business_key == ("tenant_id", "sequence_no")
    assert promoted.approved_key_evidence_fingerprint == "fingerprint"
    assert promoted.approved_by == "steward@example.test"
    assert promoted.approved_at == approved_at
    assert promoted.approval_revision == 1


@pytest.mark.parametrize("db_type", ["postgres", "mysql", "sqlserver", "oracle"])
def test_discovery_pk_query_is_schema_qualified_and_ordinally_ordered(db_type):
    query = _build_discovery_queries(db_type, "tenant_schema")["primary_keys"].lower()

    assert "table_schema" in query
    assert "key_ordinal" in query
    assert "order by" in query


def test_discovery_escapes_schema_as_a_sql_literal():
    queries = _build_discovery_queries("postgres", "tenant'schema")

    assert "tenant''schema" in queries["tables"]
    assert "tenant'schema" not in queries["tables"]


def test_source_object_identity_includes_schema_and_table():
    assert _source_object_key("tag1", "Public", "Events") != _source_object_key("tag1", "Audit", "Events")
    assert _source_object_key("tag1", "Public", "Events") != _source_object_key("tag2", "Public", "Events")


def test_tier1_discovery_persists_detected_cdc_activation_flag():
    source = Path("jobs/discover_and_promote.py").read_text(encoding="utf-8")

    assert 'cdc_prof_res.get("is_cdc", "n")' in source
    assert "cdc_columns, is_cdc, business_key" in source
    assert "is_cdc          = EXCLUDED.is_cdc" in source


def test_metadata_validator_uses_approved_governed_key_for_dedup_authority():
    source = Path("src/validation/metadata_validator.py").read_text(encoding="utf-8")

    assert "approved_source_business_key" in source
    assert "execution_authority_mode" in source
    assert "key_approval_state" in source
    assert "columns_unique_key (business key)" not in source


def test_metadata_validator_allows_keyless_governed_full_snapshot_execution():
    """A governed full snapshot is an overwrite contract, not a keyed merge."""
    source = Path("src/validation/metadata_validator.py").read_text(encoding="utf-8")

    assert "UPPER(COALESCE(cdc_strategy, 'FULL_SNAPSHOT')) <> 'FULL_SNAPSHOT'" in source


def test_mysql_discovery_uses_database_not_connection_schema():
    conn = SimpleNamespace(
        db_type="mysql",
        database="source_catalog",
        schema="logical_source_tag",
        jdbc_properties={"user": "source_user"},
    )

    assert _effective_discovery_schema(conn) == "source_catalog"


def test_non_mysql_discovery_prefers_schema():
    conn = SimpleNamespace(
        db_type="postgres",
        database="target_database",
        schema="landing_schema",
        jdbc_properties={"user": "target_user"},
    )

    assert _effective_discovery_schema(conn) == "landing_schema"
