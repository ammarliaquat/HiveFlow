# src/metadata/policy_models.py
"""
META-01 — Domain Models for Table Policy Versioning, Promotion, and Provenance Governance.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
import hashlib
import uuid

from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings
from src.metadata.key_contract import ApprovedBusinessKey, ResolvedTargetBusinessKey
from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec
from src.utils.json_utils import json_safe_dumps


class PolicyActorType(str, Enum):
    USER = "USER"
    SYSTEM = "SYSTEM"
    SCHEDULER = "SCHEDULER"


class PromotionOutcome(str, Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


class CDCStrategy(str, Enum):
    SEQUENCE_HIGH_WATERMARK = "SEQUENCE_HIGH_WATERMARK"
    TIMESTAMP_HIGH_WATERMARK = "TIMESTAMP_HIGH_WATERMARK"
    INSERT_TIMESTAMP = "INSERT_TIMESTAMP"
    FULL_SNAPSHOT = "FULL_SNAPSHOT"
    LOG_BASED = "LOG_BASED"


class SourceMutationMode(str, Enum):
    APPEND_ONLY = "APPEND_ONLY"
    MUTABLE = "MUTABLE"
    UNKNOWN = "UNKNOWN"


class CursorFinalityMode(str, Enum):
    PREFIX_FINAL = "PREFIX_FINAL"
    UNSAFE_OR_UNKNOWN = "UNSAFE_OR_UNKNOWN"


class CursorLogicalType(str, Enum):
    INT64 = "INT64"
    DECIMAL = "DECIMAL"
    TIMESTAMP = "TIMESTAMP"
    STRING = "STRING"
    UUID = "UUID"
    DATE = "DATE"


def compute_deterministic_fingerprint(data: Any) -> str:
    """Computes a deterministic SHA-256 hex digest for a JSON-serializable structure."""
    canonical_json = json_safe_dumps(
        data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ResolvedCDCPolicy:
    """Immutable CDC policy specification governing source cursor change extraction."""
    table_id: int
    strategy: CDCStrategy
    mutation_mode: SourceMutationMode
    cursor_finality_mode: CursorFinalityMode
    cursor_columns: Tuple[str, ...]
    cursor_types: Tuple[CursorLogicalType, ...]
    tie_breaker_columns: Tuple[str, ...]
    cdc_policy_fingerprint: str
    identity_mode: IdentityMode = IdentityMode.EXACT_KEY
    exception_values_payload: Optional[Dict[str, Any]] = None
    exception_values_fingerprint: Optional[str] = None
    activation_state: str = "ACTIVE"
    capability_evidence: Optional[Dict[str, Any]] = None

    @classmethod
    def build(
        cls,
        table_id: int,
        strategy: CDCStrategy,
        mutation_mode: SourceMutationMode,
        cursor_finality_mode: CursorFinalityMode,
        cursor_columns: Tuple[str, ...],
        cursor_types: Tuple[CursorLogicalType, ...],
        tie_breaker_columns: Optional[Tuple[str, ...]] = None,
        identity_mode: IdentityMode = IdentityMode.EXACT_KEY,
        exception_values: Optional[Tuple[int, ...]] = None,
        exception_values_payload: Optional[Dict[str, Any]] = None,
        activation_state: str = "ACTIVE",
        capability_evidence: Optional[Dict[str, Any]] = None,
    ) -> "ResolvedCDCPolicy":
        tie_breakers = tie_breaker_columns or ()
        mode = identity_mode if isinstance(identity_mode, IdentityMode) else IdentityMode(str(identity_mode).strip().upper())
        if exception_values_payload is not None:
            values = Int64ExceptionCodec.decode(exception_values_payload)
        else:
            values = Int64ExceptionCodec.normalize(exception_values or ())
        # During bootstrap the bounded identity is carried by the full
        # snapshot policy before a cursor has been committed.  Once the
        # policy is executable, a single cursor column is required.  Keeping
        # this distinction in the contract lets promotion preserve the
        # exception set without falsely activating CDC before bootstrap.
        if mode is IdentityMode.BOUNDED_EXCEPTION_KEY and (
            not values
            or (strategy is not CDCStrategy.FULL_SNAPSHOT and len(cursor_columns) != 1)
        ):
            raise ValueError(
                "BOUNDED_EXCEPTION_KEY requires exception values and one cursor "
                "column for an executable CDC strategy"
            )
        if mode is not IdentityMode.BOUNDED_EXCEPTION_KEY and values:
            raise ValueError("CDC exception values require BOUNDED_EXCEPTION_KEY identity")
        exception_payload = Int64ExceptionCodec.encode(values) if values else None
        exception_fp = Int64ExceptionCodec.fingerprint(values) if values else None
        payload = {
            "table_id": table_id,
            "strategy": strategy.value,
            "mutation_mode": mutation_mode.value,
            "cursor_finality_mode": cursor_finality_mode.value,
            "cursor_columns": list(cursor_columns),
            "cursor_types": [t.value for t in cursor_types],
            "tie_breaker_columns": list(tie_breakers),
            "identity_mode": mode.value,
            "exception_values_payload": exception_payload,
            "exception_values_fingerprint": exception_fp,
            "activation_state": str(activation_state or "ACTIVE").upper(),
            "capability_evidence": capability_evidence or None,
        }
        fp = compute_deterministic_fingerprint(payload)
        return cls(
            table_id=table_id,
            strategy=strategy,
            mutation_mode=mutation_mode,
            cursor_finality_mode=cursor_finality_mode,
            cursor_columns=tuple(cursor_columns),
            cursor_types=tuple(cursor_types),
            tie_breaker_columns=tuple(tie_breakers),
            cdc_policy_fingerprint=fp,
            identity_mode=mode,
            exception_values_payload=exception_payload,
            exception_values_fingerprint=exception_fp,
            activation_state=str(activation_state or "ACTIVE").upper(),
            capability_evidence=capability_evidence,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_id": self.table_id,
            "strategy": self.strategy.value,
            "mutation_mode": self.mutation_mode.value,
            "cursor_finality_mode": self.cursor_finality_mode.value,
            "cursor_columns": list(self.cursor_columns),
            "cursor_types": [t.value for t in self.cursor_types],
            "tie_breaker_columns": list(self.tie_breaker_columns),
            "cdc_policy_fingerprint": self.cdc_policy_fingerprint,
            "identity_mode": self.identity_mode.value,
            "exception_values_payload": self.exception_values_payload,
            "exception_values_fingerprint": self.exception_values_fingerprint,
            "activation_state": self.activation_state,
            "capability_evidence": self.capability_evidence,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResolvedCDCPolicy":
        return cls(
            table_id=data["table_id"],
            strategy=CDCStrategy(data["strategy"]),
            mutation_mode=SourceMutationMode(data["mutation_mode"]),
            cursor_finality_mode=CursorFinalityMode(data["cursor_finality_mode"]),
            cursor_columns=tuple(data["cursor_columns"]),
            cursor_types=tuple(CursorLogicalType(t) for t in data["cursor_types"]),
            tie_breaker_columns=tuple(data.get("tie_breaker_columns", [])),
            cdc_policy_fingerprint=data["cdc_policy_fingerprint"],
            identity_mode=IdentityMode(str(data.get("identity_mode", IdentityMode.EXACT_KEY.value)).upper()),
            exception_values_payload=data.get("exception_values_payload"),
            exception_values_fingerprint=data.get("exception_values_fingerprint"),
            activation_state=str(data.get("activation_state", "ACTIVE")).upper(),
            capability_evidence=data.get("capability_evidence"),
        )


@dataclass(frozen=True)
class CDCCursor:
    """Strict typed cursor value structure."""
    schema_version: int
    strategy: CDCStrategy
    columns: Tuple[str, ...]
    types: Tuple[CursorLogicalType, ...]
    values: Tuple[Any, ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "strategy": self.strategy.value,
            "columns": list(self.columns),
            "types": [t.value for t in self.types],
            "values": list(self.values),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CDCCursor":
        return cls(
            schema_version=data.get("schema_version", 1),
            strategy=CDCStrategy(data["strategy"]),
            columns=tuple(data["columns"]),
            types=tuple(CursorLogicalType(t) for t in data.get("types", [])),
            values=tuple(data["values"]),
        )


@dataclass(frozen=True)
class ResolvedCDCWindow:
    """Immutable execution window bounding an attempt's source change extraction."""
    window_id: str
    run_id: str
    table_id: int
    attempt_number: int
    policy_version_id: str
    cdc_policy_fingerprint: str
    strategy: CDCStrategy
    lower_cursor: Optional[CDCCursor]
    upper_cursor: CDCCursor
    window_fingerprint: str
    captured_at: datetime
    is_empty: bool = False


@dataclass(frozen=True)
class CandidateTableContract:
    """
    Candidate table execution contract assembled from candidate KEY/MAP/CDC metadata before promotion.
    Cannot be used for pipeline runtime execution without promotion.
    """
    table_id: int
    mapping_policy: MappingPolicy
    approved_source_business_key: Optional[ApprovedBusinessKey]
    resolved_mappings: ResolvedTableMappings
    canonical_target_business_key: Optional[ResolvedTargetBusinessKey]
    cdc_policy: Optional[ResolvedCDCPolicy]
    contract_payload: Dict[str, Any]
    contract_fingerprint: str
    candidate_fingerprint: str

    @classmethod
    def build(
        cls,
        table_id: int,
        mapping_policy: MappingPolicy,
        approved_source_business_key: Optional[ApprovedBusinessKey],
        resolved_mappings: ResolvedTableMappings,
        canonical_target_business_key: Optional[ResolvedTargetBusinessKey],
        cdc_policy: Optional[ResolvedCDCPolicy] = None,
        policy_schema_version: int = 1,
    ) -> "CandidateTableContract":
        # Build canonical execution payload
        mappings_payload: List[Dict[str, Any]] = []
        if mapping_policy == MappingPolicy.EXPLICIT:
            for m in resolved_mappings.mappings:
                mappings_payload.append({
                    "column_id": getattr(m, "column_id", None),
                    "column_sequence": getattr(m, "column_sequence", 0),
                    "source_column": str(m.source_column).strip(),
                    "target_column": str(m.target_column).strip(),
                    "data_type": getattr(m, "data_type", ""),
                    "source_column_datatype": getattr(m, "source_column_datatype", ""),
                    "canonical_type": getattr(m, "canonical_type", ""),
                    "transform_expr": getattr(m, "transform_expr", None),
                })
            # Ensure deterministic order by sequence / source_column
            mappings_payload.sort(key=lambda x: (x["column_sequence"], x["source_column"]))

        cdc_payload: Optional[Dict[str, Any]] = cdc_policy.to_dict() if cdc_policy else None

        contract_payload: Dict[str, Any] = {
            "schema_version": policy_schema_version,
            "table_id": table_id,
            "mapping_policy": mapping_policy.value,
            "source_business_key": ({
                "columns": list(approved_source_business_key.columns),
                "evidence_type": approved_source_business_key.evidence_type.value,
                "evidence_fingerprint": approved_source_business_key.evidence_fingerprint,
                "identity_mode": approved_source_business_key.identity_mode.value,
                "exception_values_payload": (
                    Int64ExceptionCodec.encode(approved_source_business_key.exception_values)
                    if approved_source_business_key.exception_values else None
                ),
                "exception_values_fingerprint": (
                    approved_source_business_key.exception_values_fingerprint or None
                ),
                "exception_evidence_fingerprint": (
                    approved_source_business_key.exception_evidence_fingerprint or None
                ),
            } if approved_source_business_key else None),
            "mappings": mappings_payload,
            "target_business_key": ({
                "columns": list(canonical_target_business_key.target_key_columns),
                "fingerprint": canonical_target_business_key.fingerprint,
                "identity_mode": canonical_target_business_key.identity_mode.value,
                "exception_values_payload": (
                    Int64ExceptionCodec.encode(canonical_target_business_key.exception_values)
                    if canonical_target_business_key.exception_values else None
                ),
                "exception_values_fingerprint": (
                    canonical_target_business_key.exception_values_fingerprint or None
                ),
                "exception_evidence_fingerprint": (
                    canonical_target_business_key.exception_evidence_fingerprint or None
                ),
            } if canonical_target_business_key else None),
            "cdc_policy": cdc_payload,
            "source_key_fingerprint": (
                approved_source_business_key.evidence_fingerprint
                if approved_source_business_key else None
            ),
            "mapping_fingerprint": resolved_mappings.fingerprint,
            "target_key_fingerprint": (
                canonical_target_business_key.fingerprint
                if canonical_target_business_key else None
            ),
            "cdc_policy_fingerprint": cdc_policy.cdc_policy_fingerprint if cdc_policy else None,
        }

        contract_fingerprint = compute_deterministic_fingerprint(contract_payload)
        
        policy_payload = {
            "policy_schema_version": policy_schema_version,
            "contract_fingerprint": contract_fingerprint,
        }
        candidate_fingerprint = compute_deterministic_fingerprint(policy_payload)

        return cls(
            table_id=table_id,
            mapping_policy=mapping_policy,
            approved_source_business_key=approved_source_business_key,
            resolved_mappings=resolved_mappings,
            canonical_target_business_key=canonical_target_business_key,
            cdc_policy=cdc_policy,
            contract_payload=contract_payload,
            contract_fingerprint=contract_fingerprint,
            candidate_fingerprint=candidate_fingerprint,
        )


@dataclass(frozen=True)
class PromotedTablePolicy:
    """Authoritative immutable historical policy snapshot stored in metadata.table_policy_version."""
    policy_version_id: str
    table_id: int
    version_number: int
    policy_schema_version: int
    contract_payload: Dict[str, Any]
    source_key_fingerprint: str
    mapping_fingerprint: str
    target_key_fingerprint: str
    contract_fingerprint: str
    policy_fingerprint: str
    promoted_at: datetime
    promoted_by_type: str
    promoted_by_id: str
    promotion_reason: Optional[str]
    supersedes_version_id: Optional[str]
    cdc_policy_fingerprint: Optional[str] = None


@dataclass(frozen=True)
class PromotionRequest:
    """Request payload for promoting candidate policy to authoritative versioned status."""
    table_id: int
    expected_candidate_fingerprint: str
    actor_type: str
    actor_id: str
    reason: Optional[str] = None
    expected_current_policy_version_id: Optional[str] = None
    authenticated_principal: Optional[Any] = None


@dataclass(frozen=True)
class PromotionResult:
    """Outcome of a successful policy promotion transaction."""
    table_id: int
    previous_version_number: Optional[int]
    new_version_number: int
    policy_version_id: str
    policy_fingerprint: str
    promoted_at: datetime


# ─── RECON-01 Models ──────────────────────────────────────────────────────────
class ReconciliationStrategy(str, Enum):
    """Authoritative physical reconciliation strategies separate from CDC source selection."""
    ATTEMPT_SCOPED_APPEND = "ATTEMPT_SCOPED_APPEND"
    KEYED_WINDOW_CONVERGENCE = "KEYED_WINDOW_CONVERGENCE"
    FULL_SNAPSHOT_CONVERGENCE = "FULL_SNAPSHOT_CONVERGENCE"


class ReconciliationOutcome(str, Enum):
    """High-level disposition of physical reconciliation."""
    CONVERGED = "CONVERGED"
    REJECTED = "REJECTED"
    EMPTY_VERIFIED = "EMPTY_VERIFIED"


@dataclass(frozen=True)
class AttemptPersistenceManifest:
    """
    Immutable persistence manifest frozen immediately after transformation and before/at physical persistence.
    Provides durable, verifiable expectations for physical reconciliation.
    """
    manifest_id: uuid.UUID
    run_id: str
    table_id: int
    attempt_number: int
    policy_version_id: str
    window_id: uuid.UUID
    storage_layer: str
    storage_object_identity: str
    expected_row_count: int
    expected_key_fingerprint: str
    schema_fingerprint: str
    manifest_fingerprint: str
    expected_content_fingerprint: Optional[str] = None
    created_at: Optional[datetime] = None

    @classmethod
    def build(
        cls,
        run_id: str,
        table_id: int,
        attempt_number: int,
        policy_version_id: str,
        window_id: uuid.UUID,
        storage_layer: str,
        storage_object_identity: str,
        expected_row_count: int,
        expected_key_fingerprint: str,
        schema_fingerprint: str,
        expected_content_fingerprint: Optional[str] = None,
        manifest_id: Optional[uuid.UUID] = None,
        created_at: Optional[datetime] = None,
    ) -> "AttemptPersistenceManifest":
        m_id = manifest_id or uuid.uuid4()
        safe_table_id = int(table_id)
        safe_attempt_number = int(attempt_number)
        safe_expected_row_count = int(expected_row_count)
        payload = {
            "manifest_id": str(m_id),
            "run_id": str(run_id),
            "table_id": safe_table_id,
            "attempt_number": safe_attempt_number,
            "policy_version_id": str(policy_version_id),
            "window_id": str(window_id),
            "storage_layer": str(storage_layer),
            "storage_object_identity": str(storage_object_identity),
            "expected_row_count": safe_expected_row_count,
            "expected_key_fingerprint": str(expected_key_fingerprint),
            "expected_content_fingerprint": str(expected_content_fingerprint) if expected_content_fingerprint else None,
            "schema_fingerprint": str(schema_fingerprint),
        }
        manifest_fingerprint = compute_deterministic_fingerprint(payload)
        return cls(
            manifest_id=m_id,
            run_id=str(run_id),
            table_id=safe_table_id,
            attempt_number=safe_attempt_number,
            policy_version_id=str(policy_version_id),
            window_id=window_id,
            storage_layer=str(storage_layer),
            storage_object_identity=str(storage_object_identity),
            expected_row_count=safe_expected_row_count,
            expected_key_fingerprint=str(expected_key_fingerprint),
            expected_content_fingerprint=str(expected_content_fingerprint) if expected_content_fingerprint else None,
            schema_fingerprint=str(schema_fingerprint),
            manifest_fingerprint=manifest_fingerprint,
            created_at=created_at,
        )


@dataclass(frozen=True)
class PhysicalReconciliationResult:
    """Authoritative physical reconciliation evaluation result persisted to metadata.cdc_attempt_reconciliation."""
    reconciliation_id: uuid.UUID
    manifest_id: uuid.UUID
    run_id: str
    table_id: int
    attempt_number: int
    policy_version_id: str
    window_id: uuid.UUID
    reconciliation_strategy: ReconciliationStrategy
    storage_layer: str
    is_commit_authoritative: bool
    expected_row_count: int
    physical_row_count: int
    row_count_delta: int
    missing_key_count: int
    unexpected_key_count: int
    duplicate_key_count: int
    null_key_count: int
    expected_key_fingerprint: str
    physical_key_fingerprint: str
    outcome: ReconciliationOutcome
    details: Dict[str, Any]
    reconciliation_fingerprint: str
    expected_content_fingerprint: Optional[str] = None
    physical_content_fingerprint: Optional[str] = None
    failure_code: Optional[str] = None
    reconciled_at: Optional[datetime] = None

    @classmethod
    def build(
        cls,
        manifest_id: uuid.UUID,
        run_id: str,
        table_id: int,
        attempt_number: int,
        policy_version_id: str,
        window_id: uuid.UUID,
        reconciliation_strategy: ReconciliationStrategy,
        storage_layer: str,
        is_commit_authoritative: bool,
        expected_row_count: int,
        physical_row_count: int,
        missing_key_count: int,
        unexpected_key_count: int,
        duplicate_key_count: int,
        null_key_count: int,
        expected_key_fingerprint: str,
        physical_key_fingerprint: str,
        outcome: ReconciliationOutcome,
        details: Optional[Dict[str, Any]] = None,
        expected_content_fingerprint: Optional[str] = None,
        physical_content_fingerprint: Optional[str] = None,
        failure_code: Optional[str] = None,
        reconciliation_id: Optional[uuid.UUID] = None,
        reconciled_at: Optional[datetime] = None,
    ) -> "PhysicalReconciliationResult":
        r_id = reconciliation_id or uuid.uuid4()
        safe_table_id = int(table_id)
        safe_attempt_number = int(attempt_number)
        safe_expected_row_count = int(expected_row_count)
        safe_physical_row_count = int(physical_row_count)
        safe_missing_key_count = int(missing_key_count)
        safe_unexpected_key_count = int(unexpected_key_count)
        safe_duplicate_key_count = int(duplicate_key_count)
        safe_null_key_count = int(null_key_count)
        delta = safe_physical_row_count - safe_expected_row_count
        clean_details = details or {}
        payload = {
            "reconciliation_id": str(r_id),
            "manifest_id": str(manifest_id),
            "run_id": str(run_id),
            "table_id": safe_table_id,
            "attempt_number": safe_attempt_number,
            "policy_version_id": str(policy_version_id),
            "window_id": str(window_id),
            "reconciliation_strategy": reconciliation_strategy.value,
            "storage_layer": str(storage_layer),
            "is_commit_authoritative": is_commit_authoritative,
            "expected_row_count": safe_expected_row_count,
            "physical_row_count": safe_physical_row_count,
            "row_count_delta": delta,
            "missing_key_count": safe_missing_key_count,
            "unexpected_key_count": safe_unexpected_key_count,
            "duplicate_key_count": safe_duplicate_key_count,
            "null_key_count": safe_null_key_count,
            "expected_key_fingerprint": str(expected_key_fingerprint),
            "physical_key_fingerprint": str(physical_key_fingerprint),
            "outcome": outcome.value,
            "failure_code": str(failure_code) if failure_code else None,
        }
        reconciliation_fingerprint = compute_deterministic_fingerprint(payload)
        return cls(
            reconciliation_id=r_id,
            manifest_id=manifest_id,
            run_id=str(run_id),
            table_id=safe_table_id,
            attempt_number=safe_attempt_number,
            policy_version_id=str(policy_version_id),
            window_id=window_id,
            reconciliation_strategy=reconciliation_strategy,
            storage_layer=str(storage_layer),
            is_commit_authoritative=is_commit_authoritative,
            expected_row_count=safe_expected_row_count,
            physical_row_count=safe_physical_row_count,
            row_count_delta=delta,
            missing_key_count=safe_missing_key_count,
            unexpected_key_count=safe_unexpected_key_count,
            duplicate_key_count=safe_duplicate_key_count,
            null_key_count=safe_null_key_count,
            expected_key_fingerprint=str(expected_key_fingerprint),
            physical_key_fingerprint=str(physical_key_fingerprint),
            expected_content_fingerprint=expected_content_fingerprint,
            physical_content_fingerprint=physical_content_fingerprint,
            outcome=outcome,
            failure_code=failure_code,
            details=clean_details,
            reconciliation_fingerprint=reconciliation_fingerprint,
            reconciled_at=reconciled_at,
        )


# ─── OBS-01 Models ────────────────────────────────────────────────────────────
class CDCCommitMode(str, Enum):
    """Authoritative modes of CDC watermark advancement and success certification."""
    BOOTSTRAP = "BOOTSTRAP"
    INCREMENTAL = "INCREMENTAL"
    EMPTY = "EMPTY"


@dataclass(frozen=True)
class CDCCommitEvent:
    """Immutable audit ledger record of an atomic cursor and success commit."""
    commit_id: uuid.UUID
    batch_log_id: int
    run_id: str
    table_id: int
    attempt_number: int
    policy_version_id: str
    reconciliation_id: uuid.UUID
    manifest_id: uuid.UUID
    window_id: uuid.UUID
    committed_cursor: Dict[str, Any]
    new_state_version: int
    policy_fingerprint: str
    window_fingerprint: str
    manifest_fingerprint: str
    reconciliation_fingerprint: str
    commit_mode: CDCCommitMode
    previous_cursor: Optional[Dict[str, Any]] = None
    previous_state_version: Optional[int] = None
    committed_at: Optional[datetime] = None


@dataclass(frozen=True)
class CDCCommitResult:
    """Outcome of an atomic OBS-01 commit transaction."""
    commit_id: uuid.UUID
    batch_log_id: int
    run_id: str
    table_id: int
    attempt_number: int
    policy_version_id: str
    reconciliation_id: uuid.UUID
    committed_cursor: CDCCursor
    new_state_version: int
    commit_mode: CDCCommitMode
    previous_cursor: Optional[CDCCursor] = None
    previous_state_version: Optional[int] = None
    already_committed: bool = False
    committed_at: Optional[datetime] = None


# ─── GOLD-ATOMIC-01 Models ───────────────────────────────────────────────────
class GoldPublicationStrategy(str, Enum):
    """Strategies for safe atomic publication of full-snapshot Gold state."""
    TRANSACTIONAL_SWAP = "TRANSACTIONAL_SWAP"
    TRANSACTIONAL_REPLACE = "TRANSACTIONAL_REPLACE"
    VERSIONED_POINTER_SWAP = "VERSIONED_POINTER_SWAP"
    DELTA_ACID_OVERWRITE = "DELTA_ACID_OVERWRITE"


class GoldPublicationStatus(str, Enum):
    """Status of a Gold snapshot publication attempt.

    Terminal states: PUBLISHED, ROLLED_BACK, FAILED (legacy), RECOVERY_FAILED.
    Non-terminal: PENDING (pre-DML), PUBLISHING (claim advanced, target TX in progress),
                  RECOVERY_REQUIRED (reaper target).
    No new rows use FAILED; it is preserved to protect existing records.
    """
    PENDING           = "PENDING"
    PUBLISHING        = "PUBLISHING"
    PUBLISHED         = "PUBLISHED"
    ROLLED_BACK       = "ROLLED_BACK"
    FAILED            = "FAILED"            # legacy terminal — no new writes
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    RECOVERY_FAILED   = "RECOVERY_FAILED"


@dataclass(frozen=True)
class GoldPublicationManifest:
    """Immutable audit ledger record of an atomic Gold snapshot publication."""
    publication_id: uuid.UUID
    run_id: str
    table_id: int
    attempt_number: int
    policy_version_id: str
    staging_object_identity: str
    target_object_identity: str
    publication_strategy: GoldPublicationStrategy
    reconciliation_id: uuid.UUID
    manifest_id: uuid.UUID
    published_row_count: int
    publication_fingerprint: str
    status: GoldPublicationStatus
    published_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


@dataclass(frozen=True)
class GoldPublicationResult:
    """Outcome of an atomic Gold full-snapshot publication."""
    publication_id: uuid.UUID
    run_id: str
    table_id: int
    attempt_number: int
    policy_version_id: str
    staging_object_identity: str
    target_object_identity: str
    publication_strategy: GoldPublicationStrategy
    reconciliation_id: uuid.UUID
    published_row_count: int
    status: GoldPublicationStatus
    published_at: Optional[datetime] = None




