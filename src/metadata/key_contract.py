"""KEY-01 source business-identity evidence and approval contract."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple

from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec
from src.exceptions import BusinessKeyRequiredError


class KeyEvidenceType(str, Enum):
    """How source discovery obtained a candidate key."""

    PRIMARY_KEY = "PRIMARY_KEY"
    UNIQUE_CONSTRAINT = "UNIQUE_CONSTRAINT"
    PROFILED_UNIQUE = "PROFILED_UNIQUE"
    PROFILED_COMPOSITE = "PROFILED_COMPOSITE"
    PROFILED_CONDITIONAL_KEY = "PROFILED_CONDITIONAL_KEY"
    PROFILED_LOWEST_DUP = "PROFILED_LOWEST_DUP"
    HEURISTIC = "HEURISTIC"
    NONE = "NONE"


class KeyApprovalState(str, Enum):
    """Whether candidate evidence is authorized to influence execution."""

    APPROVED = "APPROVED"
    UNAPPROVED = "UNAPPROVED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class KeyCandidate:
    """Immutable evidence found by source discovery or profiling."""

    columns: Tuple[str, ...]
    evidence_type: KeyEvidenceType
    confidence: int
    nullable: bool = False
    compatible: bool = True
    evidence_fingerprint: str = ""
    identity_mode: IdentityMode = IdentityMode.EXACT_KEY
    exception_values: Tuple[int, ...] = ()
    exception_values_fingerprint: str = ""
    # Backward-compatible name used by policy payloads and profiling evidence.
    # Keep both names in the contract while emitting one canonical fingerprint.
    exception_evidence_fingerprint: str = ""

    def __post_init__(self) -> None:
        normalized = tuple(str(column).strip() for column in self.columns)
        if any(not column for column in normalized):
            raise ValueError("Candidate key columns cannot contain empty elements")
        if len({column.lower() for column in normalized}) != len(normalized):
            raise ValueError("Candidate key columns cannot contain duplicates")
        if not 0 <= int(self.confidence) <= 100:
            raise ValueError("Candidate key confidence must be between 0 and 100")
        if self.evidence_type is not KeyEvidenceType.NONE and not normalized:
            raise ValueError("Candidate key evidence requires at least one column")
        if self.evidence_type is KeyEvidenceType.NONE and normalized:
            raise ValueError("NONE key evidence cannot contain columns")
        object.__setattr__(self, "columns", normalized)
        try:
            identity_mode = self.identity_mode if isinstance(self.identity_mode, IdentityMode) else IdentityMode(str(self.identity_mode).strip().upper())
        except ValueError as exc:
            raise ValueError(f"Unsupported key identity mode: {self.identity_mode!r}") from exc
        exception_values = Int64ExceptionCodec.normalize(self.exception_values)
        if identity_mode is IdentityMode.BOUNDED_EXCEPTION_KEY:
            if not normalized:
                raise ValueError("Bounded-exception identity requires a business key")
            if len(normalized) != 1:
                raise ValueError("Bounded-exception identity requires one business key column")
            if not exception_values:
                raise ValueError("Bounded-exception identity requires exception values")
            if self.evidence_type is not KeyEvidenceType.PROFILED_CONDITIONAL_KEY:
                raise ValueError(
                    "Bounded-exception identity requires PROFILED_CONDITIONAL_KEY evidence"
                )
            if int(self.confidence) < 95:
                raise ValueError(
                    "Bounded-exception identity requires key confidence >= 95"
                )
        elif exception_values:
            raise ValueError(
                "Exception values are only valid for BOUNDED_EXCEPTION_KEY identity"
            )
        exception_fingerprint = (
            Int64ExceptionCodec.fingerprint(exception_values) if exception_values else ""
        )
        supplied_fingerprint = (
            self.exception_values_fingerprint or self.exception_evidence_fingerprint
        )
        if (
            self.exception_values_fingerprint
            and self.exception_evidence_fingerprint
            and self.exception_values_fingerprint != self.exception_evidence_fingerprint
        ):
            raise ValueError("Exception-key evidence fingerprints do not match")
        if supplied_fingerprint and supplied_fingerprint != exception_fingerprint:
            raise ValueError("Exception-key evidence fingerprint does not match values")
        object.__setattr__(self, "identity_mode", identity_mode)
        object.__setattr__(self, "exception_values", exception_values)
        object.__setattr__(self, "exception_values_fingerprint", exception_fingerprint)
        object.__setattr__(self, "exception_evidence_fingerprint", exception_fingerprint)


@dataclass(frozen=True)
class ApprovedBusinessKey:
    """Source identity explicitly authorized for execution policy decisions."""

    columns: Tuple[str, ...]
    evidence_type: KeyEvidenceType
    evidence_fingerprint: str = ""
    identity_mode: IdentityMode = IdentityMode.EXACT_KEY
    exception_values: Tuple[int, ...] = ()
    exception_values_fingerprint: str = ""
    exception_evidence_fingerprint: str = ""

    def __post_init__(self) -> None:
        normalized = tuple(str(column).strip() for column in self.columns)
        if not normalized or any(not column for column in normalized):
            raise ValueError("Approved business key requires non-empty columns")
        if len({column.lower() for column in normalized}) != len(normalized):
            raise ValueError("Approved business key columns cannot contain duplicates")
        object.__setattr__(self, "columns", normalized)
        try:
            identity_mode = self.identity_mode if isinstance(self.identity_mode, IdentityMode) else IdentityMode(str(self.identity_mode).strip().upper())
        except ValueError as exc:
            raise ValueError(f"Unsupported key identity mode: {self.identity_mode!r}") from exc
        exception_values = Int64ExceptionCodec.normalize(self.exception_values)
        if identity_mode is IdentityMode.BOUNDED_EXCEPTION_KEY:
            if len(normalized) != 1:
                raise ValueError("Bounded-exception identity requires one business key column")
            if not exception_values:
                raise ValueError("Bounded-exception identity requires exception values")
            if self.evidence_type is not KeyEvidenceType.PROFILED_CONDITIONAL_KEY:
                raise ValueError(
                    "Bounded-exception identity requires PROFILED_CONDITIONAL_KEY evidence"
                )
            if not (self.exception_values_fingerprint or self.exception_evidence_fingerprint):
                calculated_fp = Int64ExceptionCodec.fingerprint(exception_values) if exception_values else ""
                if not calculated_fp:
                    raise ValueError(
                        "Bounded-exception identity requires exception evidence fingerprint"
                    )
                object.__setattr__(self, "exception_values_fingerprint", calculated_fp)
                object.__setattr__(self, "exception_evidence_fingerprint", calculated_fp)
        if identity_mode is not IdentityMode.BOUNDED_EXCEPTION_KEY and exception_values:
            raise ValueError(
                "Exception values are only valid for BOUNDED_EXCEPTION_KEY identity"
            )
        exception_fingerprint = (
            Int64ExceptionCodec.fingerprint(exception_values) if exception_values else ""
        )
        supplied_fingerprint = (
            self.exception_values_fingerprint or self.exception_evidence_fingerprint
        )
        if (
            self.exception_values_fingerprint
            and self.exception_evidence_fingerprint
            and self.exception_values_fingerprint != self.exception_evidence_fingerprint
        ):
            raise ValueError("Exception-key evidence fingerprints do not match")
        if supplied_fingerprint and supplied_fingerprint != exception_fingerprint:
            raise ValueError("Exception-key evidence fingerprint does not match values")
        object.__setattr__(self, "identity_mode", identity_mode)
        object.__setattr__(self, "exception_values", exception_values)
        object.__setattr__(self, "exception_values_fingerprint", exception_fingerprint)
        object.__setattr__(self, "exception_evidence_fingerprint", exception_fingerprint)


@dataclass(frozen=True)
class KeyApprovalPolicy:
    """Explicit qualification policy for source constraint evidence."""

    allow_unique_constraints: bool = True


@dataclass(frozen=True)
class KeyResolution:
    """Candidate evidence alongside its independent execution authority."""

    candidate: KeyCandidate
    approval_state: KeyApprovalState
    approved_key: Optional[ApprovedBusinessKey] = None

    @property
    def canonical_source_business_key(self) -> Optional[Tuple[str, ...]]:
        """Return the active source identity, or None when it is not approved."""
        if self.approval_state is not KeyApprovalState.APPROVED or not self.approved_key:
            return None
        return self.approved_key.columns


@dataclass(frozen=True)
class PromotionKeyContract:
    """Evidence carried during promotion without granting execution authority."""

    source_table_master_key: Optional[int]
    candidate_columns: Tuple[str, ...]
    evidence_type: Optional[str]
    approval_state: KeyApprovalState = KeyApprovalState.UNAPPROVED
    approved_source_business_key: Optional[Tuple[str, ...]] = None
    approved_key_evidence: Optional[str] = None
    approved_key_evidence_fingerprint: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    approval_revision: Optional[int] = None
    identity_mode: IdentityMode = IdentityMode.EXACT_KEY
    exception_values_payload: Optional[Dict[str, Any]] = None
    exception_values_fingerprint: Optional[str] = None
    exception_evidence_fingerprint: Optional[str] = None


def build_promotion_key_contract(
    master: Mapping,
    *,
    approve_physical_primary_key: bool = False,
    approved_by: Optional[str] = None,
    approved_at: Optional[datetime] = None,
) -> PromotionKeyContract:
    """Translate evidence and automate complete business-key provenance."""
    raw_columns = master.get("key_candidate_columns")
    if raw_columns is None and master.get("business_key"):
        raw_columns = str(master["business_key"]).split(",")
    columns = tuple(str(column).strip() for column in (raw_columns or ()) if str(column).strip())
    evidence_type = master.get("key_evidence_type") or master.get("key_source")
    raw_identity_mode = master.get("identity_mode") or master.get("key_identity_mode")
    if raw_identity_mode is None:
        evidence_hint = str(evidence_type or "").strip().upper()
        raw_identity_mode = (
            IdentityMode.BOUNDED_EXCEPTION_KEY.value
            if evidence_hint == KeyEvidenceType.PROFILED_CONDITIONAL_KEY.value
            else IdentityMode.EXACT_KEY.value
        )
    try:
        identity_mode = (
            raw_identity_mode
            if isinstance(raw_identity_mode, IdentityMode)
            else IdentityMode(str(raw_identity_mode).strip().upper())
        )
    except ValueError:
        identity_mode = IdentityMode.EXACT_KEY
    raw_exception_payload = (
        master.get("exception_values_payload")
        or master.get("conditional_exception_values_payload")
    )
    raw_exception_values = (
        master.get("exception_values")
        or master.get("conditional_exception_values")
        or master.get("exception_key_values")
    )
    if isinstance(raw_exception_payload, str) and raw_exception_payload.strip():
        import json

        try:
            raw_exception_payload = json.loads(raw_exception_payload)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid exception-key payload") from exc
    if raw_exception_payload is not None:
        exception_values = Int64ExceptionCodec.decode(raw_exception_payload)
    else:
        exception_values = Int64ExceptionCodec.normalize(raw_exception_values or ())
    exception_values_payload = (
        Int64ExceptionCodec.encode(exception_values) if exception_values else None
    )
    exception_values_fingerprint = (
        Int64ExceptionCodec.fingerprint(exception_values) if exception_values else None
    )
    supplied_exception_evidence_fingerprint = master.get("exception_evidence_fingerprint")
    base = PromotionKeyContract(
        source_table_master_key=master.get("table_master_key"),
        candidate_columns=columns,
        evidence_type=str(evidence_type).strip().upper() if evidence_type else None,
        identity_mode=identity_mode,
        exception_values_payload=exception_values_payload,
        exception_values_fingerprint=exception_values_fingerprint,
        exception_evidence_fingerprint=(
            str(supplied_exception_evidence_fingerprint).strip()
            if supplied_exception_evidence_fingerprint
            else exception_values_fingerprint
        ),
    )
    if not approve_physical_primary_key:
        return base

    # Older catalog rows can carry a legacy business_key value without the
    # completed profiling evidence required for execution authority.  Leave
    # those rows unapproved; do not manufacture provenance during promotion.
    if not columns or not evidence_type:
        return base

    normalized_evidence = {
        "PROFILED": KeyEvidenceType.PROFILED_UNIQUE.value,
        "UNIQUE": KeyEvidenceType.UNIQUE_CONSTRAINT.value,
    }.get(str(evidence_type).strip().upper(), str(evidence_type).strip().upper())
    if normalized_evidence == KeyEvidenceType.NONE.value:
        return base
    try:
        evidence_kind = KeyEvidenceType(normalized_evidence)
    except ValueError:
        return base

    actor = str(approved_by or "SYSTEM_PROFILE").strip()
    fingerprint = str(master.get("key_evidence_fingerprint") or "").strip()
    if not actor:
        actor = "SYSTEM_PROFILE"
    if approved_at is None:
        approved_at = datetime.utcnow()

    candidate = KeyCandidate(
        columns=columns,
        evidence_type=evidence_kind,
        confidence=int(master.get("key_confidence") or 0),
        nullable=master.get("key_candidate_nullable") is not False,
        compatible=master.get("key_candidate_compatible") is True,
        evidence_fingerprint=fingerprint,
        identity_mode=identity_mode,
        exception_values=exception_values,
        exception_evidence_fingerprint=(
            str(supplied_exception_evidence_fingerprint).strip()
            if supplied_exception_evidence_fingerprint
            else ""
        ),
    )
    resolution = resolve_key_candidate(candidate)
    approved = require_approved_business_key(resolution)
    if not fingerprint:
        raise ValueError("Only fingerprinted key evidence can be approved")
    if approved.evidence_type not in (
        KeyEvidenceType.PRIMARY_KEY,
        KeyEvidenceType.UNIQUE_CONSTRAINT,
        KeyEvidenceType.PROFILED_UNIQUE,
        KeyEvidenceType.PROFILED_COMPOSITE,
        KeyEvidenceType.PROFILED_CONDITIONAL_KEY,
    ):
        raise ValueError(f"Evidence type {approved.evidence_type} cannot be approved as a business key")

    return PromotionKeyContract(
        source_table_master_key=master.get("table_master_key"),
        candidate_columns=columns,
        evidence_type=approved.evidence_type.value,
        approval_state=KeyApprovalState.APPROVED,
        approved_source_business_key=approved.columns,
        approved_key_evidence=approved.evidence_type.value,
        approved_key_evidence_fingerprint=fingerprint,
        approved_by=actor,
        approved_at=approved_at,
        approval_revision=1,
        identity_mode=approved.identity_mode,
        exception_values_payload=(
            Int64ExceptionCodec.encode(approved.exception_values)
            if approved.exception_values else None
        ),
        exception_values_fingerprint=(approved.exception_values_fingerprint or None),
        exception_evidence_fingerprint=(approved.exception_evidence_fingerprint or None),
    )


def resolve_key_candidate(
    candidate: KeyCandidate,
    *,
    policy: Optional[KeyApprovalPolicy] = None,
    existing_approved: Optional[ApprovedBusinessKey] = None,
) -> KeyResolution:
    """Qualify evidence without allowing recrawls to rewrite existing approval."""
    effective_policy = policy or KeyApprovalPolicy()
    if existing_approved is not None:
        if candidate.evidence_fingerprint and existing_approved.evidence_fingerprint and candidate.evidence_fingerprint != existing_approved.evidence_fingerprint:
            return KeyResolution(candidate, KeyApprovalState.UNAPPROVED, existing_approved)
        return KeyResolution(candidate, KeyApprovalState.APPROVED, existing_approved)
    if candidate.nullable or not candidate.compatible:
        return KeyResolution(candidate, KeyApprovalState.REJECTED)
    qualifies = candidate.evidence_type is KeyEvidenceType.PRIMARY_KEY
    qualifies = qualifies or (
        candidate.evidence_type is KeyEvidenceType.UNIQUE_CONSTRAINT
        and effective_policy.allow_unique_constraints
    )
    qualifies = qualifies or (
        candidate.evidence_type in (
            KeyEvidenceType.PROFILED_UNIQUE,
            KeyEvidenceType.PROFILED_COMPOSITE,
            KeyEvidenceType.PROFILED_CONDITIONAL_KEY,
        )
        and candidate.confidence >= 90
        and not candidate.nullable
        and candidate.compatible
        and bool(candidate.evidence_fingerprint)
    )
    if qualifies:
        approved = ApprovedBusinessKey(
            candidate.columns,
            candidate.evidence_type,
            candidate.evidence_fingerprint,
            candidate.identity_mode,
            candidate.exception_values,
            candidate.exception_values_fingerprint,
        )
        return KeyResolution(candidate, KeyApprovalState.APPROVED, approved)
    return KeyResolution(candidate, KeyApprovalState.UNAPPROVED)


def require_approved_business_key(resolution: KeyResolution) -> ApprovedBusinessKey:
    """Return execution authority or fail closed for a keyed operation."""
    if resolution.approval_state is not KeyApprovalState.APPROVED or not resolution.approved_key:
        raise BusinessKeyRequiredError(
            "A keyed operation requires an approved source business key."
        )
    return resolution.approved_key


@dataclass(frozen=True)
class ResolvedTargetBusinessKey:
    """Canonical target business identity derived from approved source key and mapping."""

    table_id: int
    source_key_columns: Tuple[str, ...]
    target_key_columns: Tuple[str, ...]
    source_key_evidence_fingerprint: str
    mapping_fingerprint: str
    fingerprint: str
    identity_mode: IdentityMode = IdentityMode.EXACT_KEY
    exception_values: Tuple[int, ...] = ()
    exception_values_fingerprint: str = ""
    # Compatibility alias used by metadata policy payloads.
    exception_evidence_fingerprint: str = ""

    @classmethod
    def build(
        cls,
        table_id: int,
        source_key_columns: Tuple[str, ...],
        target_key_columns: Tuple[str, ...],
        source_key_evidence_fingerprint: str,
        mapping_fingerprint: str,
        identity_mode: IdentityMode = IdentityMode.EXACT_KEY,
        exception_values: Tuple[int, ...] = (),
    ) -> "ResolvedTargetBusinessKey":
        import hashlib
        import json

        normalized_src = tuple(str(c).strip() for c in source_key_columns)
        normalized_tgt = tuple(str(c).strip() for c in target_key_columns)
        mode = (
            identity_mode
            if isinstance(identity_mode, IdentityMode)
            else IdentityMode(str(identity_mode).strip().upper())
        )
        normalized_exceptions = Int64ExceptionCodec.normalize(exception_values)
        if mode is IdentityMode.BOUNDED_EXCEPTION_KEY:
            if len(normalized_src) != 1 or not normalized_exceptions:
                raise ValueError(
                    "BOUNDED_EXCEPTION_KEY target identity requires one source key "
                    "column and exception values"
                )
        elif normalized_exceptions:
            raise ValueError(
                "Target exception values require BOUNDED_EXCEPTION_KEY identity"
            )
        exception_fingerprint = (
            Int64ExceptionCodec.fingerprint(normalized_exceptions)
            if normalized_exceptions
            else ""
        )

        digest_payload = [
            f"table_id:{table_id}",
            f"source_key:{','.join(normalized_src)}",
            f"target_key:{','.join(normalized_tgt)}",
            f"source_fp:{source_key_evidence_fingerprint}",
            f"mapping_fp:{mapping_fingerprint}",
            f"identity_mode:{mode.value}",
            f"exception_fp:{exception_fingerprint}",
        ]
        fingerprint = hashlib.sha256(json.dumps(digest_payload).encode("utf-8")).hexdigest()
        return cls(
            table_id=table_id,
            source_key_columns=normalized_src,
            target_key_columns=normalized_tgt,
            source_key_evidence_fingerprint=source_key_evidence_fingerprint,
            mapping_fingerprint=mapping_fingerprint,
            fingerprint=fingerprint,
            identity_mode=mode,
            exception_values=normalized_exceptions,
            exception_values_fingerprint=exception_fingerprint,
            exception_evidence_fingerprint=exception_fingerprint,
        )


@dataclass(frozen=True)
class ResolvedTableContract:
    """
    Authoritative immutable execution contract materialized exclusively from a promoted policy version.
    Binds table_id, policy_version_id, version_number, policy_fingerprint, and all constituent contracts.
    """

    table_id: int
    policy_version_id: str
    version_number: int
    policy_fingerprint: str
    approved_source_business_key: Optional[ApprovedBusinessKey]
    resolved_mappings: object
    canonical_target_business_key: Optional[ResolvedTargetBusinessKey]
    contract_payload: dict
    contract_fingerprint: str
    cdc_policy: Optional[Any] = None
    identity_mode: IdentityMode = IdentityMode.EXACT_KEY
    exception_values: Tuple[int, ...] = ()
    frozen_capability_evidence: Optional[dict] = None

    @property
    def target_conditional_key_capable(self) -> bool:
        """Whether the frozen target capability evidence certifies support."""
        return bool(
            self.frozen_capability_evidence
            and self.frozen_capability_evidence.get("capable") is True
        )

    @classmethod
    def from_promoted_policy(
        cls,
        promoted_policy: Any,
        approved_source_business_key: Optional[ApprovedBusinessKey],
        resolved_mappings: object,
        canonical_target_business_key: Optional[ResolvedTargetBusinessKey],
        cdc_policy: Optional[Any] = None,
        identity_mode: Optional[IdentityMode] = None,
        exception_values: Optional[Tuple[int, ...]] = None,
        frozen_capability_evidence: Optional[dict] = None,
    ) -> "ResolvedTableContract":
        source_identity = getattr(approved_source_business_key, "identity_mode", None)
        mode = identity_mode or source_identity or IdentityMode.EXACT_KEY
        if not isinstance(mode, IdentityMode):
            mode = IdentityMode(str(mode).strip().upper())
        values = (
            Int64ExceptionCodec.normalize(exception_values)
            if exception_values is not None
            else Int64ExceptionCodec.normalize(
                getattr(approved_source_business_key, "exception_values", ())
            )
        )
        return cls(
            table_id=promoted_policy.table_id,
            policy_version_id=str(promoted_policy.policy_version_id),
            version_number=int(promoted_policy.version_number),
            policy_fingerprint=promoted_policy.policy_fingerprint,
            approved_source_business_key=approved_source_business_key,
            resolved_mappings=resolved_mappings,
            canonical_target_business_key=canonical_target_business_key,
            contract_payload=promoted_policy.contract_payload,
            contract_fingerprint=promoted_policy.contract_fingerprint,
            cdc_policy=cdc_policy,
            identity_mode=mode,
            exception_values=values,
            frozen_capability_evidence=frozen_capability_evidence,
        )



