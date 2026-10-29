# src/metadata/policy_governance.py
"""
META-01 — Policy Version Governance Service.

Authority for:
- Candidate contract assembly & drift detection
- Transactional policy promotion with advisory locks
- Authoritative execution contract materialization from promoted snapshots
- Runtime policy version fencing
"""

from typing import Optional, Dict, Any, List, Tuple
import logging
import uuid
import json

from src.connections.connection_manager import ConnectionManager
from src.transform.column_mapper import ColumnMapper
from src.metadata.target_key_resolver import TargetKeyResolver
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings, ResolvedColumnMapping
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec
from src.metadata.policy_models import (
    CandidateTableContract,
    PromotedTablePolicy,
    PromotionRequest,
    PromotionResult,
    PolicyActorType,
    CDCStrategy,
    SourceMutationMode,
    CursorFinalityMode,
    CursorLogicalType,
    ResolvedCDCPolicy,
    compute_deterministic_fingerprint,
)
from src.exceptions import (
    PolicyNotPromotedError,
    PolicyAlreadyCurrentError,
    PolicyVersionSupersededError,
    PolicyEvidenceDriftError,
    PolicyFingerprintMismatchError,
    PolicyPromotionConflictError,
    PolicyVersionNotFoundError,
    PolicyActorRequiredError,
    PolicyActorUnauthorizedError,
    PolicyTransitionInvalidError,
    PolicyDependencyInvalidError,
    BusinessKeyRequiredError,
    TargetConstraintRequiredError,
    TargetKeyColumnMissingError,
)
from src.models.principal import AuthenticatedPrincipal

logger = logging.getLogger(__name__)


def _policy_row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[index]
    except (IndexError, TypeError):
        return default


def _parse_key_columns(raw_columns: Any) -> Tuple[str, ...]:
    if isinstance(raw_columns, str):
        try:
            raw_columns = json.loads(raw_columns)
        except (TypeError, ValueError) as exc:
            raise BusinessKeyRequiredError(
                "Approved source business key must be a JSON array or list."
            ) from exc
    if not isinstance(raw_columns, (list, tuple)):
        raise BusinessKeyRequiredError("Approved source business key is missing.")
    columns = tuple(str(column).strip() for column in raw_columns)
    if not columns or any(not column for column in columns):
        raise BusinessKeyRequiredError("Approved source business key contains empty columns.")
    if len({column.lower() for column in columns}) != len(columns):
        raise BusinessKeyRequiredError("Approved source business key contains duplicate columns.")
    return columns


def _validate_sequence_cursor_types(strategy: Any, cursor_types: Any) -> None:
    """Require one exact numeric cursor for sequence high-watermark CDC."""
    strategy_name = str(getattr(strategy, "value", strategy) or "").upper()
    if strategy_name != CDCStrategy.SEQUENCE_HIGH_WATERMARK.value:
        return
    normalized = tuple(str(getattr(t, "value", t) or "").upper() for t in (cursor_types or ()))
    allowed_types = {CursorLogicalType.INT64.value, CursorLogicalType.DECIMAL.value}
    if len(normalized) != 1 or normalized[0] not in allowed_types:
        raise PolicyDependencyInvalidError(
            "SEQUENCE_HIGH_WATERMARK requires exactly one INT64 or DECIMAL numeric cursor type."
        )


def _approved_source_key_from_policy_row(table_id: int, row: Any) -> ApprovedBusinessKey:
    columns = _parse_key_columns(_policy_row_get(row, "approved_source_business_key", 8))
    evidence = str(_policy_row_get(row, "approved_key_evidence", 9) or "").strip().upper()
    approved_fingerprint = str(_policy_row_get(row, "approved_key_evidence_fingerprint", 10) or "").strip()
    approved_by = str(_policy_row_get(row, "key_approved_by", 11) or "").strip()
    approved_at = _policy_row_get(row, "key_approved_at", 12)
    approval_revision = _policy_row_get(row, "key_approval_revision", 13)
    candidate_nullable = _policy_row_get(row, "key_candidate_nullable", 14)
    candidate_compatible = _policy_row_get(row, "key_candidate_compatible", 15)
    current_fingerprint = str(_policy_row_get(row, "current_key_evidence_fingerprint", 16) or "").strip()
    identity_mode = IdentityMode.EXACT_KEY
    exception_values = ()
    # The exception identity is carried in the existing health-details JSON;
    # accept both dict rows and the two positional SELECT shapes used by this
    # service for backward compatibility.
    # The lock query used by promote_policy inserts five CDC fields before
    # cdc_health_details.  Prefer its explicit position for tuple rows so a
    # strategy such as FULL_SNAPSHOT is never mistaken for JSON evidence.
    detail_index = 22 if isinstance(row, (tuple, list)) and len(row) > 22 else 17
    detail_value = _policy_row_get(row, "cdc_health_details", detail_index)
    try:
        details = json.loads(detail_value) if isinstance(detail_value, str) else (detail_value or {})
        if str(details.get("identity_mode", "")).upper() == IdentityMode.BOUNDED_EXCEPTION_KEY.value:
            identity_mode = IdentityMode.BOUNDED_EXCEPTION_KEY
            exception_values = Int64ExceptionCodec.normalize(details.get("exception_values") or ())
    except (TypeError, ValueError):
        pass

    if not evidence or not approved_fingerprint or not approved_by or approved_at is None:
        raise BusinessKeyRequiredError(
            f"Table ID {table_id} lacks complete approved source business-key provenance."
        )
    if not approval_revision or int(approval_revision) <= 0:
        raise BusinessKeyRequiredError(
            f"Table ID {table_id} has an invalid source business-key approval revision."
        )
    if candidate_nullable is not False or candidate_compatible is not True:
        raise BusinessKeyRequiredError(
            f"Table ID {table_id} source business-key evidence is nullable or incompatible."
        )
    if not current_fingerprint or approved_fingerprint != current_fingerprint:
        raise BusinessKeyRequiredError(
            f"Table ID {table_id} source business-key evidence has drifted or is missing."
        )

    return ApprovedBusinessKey(
        columns=columns,
        evidence_type=KeyEvidenceType(evidence),
        evidence_fingerprint=approved_fingerprint,
        identity_mode=identity_mode,
        exception_values=exception_values,
        exception_values_fingerprint=(Int64ExceptionCodec.fingerprint(exception_values) if exception_values else ""),
    )


class PolicyGovernanceService:
    """
    Authoritative service governing table policy versions, promotions,
    historical execution materialization, and attempt policy fencing.
    """

    def __init__(
        self,
        conn_manager: ConnectionManager,
        mapper: Optional[ColumnMapper] = None,
        resolver: Optional[TargetKeyResolver] = None,
    ):
        self._conn_manager = conn_manager
        self._mapper = mapper or ColumnMapper(conn_manager)
        self._resolver = resolver or TargetKeyResolver(conn_manager)

    def set_candidate_cdc_policy(
        self,
        table_id: int,
        strategy: CDCStrategy,
        mutation_mode: SourceMutationMode,
        cursor_finality_mode: CursorFinalityMode,
        cursor_columns: Tuple[str, ...],
        cursor_types: Tuple[CursorLogicalType, ...],
        tie_breaker_columns: Optional[Tuple[str, ...]] = None,
        identity_mode: IdentityMode = IdentityMode.EXACT_KEY,
        exception_values: Optional[Tuple[int, ...]] = None,
        activation_state: str = "ACTIVE",
        capability_evidence: Optional[Dict[str, Any]] = None,
    ) -> ResolvedCDCPolicy:
        """
        Upserts an explicit candidate CDC policy into metadata.table_cdc_policy_candidate.
        Does not mutate active execution policy until promoted.
        """
        _validate_sequence_cursor_types(strategy, cursor_types)
        cdc_policy = ResolvedCDCPolicy.build(
            table_id=table_id,
            strategy=strategy,
            mutation_mode=mutation_mode,
            cursor_finality_mode=cursor_finality_mode,
            cursor_columns=cursor_columns,
            cursor_types=cursor_types,
            tie_breaker_columns=tie_breaker_columns,
            identity_mode=identity_mode,
            exception_values=exception_values,
            activation_state=activation_state,
            capability_evidence=capability_evidence,
        )

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metadata.table_cdc_policy_candidate (
                        table_id, policy_schema_version, strategy, mutation_mode,
                        cursor_finality_mode, cursor_columns, cursor_types,
                        tie_breaker_columns, candidate_revision, updated_at
                    ) VALUES (
                        %s, 1, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, 1, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (table_id) DO UPDATE SET
                        strategy = EXCLUDED.strategy,
                        mutation_mode = EXCLUDED.mutation_mode,
                        cursor_finality_mode = EXCLUDED.cursor_finality_mode,
                        cursor_columns = EXCLUDED.cursor_columns,
                        cursor_types = EXCLUDED.cursor_types,
                        tie_breaker_columns = EXCLUDED.tie_breaker_columns,
                        candidate_revision = metadata.table_cdc_policy_candidate.candidate_revision + 1,
                        updated_at = CURRENT_TIMESTAMP
                """, [
                    table_id,
                    strategy.value,
                    mutation_mode.value,
                    cursor_finality_mode.value,
                    json.dumps(list(cursor_columns)),
                    json.dumps([t.value for t in cursor_types]),
                    json.dumps(list(tie_breaker_columns or ())),
                ])
            conn.commit()

        return cdc_policy

    def _parse_cdc_row(
        self,
        table_id: int,
        cdc_row: Any,
        *,
        identity_mode: IdentityMode = IdentityMode.EXACT_KEY,
        exception_values: Tuple[int, ...] = (),
        activation_state: str = "ACTIVE",
        capability_evidence: Optional[Dict[str, Any]] = None,
    ) -> Optional[ResolvedCDCPolicy]:
        if cdc_row and isinstance(cdc_row[0], str) and cdc_row[0] in {s.value for s in CDCStrategy}:
            strategy = CDCStrategy(cdc_row[0])
            mutation_mode = SourceMutationMode(cdc_row[1])
            finality_mode = CursorFinalityMode(cdc_row[2])
            cursor_cols = tuple(cdc_row[3]) if isinstance(cdc_row[3], list) else tuple(json.loads(cdc_row[3]))
            cursor_types = tuple(CursorLogicalType(t) for t in (cdc_row[4] if isinstance(cdc_row[4], list) else json.loads(cdc_row[4])))
            tie_breakers = tuple(cdc_row[5]) if cdc_row[5] and isinstance(cdc_row[5], list) else (tuple(json.loads(cdc_row[5])) if cdc_row[5] else ())
            return ResolvedCDCPolicy.build(
                table_id=table_id,
                strategy=strategy,
                mutation_mode=mutation_mode,
                cursor_finality_mode=finality_mode,
                cursor_columns=cursor_cols,
                cursor_types=cursor_types,
                tie_breaker_columns=tie_breakers,
                identity_mode=identity_mode,
                exception_values=exception_values,
                activation_state=activation_state,
                capability_evidence=capability_evidence,
            )
        return None

    def _load_candidate_cdc_policy(self, table_id: int, cur: Optional[Any] = None) -> Optional[ResolvedCDCPolicy]:
        """Loads candidate CDC policy from metadata.table_cdc_policy_candidate if present."""
        if cur is not None:
            try:
                cur.execute("""
                    SELECT strategy, mutation_mode, cursor_finality_mode, cursor_columns, cursor_types, tie_breaker_columns
                    FROM metadata.table_cdc_policy_candidate
                    WHERE table_id = %s
                """, [table_id])
                row = cur.fetchone()
                if not row:
                    return None
                details = {}
                cur.execute("SELECT cdc_health_details FROM metadata.db_tables_etl WHERE table_id = %s", [table_id])
                raw_details = cur.fetchone()
                if raw_details:
                    value = raw_details[0] if not isinstance(raw_details, dict) else raw_details.get("cdc_health_details")
                    try:
                        details = json.loads(value) if isinstance(value, str) else (value or {})
                    except (TypeError, ValueError):
                        details = {}
                identity_mode = IdentityMode(str(details.get("identity_mode", IdentityMode.EXACT_KEY.value)).upper())
                exception_values = Int64ExceptionCodec.normalize(details.get("exception_values") or ())
                return self._parse_cdc_row(table_id, row, identity_mode=identity_mode, exception_values=exception_values, activation_state=details.get("activation_state", "ACTIVE"), capability_evidence=details.get("capability_evidence"))
            except Exception:
                return None

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur_internal:
                try:
                    cur_internal.execute("""
                        SELECT strategy, mutation_mode, cursor_finality_mode, cursor_columns, cursor_types, tie_breaker_columns
                        FROM metadata.table_cdc_policy_candidate
                        WHERE table_id = %s
                    """, [table_id])
                    row = cur_internal.fetchone()
                    if not row:
                        return None
                    cur_internal.execute("SELECT cdc_health_details FROM metadata.db_tables_etl WHERE table_id = %s", [table_id])
                    raw_details = cur_internal.fetchone()
                    details = {}
                    if raw_details:
                        value = raw_details[0] if not isinstance(raw_details, dict) else raw_details.get("cdc_health_details")
                        try:
                            details = json.loads(value) if isinstance(value, str) else (value or {})
                        except (TypeError, ValueError):
                            details = {}
                    identity_mode = IdentityMode(str(details.get("identity_mode", IdentityMode.EXACT_KEY.value)).upper())
                    exception_values = Int64ExceptionCodec.normalize(details.get("exception_values") or ())
                    return self._parse_cdc_row(table_id, row, identity_mode=identity_mode, exception_values=exception_values, activation_state=details.get("activation_state", "ACTIVE"), capability_evidence=details.get("capability_evidence"))
                except Exception:
                    pass
        return None

    def build_candidate_contract(
        self,
        table_id: int,
        table_metadata_row: Optional[Tuple[Any, ...]] = None,
        cdc_policy: Optional[ResolvedCDCPolicy] = None,
        cur: Optional[Any] = None,
    ) -> CandidateTableContract:
        """
        Assembles a CandidateTableContract from mutable KEY, MAP, and CDC metadata.
        This candidate contract has not been promoted and cannot be used by execution attempts.
        """
        if table_metadata_row is None:
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur_internal:
                    cur_internal.execute("""
                        SELECT
                            e.table_id, e.mapping_policy, e.source_tag, e.source_table,
                            e.target_tag, e.target_schema, e.target_table, e.current_policy_version_id,
                            e.approved_source_business_key, e.approved_key_evidence,
                            e.approved_key_evidence_fingerprint, e.key_approved_by,
                            e.key_approved_at, e.key_approval_revision,
                            m.key_candidate_nullable, m.key_candidate_compatible,
                            m.key_evidence_fingerprint AS current_key_evidence_fingerprint,
                            e.cdc_health_details
                        FROM metadata.db_tables_etl e
                        LEFT JOIN metadata.db_tables m
                          ON m.table_master_key = e.source_table_master_key
                        WHERE e.table_id = %s
                    """, [table_id])
                    table_metadata_row = cur_internal.fetchone()
                    if not table_metadata_row:
                        raise PolicyVersionNotFoundError(f"Table ID {table_id} not found in metadata.db_tables_etl.")

                    if cdc_policy is None:
                        cdc_policy = self._load_candidate_cdc_policy(table_id, cur=cur_internal)
        else:
            if cdc_policy is None:
                cdc_policy = self._load_candidate_cdc_policy(table_id, cur=cur)

        mapping_policy_str = table_metadata_row[1] or "EXPLICIT"
        mapping_policy = MappingPolicy(mapping_policy_str.upper())
        try:
            approved_source_key = _approved_source_key_from_policy_row(table_id, table_metadata_row)
        except BusinessKeyRequiredError:
            # A full snapshot overwrite is the explicit safe fallback for a
            # discovered table without a certified merge identity.  CDC still
            # requires an approved key and therefore remains fail-closed.
            if cdc_policy and cdc_policy.strategy != CDCStrategy.FULL_SNAPSHOT:
                raise
            approved_source_key = None

        resolved_mappings = self._mapper.resolve_table_mappings(
            table_id=table_id,
            mapping_policy=mapping_policy,
        )

        resolved_target_key = None
        if approved_source_key:
            resolved_target_key = self._resolver.resolve_target_business_key(
                table_id=table_id,
                approved_source_key=approved_source_key,
                resolved_mappings=resolved_mappings,
            )

        return CandidateTableContract.build(
            table_id=table_id,
            mapping_policy=mapping_policy,
            approved_source_business_key=approved_source_key,
            resolved_mappings=resolved_mappings,
            canonical_target_business_key=resolved_target_key,
            cdc_policy=cdc_policy,
        )

    def demote_cdc_policy(
        self,
        *,
        table_id: int,
        expected_current_policy_version_id: str,
        actor_id: str,
        reason: str,
    ) -> PromotionResult:
        """Create a non-CDC policy version for future runs only."""
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext('table_policy:' || %s))",
                    [str(table_id)],
                )
                cur.execute(
                    "SELECT current_policy_version_id FROM metadata.db_tables_etl "
                    "WHERE table_id = %s FOR UPDATE",
                    [table_id],
                )
                row = cur.fetchone()
                if not row or str(row[0]) != str(expected_current_policy_version_id):
                    raise PolicyVersionSupersededError(
                        f"Table {table_id} policy changed before CDC demotion."
                    )
                cur.execute(
                    "DELETE FROM metadata.table_cdc_policy_candidate WHERE table_id = %s",
                    [table_id],
                )
            conn.commit()

        candidate = self.build_candidate_contract(table_id)
        principal = AuthenticatedPrincipal(actor_id=actor_id, actor_type="SYSTEM", roles={"ADMIN", "admin"})
        return self.promote_policy(
            PromotionRequest(
                table_id=table_id,
                expected_candidate_fingerprint=candidate.candidate_fingerprint,
                actor_type=PolicyActorType.SYSTEM.value,
                actor_id=actor_id,
                reason=reason,
                expected_current_policy_version_id=str(expected_current_policy_version_id),
                authenticated_principal=principal,
            ),
            principal=principal,
        )

    def _validate_profiled_cdc_activation(self, candidate_contract, table_row):
        """
        Validate that candidate contract CDC policy and Tier-2 CDC evidence meet
        health, confidence, and cursor nullity invariants before promotion.
        Supports both positional database tuple and dictionary shapes.
        """
        if not candidate_contract:
            raise PolicyDependencyInvalidError("Missing candidate contract for CDC activation.")

        cdc_policy = getattr(candidate_contract, "cdc_policy", None)
        cdc_strat = getattr(cdc_policy, "strategy", None)
        strat_str = str(getattr(cdc_strat, "value", cdc_strat) or "").upper()
        _validate_sequence_cursor_types(cdc_strat, getattr(cdc_policy, "cursor_types", ()))
        if strat_str in ("FULL_SNAPSHOT", "FULL_SOURCE_UPSERT", ""):
            return True

        if isinstance(table_row, dict):
            health = str(table_row.get("cdc_health") or "").upper()
            source = str(table_row.get("cdc_source") or "").upper()
            raw_details = table_row.get("cdc_health_details") or {}
            try:
                confidence = int(table_row.get("cdc_confidence") or 0)
            except Exception:
                confidence = 0
            try:
                null_rows = int(table_row.get("cdc_null_rows") or 0)
            except Exception:
                null_rows = 0
        elif isinstance(table_row, (tuple, list)):
            # Extended CDC indices in SELECT query:
            # 17: cdc_strategy, 18: cdc_source, 19: cdc_confidence, 20: cdc_health, 21: cdc_null_rows
            source = str(table_row[18] or "").upper() if len(table_row) > 18 else ""
            try:
                confidence = int(table_row[19] or 0) if len(table_row) > 19 else 0
            except Exception:
                confidence = 0
            health = str(table_row[20] or "").upper() if len(table_row) > 20 else ""
            raw_details = table_row[22] if len(table_row) > 22 else {}
            try:
                null_rows = int(table_row[21] or 0) if len(table_row) > 21 else 0
            except Exception:
                null_rows = 0
        else:
            return True

        try:
            details = json.loads(raw_details) if isinstance(raw_details, str) else dict(raw_details or {})
        except (TypeError, ValueError) as exc:
            raise PolicyDependencyInvalidError("CDC health evidence is not valid JSON.") from exc
        if str(details.get("operator_cdc_override") or "").upper() == "OFF":
            raise PolicyDependencyInvalidError(
                "CDC activation rejected because the operator explicitly disabled CDC."
            )

        if health in ("CRITICAL", "ERROR", "UNHEALTHY", "FAILED"):
            raise PolicyDependencyInvalidError(
                f"CDC activation rejected: CDC health is '{health}' for strategy '{strat_str}'."
            )

        if source == "PROFILED" and confidence < 50:
            raise PolicyDependencyInvalidError(
                f"CDC activation rejected: Profiled CDC confidence ({confidence}%) is below 50% threshold."
            )

        if null_rows > 0:
            raise PolicyDependencyInvalidError(
                f"CDC activation rejected: CDC cursor contains {null_rows} null rows; clean cursor evidence required."
            )

        return True

    def promote_policy(
        self,
        request: PromotionRequest,
        principal: Optional[AuthenticatedPrincipal] = None,
    ) -> PromotionResult:
        """
        Promotes a candidate contract to authoritative versioned status inside a transaction
        with advisory locking. Detects evidence drift, no-op changes, and records immutable audit logs.
        """
        if not request.actor_id or not str(request.actor_id).strip():
            raise PolicyActorRequiredError("Actor ID is required for policy promotion.")

        valid_actors = {a.value for a in PolicyActorType}
        if str(request.actor_type).upper() not in valid_actors:
            raise PolicyActorRequiredError(
                f"Invalid actor type '{request.actor_type}'. Must be one of {valid_actors}."
            )

        auth_principal = principal or getattr(request, "authenticated_principal", None)
        if auth_principal is None:
            try:
                import streamlit as st
                user = st.session_state.get("user")
                if user and isinstance(user, dict):
                    auth_principal = AuthenticatedPrincipal(
                        actor_id=str(user.get("username") or user.get("id") or "anonymous"),
                        actor_type="USER",
                        roles={str(user.get("role", "")).lower(), str(user.get("role", "")).upper()},
                        username=user.get("username"),
                        user_id=str(user.get("id")) if user.get("id") else None,
                    )
            except Exception:
                pass

        if auth_principal is None:
            raise PolicyActorUnauthorizedError(
                "Step 4 policy promotion requires an authenticated server-owned principal. Unauthenticated request rejected."
            )

        if request.actor_id and auth_principal.actor_id and str(request.actor_id).strip() != str(auth_principal.actor_id).strip():
            raise PolicyActorUnauthorizedError(
                f"Forged policy promotion request: request actor_id '{request.actor_id}' "
                f"does not match authenticated principal '{auth_principal.actor_id}'."
            )

        is_admin = auth_principal.has_role("admin") or auth_principal.has_role("ADMIN")
        is_system = (auth_principal.actor_type or "").upper() == "SYSTEM"
        if not (is_admin or is_system):
            raise PolicyActorUnauthorizedError(
                f"Principal '{auth_principal.actor_id}' with roles {auth_principal.roles} is not authorized for policy promotion. "
                "ADMIN role or SYSTEM actor_type required."
            )

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Acquire transaction-level advisory lock on table_id
                cur.execute("SELECT pg_advisory_xact_lock(hashtext('table_policy:' || %s))", [str(request.table_id)])

                # 2. Lock table row in db_tables_etl
                cur.execute("""
                    SELECT
                        e.table_id, e.mapping_policy, e.source_tag, e.source_table,
                        e.target_tag, e.target_schema, e.target_table, e.current_policy_version_id,
                        e.approved_source_business_key, e.approved_key_evidence,
                        e.approved_key_evidence_fingerprint, e.key_approved_by,
                        e.key_approved_at, e.key_approval_revision,
                        m.key_candidate_nullable, m.key_candidate_compatible,
                        m.key_evidence_fingerprint AS current_key_evidence_fingerprint,
                        e.cdc_strategy, e.cdc_source, e.cdc_confidence, e.cdc_health, e.cdc_null_rows
                        , e.cdc_health_details
                    FROM metadata.db_tables_etl e
                    LEFT JOIN metadata.db_tables m
                      ON m.table_master_key = e.source_table_master_key
                    WHERE e.table_id = %s
                    FOR UPDATE OF e
                """, [request.table_id])
                table_row = cur.fetchone()
                if not table_row:
                    raise PolicyVersionNotFoundError(f"Table ID {request.table_id} not found in db_tables_etl.")

                target_tag = table_row[4]
                target_schema = table_row[5] or "landing"
                target_table = table_row[6] or ""
                current_policy_version_id = table_row[7]

                # 3. Build current candidate contract using locked metadata snapshot
                try:
                    candidate = self.build_candidate_contract(
                        table_id=request.table_id,
                        table_metadata_row=table_row,
                        cur=cur,
                    )
                except Exception as e:
                    self._record_audit_event(
                        cur=cur,
                        request=request,
                        outcome="REJECTED",
                        policy_version_id=None,
                        failure_code="POLICY_DEPENDENCY_INVALID",
                        details={"error": str(e)},
                    )
                    conn.commit()
                    raise PolicyDependencyInvalidError(f"Candidate contract build failed: {e}") from e


                if candidate.candidate_fingerprint != request.expected_candidate_fingerprint:
                    self._record_audit_event(
                        cur=cur,
                        request=request,
                        outcome="REJECTED",
                        policy_version_id=None,
                        failure_code="POLICY_EVIDENCE_DRIFT",
                        details={
                            "expected": request.expected_candidate_fingerprint,
                            "actual": candidate.candidate_fingerprint,
                        },
                    )
                    conn.commit()
                    raise PolicyEvidenceDriftError(
                        f"Candidate evidence drifted for table_id {request.table_id}. "
                        f"Expected fingerprint {request.expected_candidate_fingerprint}, got {candidate.candidate_fingerprint}."
                    )

                # 4. Validate CDC profile invariants (health, confidence, nullity)
                try:
                    self._validate_profiled_cdc_activation(candidate, table_row)
                except PolicyDependencyInvalidError as e:
                    self._record_audit_event(
                        cur=cur,
                        request=request,
                        outcome="REJECTED",
                        policy_version_id=None,
                        failure_code="POLICY_DEPENDENCY_INVALID",
                        details={"error": str(e)},
                    )
                    conn.commit()
                    raise

                # Physical target readiness is intentionally deferred to the
                # landing-job preflight. Policy promotion freezes source key,
                # mappings, and CDC evidence even when the target table does
                # not exist yet; no target DML is permitted before that
                # preflight certifies/provisions the target schema.

                # 5. Check if candidate matches current promoted version (No-Op check)
                prev_version_number: Optional[int] = None
                if current_policy_version_id:
                    cur.execute("""
                        SELECT version_number, policy_fingerprint
                        FROM metadata.table_policy_version
                        WHERE policy_version_id = %s
                    """, [current_policy_version_id])
                    curr_ver_row = cur.fetchone()
                    if curr_ver_row:
                        prev_version_number = curr_ver_row[0]
                        if curr_ver_row[1] == candidate.candidate_fingerprint:
                            self._record_audit_event(
                                cur=cur,
                                request=request,
                                outcome="REJECTED",
                                policy_version_id=current_policy_version_id,
                                failure_code="POLICY_ALREADY_CURRENT",
                                details={"current_version": prev_version_number},
                            )
                            conn.commit()
                            raise PolicyAlreadyCurrentError(
                                f"Policy candidate for table_id {request.table_id} is already current at version {prev_version_number}."
                            )

                new_version_number = (prev_version_number + 1) if prev_version_number else 1
                new_policy_version_id = str(uuid.uuid4())

                # 6. Insert new immutable policy version
                cur.execute("""
                    INSERT INTO metadata.table_policy_version (
                        policy_version_id,
                        table_id,
                        version_number,
                        policy_schema_version,
                        contract_payload,
                        source_key_fingerprint,
                        mapping_fingerprint,
                        target_key_fingerprint,
                        contract_fingerprint,
                        policy_fingerprint,
                        promoted_at,
                        promoted_by_type,
                        promoted_by_id,
                        promotion_reason,
                        supersedes_version_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s)
                    RETURNING promoted_at;
                """, [
                    new_policy_version_id,
                    request.table_id,
                    new_version_number,
                    1,
                    json.dumps(candidate.contract_payload),
                    (candidate.approved_source_business_key.evidence_fingerprint
                     if candidate.approved_source_business_key else ""),
                    candidate.resolved_mappings.fingerprint,
                    (candidate.canonical_target_business_key.fingerprint
                     if candidate.canonical_target_business_key else ""),
                    candidate.contract_fingerprint,
                    candidate.candidate_fingerprint,
                    str(request.actor_type).upper(),
                    request.actor_id,
                    request.reason,
                    current_policy_version_id,
                ])
                promoted_at = cur.fetchone()[0]

                # 7. Update pointer, authority, and active state atomically in db_tables_etl
                cdc_policy = candidate.cdc_policy
                is_executable_cdc = bool(
                    cdc_policy
                    and cdc_policy.strategy in (
                        CDCStrategy.SEQUENCE_HIGH_WATERMARK,
                        CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
                    )
                )
                cur.execute("""
                    UPDATE metadata.db_tables_etl
                    SET current_policy_version_id = %s,
                        execution_authority_mode = 'GOVERNED',
                        is_cdc = %s,
                        cdc_strategy = %s,
                        cdc_provider = %s,
                        cdc_columns = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE table_id = %s
                      AND (execution_authority_mode = 'BLOCKED' OR (%s IS NOT NULL AND execution_authority_mode = 'GOVERNED'))
                """, [
                    new_policy_version_id,
                    "y" if is_executable_cdc else "n",
                    cdc_policy.strategy.value if cdc_policy else "FULL_SNAPSHOT",
                    cdc_policy.strategy.value if cdc_policy else "FULL_SNAPSHOT",
                    ", ".join(cdc_policy.cursor_columns) if cdc_policy else None,
                    request.table_id,
                    current_policy_version_id,
                ])
                if cur.rowcount == 0 or (isinstance(cur.rowcount, int) and cur.rowcount != 1):
                    raise PolicyTransitionInvalidError(
                        f"Table ID {request.table_id} authority transition failed: "
                        f"table must transition from 'BLOCKED' to 'GOVERNED' with exactly one affected row. "
                        f"Affected rows: {cur.rowcount}"
                    )

                # 8. Record ACCEPTED audit event
                self._record_audit_event(
                    cur=cur,
                    request=request,
                    outcome="ACCEPTED",
                    policy_version_id=new_policy_version_id,
                    failure_code=None,
                    details={
                        "version_number": new_version_number,
                        "previous_version": prev_version_number,
                        "contract_fingerprint": candidate.contract_fingerprint,
                        "policy_fingerprint": candidate.candidate_fingerprint,
                    },
                )

                conn.commit()

        return PromotionResult(
            table_id=request.table_id,
            previous_version_number=prev_version_number,
            new_version_number=new_version_number,
            policy_version_id=new_policy_version_id,
            policy_fingerprint=candidate.candidate_fingerprint,
            promoted_at=promoted_at,
        )

    def _record_audit_event(
        self,
        cur: Any,
        request: PromotionRequest,
        outcome: str,
        policy_version_id: Optional[str],
        failure_code: Optional[str],
        details: Optional[Dict[str, Any]],
    ) -> None:
        """Helper to write an audit entry into metadata.policy_promotion_audit."""
        cur.execute("""
            INSERT INTO metadata.policy_promotion_audit (
                promotion_request_id,
                table_id,
                expected_candidate_fingerprint,
                outcome,
                policy_version_id,
                failure_code,
                actor_type,
                actor_id,
                reason,
                details,
                requested_at,
                completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, [
            str(uuid.uuid4()),
            request.table_id,
            request.expected_candidate_fingerprint,
            outcome,
            policy_version_id,
            failure_code,
            str(request.actor_type).upper(),
            request.actor_id,
            request.reason,
            json.dumps(details or {}),
        ])

    def get_current_promoted_policy(self, table_id: int) -> PromotedTablePolicy:
        """Fetches the authoritative current promoted policy version for a table."""
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.current_policy_version_id
                    FROM metadata.db_tables_etl t
                    WHERE t.table_id = %s
                """, [table_id])
                row = cur.fetchone()
                if not row or not row[0]:
                    raise PolicyNotPromotedError(
                        f"Table ID {table_id} has no promoted policy version in metadata.db_tables_etl."
                    )

                policy_version_id = row[0]
                cur.execute("""
                    SELECT 
                        policy_version_id, table_id, version_number, policy_schema_version,
                        contract_payload, source_key_fingerprint, mapping_fingerprint,
                        target_key_fingerprint, contract_fingerprint, policy_fingerprint,
                        promoted_at, promoted_by_type, promoted_by_id, promotion_reason,
                        supersedes_version_id
                    FROM metadata.table_policy_version
                    WHERE policy_version_id = %s
                """, [policy_version_id])
                prow = cur.fetchone()
                if not prow:
                    raise PolicyVersionNotFoundError(
                        f"Policy version {policy_version_id} referenced by table_id {table_id} not found."
                    )

        payload = prow[4] if isinstance(prow[4], dict) else json.loads(prow[4])
        return PromotedTablePolicy(
            policy_version_id=str(prow[0]),
            table_id=prow[1],
            version_number=prow[2],
            policy_schema_version=prow[3],
            contract_payload=payload,
            source_key_fingerprint=prow[5],
            mapping_fingerprint=prow[6],
            target_key_fingerprint=prow[7],
            contract_fingerprint=prow[8],
            policy_fingerprint=prow[9],
            promoted_at=prow[10],
            promoted_by_type=prow[11],
            promoted_by_id=prow[12],
            promotion_reason=prow[13],
            supersedes_version_id=str(prow[14]) if prow[14] else None,
        )

    def materialize_execution_contract(self, table_id: int) -> ResolvedTableContract:
        """
        Materializes the authoritative execution contract from the promoted snapshot.
        Verifies payload integrity and cryptographic fingerprints against tampering.
        """
        promoted = self.get_current_promoted_policy(table_id)
        payload = promoted.contract_payload

        # Re-verify contract and policy fingerprints against payload
        recomputed_contract_fp = compute_deterministic_fingerprint(payload)
        policy_payload = {
            "policy_schema_version": promoted.policy_schema_version,
            "contract_fingerprint": recomputed_contract_fp,
        }
        recomputed_policy_fp = compute_deterministic_fingerprint(policy_payload)

        if recomputed_contract_fp != promoted.contract_fingerprint or recomputed_policy_fp != promoted.policy_fingerprint:
            raise PolicyFingerprintMismatchError(
                f"Policy integrity violation for table_id {table_id}, version {promoted.version_number}. "
                f"Stored policy fingerprint {promoted.policy_fingerprint} does not match recomputed {recomputed_policy_fp}."
            )

        # A keyless contract is valid only for a non-CDC full snapshot.
        # Never invent a source or target key to make an unsafe merge possible.
        src_key_dict = payload.get("source_business_key")
        approved_source_key = None
        if src_key_dict:
            raw_exception_payload = src_key_dict.get("exception_values_payload")
            raw_exception_values = ()
            if raw_exception_payload is not None:
                raw_exception_values = Int64ExceptionCodec.decode(raw_exception_payload)
            identity_mode = IdentityMode(str(
                src_key_dict.get("identity_mode", IdentityMode.EXACT_KEY.value)
            ).upper())
            approved_source_key = ApprovedBusinessKey(
                columns=tuple(src_key_dict.get("columns", [])),
                evidence_type=KeyEvidenceType(src_key_dict.get("evidence_type", "PRIMARY_KEY")),
                evidence_fingerprint=src_key_dict.get("evidence_fingerprint", ""),
                identity_mode=identity_mode,
                exception_values=raw_exception_values,
                exception_values_fingerprint=src_key_dict.get("exception_values_fingerprint", ""),
            )

        # Deserialize resolved mappings
        mapping_policy = MappingPolicy(payload.get("mapping_policy", "EXPLICIT"))
        raw_mappings = payload.get("mappings", [])
        resolved_mappings = ResolvedTableMappings.build(
            table_id=table_id,
            raw_mappings=raw_mappings,
            mapping_policy=mapping_policy,
        )

        # Deserialize canonical target key
        tgt_key_dict = payload.get("target_business_key")
        resolved_target_key = None
        if approved_source_key:
            if not tgt_key_dict:
                raise PolicyDependencyInvalidError(
                    f"Table ID {table_id} policy has a source key but no target key."
                )
            resolved_target_key = ResolvedTargetBusinessKey.build(
                table_id=table_id,
                source_key_columns=approved_source_key.columns,
                target_key_columns=tuple(tgt_key_dict.get("columns", [])),
                source_key_evidence_fingerprint=approved_source_key.evidence_fingerprint,
                mapping_fingerprint=resolved_mappings.fingerprint,
                identity_mode=approved_source_key.identity_mode,
                exception_values=approved_source_key.exception_values,
            )

        # Deserialize CDC policy if present
        cdc_policy_dict = payload.get("cdc_policy")
        cdc_policy: Optional[ResolvedCDCPolicy] = None
        if cdc_policy_dict:
            cdc_policy = ResolvedCDCPolicy.from_dict(cdc_policy_dict)
        if not approved_source_key and cdc_policy and cdc_policy.strategy != CDCStrategy.FULL_SNAPSHOT:
            raise PolicyDependencyInvalidError(
                f"Table ID {table_id} policy attempts CDC without an approved source business key."
            )
        if not approved_source_key and tgt_key_dict:
            raise PolicyDependencyInvalidError(
                f"Table ID {table_id} policy has a target key without an approved source key."
            )

        return ResolvedTableContract.from_promoted_policy(
            promoted_policy=promoted,
            approved_source_business_key=approved_source_key,
            resolved_mappings=resolved_mappings,
            canonical_target_business_key=resolved_target_key,
            cdc_policy=cdc_policy,
        )

    def activate_bounded_exception_cdc(
        self,
        table_id: int,
        actor_id: str = "SYSTEM_BOOTSTRAP",
        capability_evidence: Optional[Dict[str, Any]] = None,
    ) -> PromotionResult:
        """Promote sequence CDC after a verified bounded-identity bootstrap.

        Bootstrap remains a governed full snapshot.  Only a successful Gold
        publication may call this method; it converts the existing policy to
        an executable sequence window while preserving the typed exception
        payload in ``cdc_health_details``.  No new metadata table or operator
        action is required.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor(cursor_factory=getattr(__import__("psycopg2.extras", fromlist=["RealDictCursor"]), "RealDictCursor")) as cur:
                cur.execute(
                    """
                    SELECT e.table_id, e.source_tag, e.source_table, e.cdc_health_details,
                           e.approved_source_business_key, e.approved_key_evidence,
                           e.approved_key_evidence_fingerprint, e.key_approved_by,
                           e.key_approved_at, e.key_approval_revision,
                           m.key_candidate_nullable, m.key_candidate_compatible,
                           m.key_evidence_fingerprint AS current_key_evidence_fingerprint,
                           e.target_tag, e.target_schema, e.target_table
                    FROM metadata.db_tables_etl e
                    LEFT JOIN metadata.db_tables m
                      ON m.table_master_key = e.source_table_master_key
                    WHERE e.table_id = %s
                    FOR UPDATE OF e
                    """,
                    [table_id],
                )
                row = cur.fetchone()
                if not row:
                    raise PolicyVersionNotFoundError(f"Table ID {table_id} not found in db_tables_etl.")
                details_raw = row.get("cdc_health_details")
                try:
                    details = json.loads(details_raw) if isinstance(details_raw, str) else dict(details_raw or {})
                except (TypeError, ValueError) as exc:
                    raise PolicyDependencyInvalidError("Bounded CDC evidence is not valid JSON.") from exc
                if str(details.get("identity_mode", "")).upper() != IdentityMode.BOUNDED_EXCEPTION_KEY.value:
                    raise PolicyDependencyInvalidError("Bounded CDC activation requires BOUNDED_EXCEPTION_KEY identity.")
                exception_values = Int64ExceptionCodec.normalize(details.get("exception_values") or ())
                approved_key = _approved_source_key_from_policy_row(table_id, row)
                if approved_key.identity_mode is not IdentityMode.BOUNDED_EXCEPTION_KEY:
                    raise PolicyDependencyInvalidError("Approved source key is not bounded-exception identity.")
                evidence = dict(capability_evidence or details.get("capability_evidence") or {})
                if evidence.get("capable") is not True:
                    raise PolicyDependencyInvalidError(
                        "Bounded CDC activation requires verified target capability evidence."
                    )
                key_column = approved_key.columns[0]
                cur.execute(
                    """
                    SELECT COALESCE(c.data_type, e.source_column_datatype) AS data_type,
                           c.data_precision, c.data_scale
                    FROM metadata.db_columns_etl e
                    LEFT JOIN metadata.db_columns c
                      ON c.table_master_key = (
                           SELECT source_table_master_key FROM metadata.db_tables_etl WHERE table_id = e.table_id
                         )
                     AND LOWER(c.column_name) = LOWER(e.source_column)
                    WHERE e.table_id = %s AND LOWER(e.source_column) = LOWER(%s)
                    ORDER BY e.is_active DESC, e.column_id DESC
                    LIMIT 1
                    """,
                    [table_id, key_column],
                )
                key_row = cur.fetchone()
                data_type = str((key_row or {}).get("data_type") or "").upper()
                try:
                    precision = int((key_row or {}).get("data_precision")) if (key_row or {}).get("data_precision") is not None else None
                except (TypeError, ValueError):
                    precision = None
                try:
                    scale = int((key_row or {}).get("data_scale")) if (key_row or {}).get("data_scale") is not None else None
                except (TypeError, ValueError):
                    scale = None
                integer_types = {"INT", "INT2", "INT4", "INT8", "INTEGER", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT", "NUMBER", "DECIMAL", "NUMERIC", "DEC"}
                if data_type not in integer_types or (scale not in (None, 0)):
                    raise PolicyDependencyInvalidError(
                        f"Bounded CDC cursor column '{key_column}' is not an integer-compatible source type."
                    )
                cursor_type = CursorLogicalType.INT64 if data_type not in {"NUMBER", "DECIMAL", "NUMERIC", "DEC"} or (precision is not None and precision <= 18) else CursorLogicalType.DECIMAL
                details.update({
                    "activation_state": "ACTIVE",
                    "cdc_strategy": CDCStrategy.SEQUENCE_HIGH_WATERMARK.value,
                    "cdc_provider": CDCStrategy.SEQUENCE_HIGH_WATERMARK.value,
                    "exception_values": list(exception_values),
                    "exception_values_payload": Int64ExceptionCodec.encode(exception_values),
                    "exception_values_fingerprint": Int64ExceptionCodec.fingerprint(exception_values),
                    "capability_evidence": evidence,
                })
                cur.execute(
                    """
                    UPDATE metadata.db_tables_etl
                    SET cdc_columns = %s,
                        cdc_strategy = %s,
                        cdc_provider = %s,
                        cdc_source = 'PROFILED_CONDITIONAL',
                        cdc_confidence = GREATEST(COALESCE(cdc_confidence, 0), 95),
                        cdc_health = 'OK',
                        cdc_health_details = %s::jsonb,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE table_id = %s
                    """,
                    [key_column, CDCStrategy.SEQUENCE_HIGH_WATERMARK.value, CDCStrategy.SEQUENCE_HIGH_WATERMARK.value, json.dumps(details), table_id],
                )
            conn.commit()

        cdc_policy = self.set_candidate_cdc_policy(
            table_id=table_id,
            strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            mutation_mode=SourceMutationMode.APPEND_ONLY,
            cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
            cursor_columns=(key_column,),
            cursor_types=(cursor_type,),
            identity_mode=IdentityMode.BOUNDED_EXCEPTION_KEY,
            exception_values=exception_values,
            activation_state="ACTIVE",
            capability_evidence=evidence,
        )
        candidate = self.build_candidate_contract(table_id, cdc_policy=cdc_policy)
        principal = AuthenticatedPrincipal(actor_id=actor_id, actor_type="SYSTEM", roles={"ADMIN"})
        return self.promote_policy(
            PromotionRequest(
                table_id=table_id,
                expected_candidate_fingerprint=candidate.candidate_fingerprint,
                actor_type=PolicyActorType.SYSTEM.value,
                actor_id=actor_id,
                reason="Verified bounded-exception bootstrap completed; sequence CDC activated.",
                authenticated_principal=principal,
            ),
            principal=principal,
        )

    def certify_attempt_policy_fence(self, table_id: int, policy_version_id: str) -> bool:
        """
        Validates that an executing attempt's policy version remains current.
        Fails closed with POLICY_VERSION_SUPERSEDED if a new version was promoted.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT current_policy_version_id
                    FROM metadata.db_tables_etl
                    WHERE table_id = %s
                """, [table_id])
                row = cur.fetchone()
                if not row or not row[0]:
                    raise PolicyNotPromotedError(f"Table ID {table_id} has no current promoted policy.")

                current_id = str(row[0])
                if current_id != str(policy_version_id):
                    raise PolicyVersionSupersededError(
                        f"Execution policy version {policy_version_id} has been superseded by {current_id} on table_id {table_id}. "
                        "Execution cannot commit under a superseded policy."
                    )

        return True
