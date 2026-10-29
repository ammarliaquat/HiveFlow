# src/validation/strategy_reconciler.py
"""
RECON-01 — Strategy-Aware Physical Reconciliation Engine.

Verifies that physical destination storage layers (Landing / Staging / Parquet / Delta / Target)
mathematically and empirically converge to the exact Attempt Persistence Manifest and CDC Attempt Window
before OBS-01 authorizes watermark advancement.

Enforces:
- Identity-set verification over pure row counts (detects replaced keys).
- Durable persistence manifest frozen before physical persistence.
- Scope isolation: verifies the exact attempt window (L, H] without whole-table pollution.
- Pre- and post-fencing against superseded policies or mismatched windows.
- Durable audit logging in metadata.cdc_attempt_reconciliation (persists failures before raising).
- Zero mutations to metadata.table_cdc_state (Stage-0 freeze).
"""

from datetime import datetime
import json
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union
import uuid

from src.connections.connection_manager import ConnectionManager
from src.metadata.key_contract import ResolvedTableContract
from src.metadata.policy_models import (
    AttemptPersistenceManifest,
    PhysicalReconciliationResult,
    ReconciliationOutcome,
    ReconciliationStrategy,
    ResolvedCDCWindow,
    compute_deterministic_fingerprint,
)
from src.metadata.policy_governance import PolicyGovernanceService
from src.utils.json_utils import json_safe_dumps
from src.exceptions import (
    ReconciliationFailedError,
    ReconciliationRowLossError,
    ReconciliationOverflowError,
    ReconciliationKeySetMismatchError,
    ReconciliationCursorBoundaryViolationError,
    ReconciliationNullKeyError,
    ReconciliationDuplicateKeyError,
    ReconciliationContentMismatchError,
    ReconciliationWindowMismatchError,
    ReconciliationPolicySupersededError,
    ReconciliationStrategyUnsupportedError,
    ReconciliationManifestRequiredError,
)


class StrategyAwareReconciler:
    """
    Authoritative physical reconciliation service for RECON-01.
    """

    def __init__(self, conn_manager: ConnectionManager):
        self._conn_manager = conn_manager

    @staticmethod
    def compute_key_set_fingerprint(keys: Iterable[Any]) -> str:
        """
        Computes a deterministic, canonical SHA-256 fingerprint for a collection of business keys.
        Normalizes single values and tuples, formats them canonically, and hashes the sorted collection.
        """
        normalized_keys: List[str] = []
        for k in keys:
            if k is None:
                continue
            if isinstance(k, tuple):
                # Tuple of composite keys
                canonical_item = json.dumps([str(val) for val in k], sort_keys=True)
            elif isinstance(k, list):
                canonical_item = json.dumps([str(val) for val in k], sort_keys=True)
            else:
                canonical_item = json.dumps([str(k)], sort_keys=True)
            normalized_keys.append(canonical_item)

        if not normalized_keys:
            return compute_deterministic_fingerprint({"key_set": "EMPTY_KEY_SET"})

        # Sort canonically to achieve order-independent set identity
        normalized_keys.sort()
        return compute_deterministic_fingerprint({"key_set": normalized_keys})

    def freeze_persistence_manifest(
        self,
        run_id: str,
        table_id: int,
        attempt_number: int,
        contract: ResolvedTableContract,
        window: ResolvedCDCWindow,
        storage_layer: str,
        storage_object_identity: str,
        expected_keys: Optional[Iterable[Any]],
        schema_fingerprint: str,
        expected_content_fingerprint: Optional[str] = None,
        manifest_id: Optional[uuid.UUID] = None,
        expected_row_count: Optional[int] = None,
        expected_key_fingerprint: Optional[str] = None,
    ) -> AttemptPersistenceManifest:
        """
        Creates and durably persists an AttemptPersistenceManifest in metadata.attempt_persistence_manifest.
        Must be called after transformation is finalized and before/at physical persistence.
        """
        if expected_keys is None:
            if expected_row_count is None or expected_key_fingerprint is None:
                raise ReconciliationManifestRequiredError(
                    "A persistence manifest requires keys or explicit bounded evidence."
                )
        else:
            keys_list = list(expected_keys)
            expected_row_count = len(keys_list)
            expected_key_fingerprint = self.compute_key_set_fingerprint(keys_list)

        manifest = AttemptPersistenceManifest.build(
            run_id=run_id,
            table_id=table_id,
            attempt_number=attempt_number,
            policy_version_id=contract.policy_version_id,
            window_id=window.window_id,
            storage_layer=storage_layer,
            storage_object_identity=storage_object_identity,
            expected_row_count=expected_row_count,
            expected_key_fingerprint=expected_key_fingerprint,
            schema_fingerprint=schema_fingerprint,
            expected_content_fingerprint=expected_content_fingerprint,
            manifest_id=manifest_id,
        )

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metadata.attempt_persistence_manifest (
                        manifest_id, run_id, table_id, attempt_number,
                        policy_version_id, window_id, storage_layer,
                        storage_object_identity, expected_row_count,
                        expected_key_fingerprint, expected_content_fingerprint,
                        schema_fingerprint, manifest_fingerprint, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (run_id, table_id, attempt_number, storage_layer) DO NOTHING
                """, [
                    str(manifest.manifest_id),
                    manifest.run_id,
                    manifest.table_id,
                    manifest.attempt_number,
                    manifest.policy_version_id,
                    str(manifest.window_id),
                    manifest.storage_layer,
                    manifest.storage_object_identity,
                    manifest.expected_row_count,
                    manifest.expected_key_fingerprint,
                    manifest.expected_content_fingerprint,
                    manifest.schema_fingerprint,
                    manifest.manifest_fingerprint,
                ])
                # ON CONFLICT DO NOTHING keeps the existing row without
                # returning its manifest_id.  Always read back the persisted
                # UUID so callers hold the canonical FK-valid identifier, not
                # the freshly-built (uninserted) one from a retry attempt.
                cur.execute("""
                    SELECT manifest_id FROM metadata.attempt_persistence_manifest
                    WHERE run_id = %s AND table_id = %s
                      AND attempt_number = %s AND storage_layer = %s
                """, [
                    manifest.run_id,
                    manifest.table_id,
                    manifest.attempt_number,
                    manifest.storage_layer,
                ])
                persisted_row = cur.fetchone()
            conn.commit()

        if persisted_row and str(persisted_row[0]) != str(manifest.manifest_id):
            # Conflict path fired — substitute the persisted manifest_id.
            import dataclasses
            try:
                manifest_id_val = persisted_row[0] if isinstance(persisted_row[0], uuid.UUID) else uuid.UUID(str(persisted_row[0]))
                manifest = dataclasses.replace(manifest, manifest_id=manifest_id_val)
            except (ValueError, TypeError, AttributeError):
                pass

        return manifest

    def reconcile_staged_target_convergence(
        self,
        *,
        run_id: str,
        table_id: int,
        attempt_number: int,
        contract: ResolvedTableContract,
        window: ResolvedCDCWindow,
        manifest: AttemptPersistenceManifest,
        target_evidence: Dict[str, Any],
    ) -> PhysicalReconciliationResult:
        """Persist a bounded, physical target membership reconciliation.

        The loader executes one database-side ``staging LEFT JOIN target``
        aggregate before deleting the staging relation.  With preflight's
        duplicate and target-UNIQUE checks, matching every staged key is an
        exact convergence proof for the attempt window.  This method consumes
        that target observation; it never accepts source keys as target keys
        and never materializes a large key set in the Python driver.
        """
        if (
            str(manifest.policy_version_id) != str(contract.policy_version_id)
            or str(window.policy_version_id) != str(contract.policy_version_id)
            or str(manifest.window_id) != str(window.window_id)
            or str(manifest.run_id) != str(run_id)
            or manifest.table_id != table_id
            or manifest.attempt_number != attempt_number
        ):
            raise ReconciliationWindowMismatchError(
                "Contract, window, and manifest identities do not match."
            )

        staged_rows = int(target_evidence.get("staged_rows", -1))
        matched_rows = int(target_evidence.get("matched_rows", -1))
        missing_rows = int(target_evidence.get("missing_rows", -1))
        details: Dict[str, Any] = {
            "proof_mode": target_evidence.get("proof_mode"),
            "staging_table": target_evidence.get("staging_table"),
            "staged_rows": staged_rows,
            "matched_rows": matched_rows,
            "missing_key_count": missing_rows,
            "target_observed": True,
        }
        outcome = ReconciliationOutcome.CONVERGED
        failure_code: Optional[str] = None
        if staged_rows != manifest.expected_row_count or matched_rows != manifest.expected_row_count or missing_rows != 0:
            outcome = ReconciliationOutcome.REJECTED
            failure_code = "RECONCILIATION_ROW_LOSS_DETECTED"
            details["error"] = (
                "Staging-to-target membership did not converge: "
                f"expected={manifest.expected_row_count}, staged={staged_rows}, "
                f"matched={matched_rows}, missing={missing_rows}."
            )

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_policy_version_id FROM metadata.db_tables_etl WHERE table_id = %s",
                    [table_id],
                )
                row = cur.fetchone()
                if not row or str(row[0]) != str(contract.policy_version_id):
                    outcome = ReconciliationOutcome.REJECTED
                    failure_code = "RECONCILIATION_POLICY_SUPERSEDED"
                    details["error"] = "The frozen policy was superseded during target reconciliation."

        # The manifest fingerprint names its legacy column "key fingerprint".
        # For this bounded proof it binds the immutable attempt/staging evidence;
        # the evidence details explicitly record that verification occurred in
        # the physical target rather than pretending it is a source key list.
        result = PhysicalReconciliationResult.build(
            manifest_id=manifest.manifest_id,
            run_id=run_id,
            table_id=table_id,
            attempt_number=attempt_number,
            policy_version_id=contract.policy_version_id,
            window_id=window.window_id,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer=manifest.storage_layer,
            is_commit_authoritative=True,
            expected_row_count=manifest.expected_row_count,
            physical_row_count=matched_rows,
            missing_key_count=max(0, missing_rows),
            unexpected_key_count=0,
            duplicate_key_count=0,
            null_key_count=0,
            expected_key_fingerprint=manifest.expected_key_fingerprint,
            physical_key_fingerprint=manifest.expected_key_fingerprint,
            outcome=outcome,
            failure_code=failure_code,
            details=details,
        )
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metadata.cdc_attempt_reconciliation (
                        reconciliation_id, manifest_id, run_id, table_id,
                        attempt_number, policy_version_id, window_id,
                        reconciliation_strategy, storage_layer, is_commit_authoritative,
                        expected_row_count, physical_row_count, row_count_delta,
                        missing_key_count, unexpected_key_count, duplicate_key_count,
                        null_key_count, expected_key_fingerprint, physical_key_fingerprint,
                        expected_content_fingerprint, physical_content_fingerprint,
                        outcome, failure_code, details, reconciliation_fingerprint,
                        reconciled_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, CURRENT_TIMESTAMP
                    )
                """, [
                    str(result.reconciliation_id), str(result.manifest_id), result.run_id,
                    result.table_id, result.attempt_number, result.policy_version_id,
                    str(result.window_id), result.reconciliation_strategy.value,
                    result.storage_layer, result.is_commit_authoritative,
                    result.expected_row_count, result.physical_row_count, result.row_count_delta,
                    result.missing_key_count, result.unexpected_key_count, result.duplicate_key_count,
                    result.null_key_count, result.expected_key_fingerprint,
                    result.physical_key_fingerprint, result.expected_content_fingerprint,
                    result.physical_content_fingerprint, result.outcome.value,
                    result.failure_code, json_safe_dumps(result.details), result.reconciliation_fingerprint,
                ])
            conn.commit()

        if outcome == ReconciliationOutcome.REJECTED:
            if failure_code == "RECONCILIATION_POLICY_SUPERSEDED":
                raise ReconciliationPolicySupersededError(details["error"])
            raise ReconciliationRowLossError(details["error"])
        return result

    def reconcile_attempt_physical_convergence(
        self,
        run_id: str,
        table_id: int,
        attempt_number: int,
        contract: ResolvedTableContract,
        window: ResolvedCDCWindow,
        manifest: AttemptPersistenceManifest,
        reconciliation_strategy: ReconciliationStrategy,
        storage_layer: str,
        physical_keys: Iterable[Any],
        expected_keys: Iterable[Any],
        is_commit_authoritative: bool = False,
        physical_content_fingerprint: Optional[str] = None,
        out_of_bounds_count: int = 0,
    ) -> PhysicalReconciliationResult:
        """
        Reconciles physical evidence against the attempt manifest and CDC window.
        Always records the outcome in metadata.cdc_attempt_reconciliation before raising.
        """
        # 1. Pre-Fencing: Validate window / manifest / contract identity match
        if (
            str(manifest.policy_version_id) != str(contract.policy_version_id)
            or str(window.policy_version_id) != str(contract.policy_version_id)
            or str(manifest.window_id) != str(window.window_id)
            or str(manifest.run_id) != str(run_id)
            or str(window.run_id) != str(run_id)
            or manifest.table_id != table_id
            or window.table_id != table_id
            or manifest.attempt_number != attempt_number
            or window.attempt_number != attempt_number
        ):
            raise ReconciliationWindowMismatchError(
                f"[RECONCILIATION_WINDOW_MISMATCH] Contract, Window, and Manifest identities do not match! "
                f"Contract ver={contract.policy_version_id}, Window ver={window.policy_version_id}, Manifest ver={manifest.policy_version_id}."
            )

        # 2. Extract & Analyze Physical Key Evidence
        physical_keys_list = list(physical_keys)
        expected_keys_list = list(expected_keys)

        physical_row_count = len(physical_keys_list)
        null_key_count = sum(1 for k in physical_keys_list if k is None or (isinstance(k, tuple) and any(sub is None for sub in k)))
        
        # Filter valid non-null keys
        valid_physical_keys = [k for k in physical_keys_list if k is not None and not (isinstance(k, tuple) and any(sub is None for sub in k))]
        
        # Canonical string sets for key comparison
        def to_canonical_key(k: Any) -> str:
            if isinstance(k, (tuple, list)):
                return json.dumps([str(x) for x in k], sort_keys=True)
            return json.dumps([str(k)], sort_keys=True)

        canonical_physical_keys = [to_canonical_key(k) for k in valid_physical_keys]
        canonical_expected_keys = [to_canonical_key(k) for k in expected_keys_list if k is not None]

        physical_key_set = set(canonical_physical_keys)
        expected_key_set = set(canonical_expected_keys)

        duplicate_key_count = len(canonical_physical_keys) - len(physical_key_set)
        missing_key_set = expected_key_set - physical_key_set
        unexpected_key_set = physical_key_set - expected_key_set

        missing_key_count = len(missing_key_set)
        unexpected_key_count = len(unexpected_key_set)

        physical_key_fingerprint = self.compute_key_set_fingerprint(valid_physical_keys)

        # 3. Determine Outcome and Failure Code
        outcome = ReconciliationOutcome.CONVERGED
        failure_code: Optional[str] = None
        details: Dict[str, Any] = {
            "expected_row_count": manifest.expected_row_count,
            "physical_row_count": physical_row_count,
            "missing_key_count": missing_key_count,
            "unexpected_key_count": unexpected_key_count,
            "duplicate_key_count": duplicate_key_count,
            "null_key_count": null_key_count,
            "out_of_bounds_count": out_of_bounds_count,
        }

        if window.is_empty and manifest.expected_row_count == 0:
            if physical_row_count == 0:
                outcome = ReconciliationOutcome.EMPTY_VERIFIED
            else:
                outcome = ReconciliationOutcome.REJECTED
                failure_code = "RECONCILIATION_OVERFLOW_DETECTED"
                details["error"] = f"Empty window expected 0 rows, found {physical_row_count} physical rows."
        else:
            if null_key_count > 0:
                outcome = ReconciliationOutcome.REJECTED
                failure_code = "RECONCILIATION_NULL_KEY_DETECTED"
                details["error"] = f"Found {null_key_count} records with NULL business keys in physical destination."
            elif duplicate_key_count > 0:
                outcome = ReconciliationOutcome.REJECTED
                failure_code = "RECONCILIATION_DUPLICATE_KEY_DETECTED"
                details["error"] = f"Found {duplicate_key_count} duplicate business key entries in physical destination."
            elif out_of_bounds_count > 0:
                outcome = ReconciliationOutcome.REJECTED
                failure_code = "RECONCILIATION_CURSOR_BOUNDARY_VIOLATED"
                details["error"] = f"Found {out_of_bounds_count} records outside window ({window.lower_cursor}, {window.upper_cursor}] in physical destination."
            elif missing_key_count > 0:
                outcome = ReconciliationOutcome.REJECTED
                failure_code = "RECONCILIATION_ROW_LOSS_DETECTED"
                details["error"] = f"Missing {missing_key_count} expected business keys in physical destination."
            elif unexpected_key_count > 0:
                outcome = ReconciliationOutcome.REJECTED
                failure_code = "RECONCILIATION_OVERFLOW_DETECTED"
                details["error"] = f"Found {unexpected_key_count} unexpected business keys in physical destination."
            elif physical_key_fingerprint != manifest.expected_key_fingerprint:
                outcome = ReconciliationOutcome.REJECTED
                failure_code = "RECONCILIATION_KEY_SET_MISMATCH"
                details["error"] = f"Key set fingerprint mismatch: expected {manifest.expected_key_fingerprint}, got {physical_key_fingerprint}."
            elif manifest.expected_content_fingerprint and physical_content_fingerprint:
                if manifest.expected_content_fingerprint != physical_content_fingerprint:
                    outcome = ReconciliationOutcome.REJECTED
                    failure_code = "RECONCILIATION_CONTENT_MISMATCH"
                    details["error"] = f"Content fingerprint mismatch: expected {manifest.expected_content_fingerprint}, got {physical_content_fingerprint}."

        # 4. Post-Fencing: Verify policy version is still current in metadata
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT current_policy_version_id
                    FROM metadata.db_tables_etl
                    WHERE table_id = %s
                """, [table_id])
                row = cur.fetchone()
                if not row or str(row[0]) != str(contract.policy_version_id):
                    outcome = ReconciliationOutcome.REJECTED
                    failure_code = "RECONCILIATION_POLICY_SUPERSEDED"
                    details["error"] = f"Policy version {contract.policy_version_id} was superseded during physical reconciliation scan."

        # 5. Build and Persist Durable Reconciliation Result (Even on Rejection)
        recon_result = PhysicalReconciliationResult.build(
            manifest_id=manifest.manifest_id,
            run_id=run_id,
            table_id=table_id,
            attempt_number=attempt_number,
            policy_version_id=contract.policy_version_id,
            window_id=window.window_id,
            reconciliation_strategy=reconciliation_strategy,
            storage_layer=storage_layer,
            is_commit_authoritative=is_commit_authoritative,
            expected_row_count=manifest.expected_row_count,
            physical_row_count=physical_row_count,
            missing_key_count=missing_key_count,
            unexpected_key_count=unexpected_key_count,
            duplicate_key_count=duplicate_key_count,
            null_key_count=null_key_count,
            expected_key_fingerprint=manifest.expected_key_fingerprint,
            physical_key_fingerprint=physical_key_fingerprint,
            expected_content_fingerprint=manifest.expected_content_fingerprint,
            physical_content_fingerprint=physical_content_fingerprint,
            outcome=outcome,
            failure_code=failure_code,
            details=details,
        )

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metadata.cdc_attempt_reconciliation (
                        reconciliation_id, manifest_id, run_id, table_id,
                        attempt_number, policy_version_id, window_id,
                        reconciliation_strategy, storage_layer, is_commit_authoritative,
                        expected_row_count, physical_row_count, row_count_delta,
                        missing_key_count, unexpected_key_count, duplicate_key_count,
                        null_key_count, expected_key_fingerprint, physical_key_fingerprint,
                        expected_content_fingerprint, physical_content_fingerprint,
                        outcome, failure_code, details, reconciliation_fingerprint,
                        reconciled_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, CURRENT_TIMESTAMP
                    )
                """, [
                    str(recon_result.reconciliation_id),
                    str(recon_result.manifest_id),
                    recon_result.run_id,
                    recon_result.table_id,
                    recon_result.attempt_number,
                    recon_result.policy_version_id,
                    str(recon_result.window_id),
                    recon_result.reconciliation_strategy.value,
                    recon_result.storage_layer,
                    recon_result.is_commit_authoritative,
                    recon_result.expected_row_count,
                    recon_result.physical_row_count,
                    recon_result.row_count_delta,
                    recon_result.missing_key_count,
                    recon_result.unexpected_key_count,
                    recon_result.duplicate_key_count,
                    recon_result.null_key_count,
                    recon_result.expected_key_fingerprint,
                    recon_result.physical_key_fingerprint,
                    recon_result.expected_content_fingerprint,
                    recon_result.physical_content_fingerprint,
                    recon_result.outcome.value,
                    recon_result.failure_code,
                    json_safe_dumps(recon_result.details),
                    recon_result.reconciliation_fingerprint,
                ])
            conn.commit()

        # 6. Dispatch Machine-Classified Exception if Rejected
        if outcome == ReconciliationOutcome.REJECTED:
            msg = f"[{failure_code}] Table {table_id} physical reconciliation rejected for layer '{storage_layer}': {details.get('error')}"
            if failure_code == "RECONCILIATION_NULL_KEY_DETECTED":
                raise ReconciliationNullKeyError(msg)
            elif failure_code == "RECONCILIATION_DUPLICATE_KEY_DETECTED":
                raise ReconciliationDuplicateKeyError(msg)
            elif failure_code == "RECONCILIATION_CURSOR_BOUNDARY_VIOLATED":
                raise ReconciliationCursorBoundaryViolationError(msg)
            elif failure_code == "RECONCILIATION_ROW_LOSS_DETECTED":
                raise ReconciliationRowLossError(msg)
            elif failure_code == "RECONCILIATION_OVERFLOW_DETECTED":
                raise ReconciliationOverflowError(msg)
            elif failure_code == "RECONCILIATION_KEY_SET_MISMATCH":
                raise ReconciliationKeySetMismatchError(msg)
            elif failure_code == "RECONCILIATION_CONTENT_MISMATCH":
                raise ReconciliationContentMismatchError(msg)
            elif failure_code == "RECONCILIATION_POLICY_SUPERSEDED":
                raise ReconciliationPolicySupersededError(msg)
            else:
                raise ReconciliationFailedError(msg)

        return recon_result

    def audit_and_demote_sequence_mutation(
        self,
        table_id: int,
        detected_reason: str,
        expected_current_policy_version_id: str,
        actor_id: str = "INTEGRITY_PROFILER",
    ) -> None:
        """P0 Guard: When historical mutation is detected in a SEQUENCE_HIGH_WATERMARK table,
        creates a superseding non-CDC policy for future work only.  It never
        mutates the active run's frozen contract, cursor, or authority.
        """
        PolicyGovernanceService(self._conn_manager).demote_cdc_policy(
            table_id=table_id,
            expected_current_policy_version_id=expected_current_policy_version_id,
            actor_id=actor_id,
            reason=(
                "APPEND_ONLY_MUTATION_DETECTED: "
                f"{detected_reason}"
            ),
        )
