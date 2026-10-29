# src/exceptions.py
"""
Domain-specific exceptions for the ETL pipeline.
"""

class PipelineError(Exception):
    """Base class for pipeline errors."""
    error_code = "PIPELINE_ERROR"
    
    def __init__(self, message: str, **kwargs):
        self.message = message
        self.context = kwargs
        super().__init__(self.message)

    def __str__(self):
        ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
        if ctx_str:
            return f"[{self.error_code}] {self.message} ({ctx_str})"
        return f"[{self.error_code}] {self.message}"


class TargetKeyResolutionError(PipelineError):
    error_code = "TARGET_KEY_RESOLUTION_FAILED"


class ConnectionResolutionError(PipelineError):
    error_code = "CONNECTION_RESOLUTION_FAILED"


class TargetConstraintError(PipelineError):
    error_code = "TARGET_CONSTRAINT_VERIFICATION_FAILED"


class TargetPreflightError(PipelineError):
    error_code = "TARGET_PREFLIGHT_FAILED"


class BusinessKeyRequiredError(PipelineError):
    error_code = "BUSINESS_KEY_REQUIRED"


class ExecutionOwnershipRequiredError(PipelineError):
    error_code = "EXECUTION_OWNERSHIP_REQUIRED"


class CDCPolicyNotMigratedError(PipelineError):
    error_code = "CDC_POLICY_NOT_MIGRATED"


class TargetConstraintRequiredError(PipelineError):
    error_code = "TARGET_CONSTRAINT_REQUIRED"


class MetadataSchemaIncompatibleError(PipelineError):
    error_code = "METADATA_SCHEMA_INCOMPATIBLE"


class ExecutionProfileInvalidError(PipelineError):
    error_code = "EXECUTION_PROFILE_INVALID"


class SourceBusinessKeyRequiredError(PipelineError):
    error_code = "SOURCE_BUSINESS_KEY_REQUIRED"


class SourceKeyColumnMissingError(PipelineError):
    error_code = "SOURCE_KEY_COLUMN_MISSING"


class TargetKeyMappingMissingError(PipelineError):
    error_code = "TARGET_KEY_MAPPING_MISSING"


class TargetKeyMappingAmbiguousError(PipelineError):
    error_code = "TARGET_KEY_MAPPING_AMBIGUOUS"


class TargetKeyCollisionError(PipelineError):
    error_code = "TARGET_KEY_COLLISION"


class TargetKeyTransformationUnsafeError(PipelineError):
    error_code = "TARGET_KEY_TRANSFORMATION_UNSAFE"


class TargetKeyColumnMissingError(PipelineError):
    error_code = "TARGET_KEY_COLUMN_MISSING"


# ─── META-01 Policy Versioning & Governance Exceptions ─────────────────────────
class PolicyNotPromotedError(PipelineError):
    error_code = "POLICY_NOT_PROMOTED"


class PolicyAlreadyCurrentError(PipelineError):
    error_code = "POLICY_ALREADY_CURRENT"


class PolicyVersionSupersededError(PipelineError):
    error_code = "POLICY_VERSION_SUPERSEDED"


class GovernedPolicyBindingMissingError(PipelineError):
    """A governed attempt lost its immutable policy identity before commit."""

    error_code = "GOVERNED_POLICY_BINDING_MISSING"


class PolicyEvidenceDriftError(PipelineError):
    error_code = "POLICY_EVIDENCE_DRIFT"


class PolicyFingerprintMismatchError(PipelineError):
    error_code = "POLICY_FINGERPRINT_MISMATCH"


class PolicyPromotionConflictError(PipelineError):
    error_code = "POLICY_PROMOTION_CONFLICT"


class PolicyVersionNotFoundError(PipelineError):
    error_code = "POLICY_VERSION_NOT_FOUND"


class PolicyActorRequiredError(PipelineError):
    error_code = "POLICY_ACTOR_REQUIRED"


class PolicyActorUnauthorizedError(PipelineError):
    error_code = "POLICY_ACTOR_UNAUTHORIZED"


class PolicyTransitionInvalidError(PipelineError):
    error_code = "POLICY_TRANSITION_INVALID"


class PolicyDependencyInvalidError(PipelineError):
    error_code = "POLICY_DEPENDENCY_INVALID"


class ConditionalKeyViolationError(PipelineError):
    """A duplicate key was found outside the frozen exception set."""

    error_code = "CONDITIONAL_KEY_VIOLATION"


class ConditionalKeyCapabilityError(PipelineError):
    """The target cannot prove the frozen conditional-key capability."""

    error_code = "CONDITIONAL_KEY_CAPABILITY_MISSING"


class ExceptionValueTypeError(PipelineError, ValueError):
    """An exception value is not a supported INT64 value."""

    error_code = "EXCEPTION_VALUE_TYPE_INVALID"


class ExceptionCodecVersionError(PipelineError, ValueError):
    """An exception payload uses an unsupported codec version."""

    error_code = "EXCEPTION_CODEC_VERSION_MISMATCH"


# ─── CDC-01 Bounded Cursor & Extraction Exceptions ────────────────────────────
class CDCPolicyNotPromotedError(PipelineError):
    error_code = "CDC_POLICY_NOT_PROMOTED"


class CDCStrategyNotImplementedError(PipelineError):
    error_code = "CDC_STRATEGY_NOT_IMPLEMENTED"


class CDCMutationModeUnsupportedError(PipelineError):
    error_code = "CDC_MUTATION_MODE_UNSUPPORTED"


class CDCCursorFinalityUncertifiedError(PipelineError):
    error_code = "CDC_CURSOR_FINALITY_UNCERTIFIED"


class CDCCursorRequiredError(PipelineError):
    error_code = "CDC_CURSOR_REQUIRED"


class CDCCursorColumnMissingError(PipelineError):
    error_code = "CDC_CURSOR_COLUMN_MISSING"


class CDCCursorTypeInvalidError(PipelineError):
    error_code = "CDC_CURSOR_TYPE_INVALID"


class CDCCursorStateInvalidError(PipelineError):
    error_code = "CDC_CURSOR_STATE_INVALID"


class CDCCursorPolicyMismatchError(PipelineError):
    error_code = "CDC_CURSOR_POLICY_MISMATCH"


class CDCSourceCursorRegressionError(PipelineError):
    error_code = "CDC_SOURCE_CURSOR_REGRESSION"


class CDCUpperBoundCaptureFailedError(PipelineError):
    error_code = "CDC_UPPER_BOUND_CAPTURE_FAILED"


class CDCWindowInvalidError(PipelineError):
    error_code = "CDC_WINDOW_INVALID"


class CDCCursorNullabilityUnsupportedError(PipelineError):
    error_code = "CDC_CURSOR_NULLABILITY_UNSUPPORTED"


# ─── RECON-01 Physical Reconciliation Exceptions ──────────────────────────────
class ReconciliationFailedError(PipelineError):
    error_code = "RECONCILIATION_FAILED"


class ReconciliationRowLossError(ReconciliationFailedError):
    error_code = "RECONCILIATION_ROW_LOSS_DETECTED"


class ReconciliationOverflowError(ReconciliationFailedError):
    error_code = "RECONCILIATION_OVERFLOW_DETECTED"


class ReconciliationKeySetMismatchError(ReconciliationFailedError):
    error_code = "RECONCILIATION_KEY_SET_MISMATCH"


class ReconciliationCursorBoundaryViolationError(ReconciliationFailedError):
    error_code = "RECONCILIATION_CURSOR_BOUNDARY_VIOLATED"


class ReconciliationNullKeyError(ReconciliationFailedError):
    error_code = "RECONCILIATION_NULL_KEY_DETECTED"


class ReconciliationDuplicateKeyError(ReconciliationFailedError):
    error_code = "RECONCILIATION_DUPLICATE_KEY_DETECTED"


class ReconciliationContentMismatchError(ReconciliationFailedError):
    error_code = "RECONCILIATION_CONTENT_MISMATCH"


class ReconciliationWindowMismatchError(ReconciliationFailedError):
    error_code = "RECONCILIATION_WINDOW_MISMATCH"


class ReconciliationPolicySupersededError(ReconciliationFailedError):
    error_code = "RECONCILIATION_POLICY_SUPERSEDED"


class ReconciliationStrategyUnsupportedError(ReconciliationFailedError):
    error_code = "RECONCILIATION_STRATEGY_UNSUPPORTED"


class ReconciliationManifestRequiredError(ReconciliationFailedError):
    error_code = "RECONCILIATION_MANIFEST_REQUIRED"


# ─── OBS-01 Atomic Cursor & Commit Exceptions ─────────────────────────────────
class CDCCommitFailedError(PipelineError):
    error_code = "CDC_COMMIT_FAILED"


class CDCStateCommitConflictError(CDCCommitFailedError):
    error_code = "CDC_STATE_COMMIT_CONFLICT"


class CDCCommitReplayConflictError(CDCCommitFailedError):
    error_code = "CDC_COMMIT_REPLAY_CONFLICT"


class JobStateInvalidError(CDCCommitFailedError):
    error_code = "JOB_STATE_INVALID"


class BatchStateInvalidError(CDCCommitFailedError):
    error_code = "BATCH_STATE_INVALID"


class ReconciliationOutcomeInvalidError(CDCCommitFailedError):
    error_code = "RECONCILIATION_OUTCOME_INVALID"


class ReconciliationNotAuthoritativeError(CDCCommitFailedError):
    error_code = "RECONCILIATION_NOT_AUTHORITATIVE"


class ReconciliationDestinationMismatchError(CDCCommitFailedError):
    error_code = "RECONCILIATION_DESTINATION_MISMATCH"


class AttemptIdentityMismatchError(CDCCommitFailedError):
    error_code = "ATTEMPT_IDENTITY_MISMATCH"


class FingerprintMismatchError(CDCCommitFailedError):
    error_code = "FINGERPRINT_MISMATCH"


# ─── GOLD-ATOMIC-01 Full-Snapshot Publication Exceptions ─────────────────────
class GoldPublicationFailedError(PipelineError):
    error_code = "GOLD_PUBLICATION_FAILED"


class GoldStagingReconciliationFailedError(GoldPublicationFailedError):
    error_code = "GOLD_STAGING_RECONCILIATION_FAILED"


class GoldPublicationConflictError(GoldPublicationFailedError):
    error_code = "GOLD_PUBLICATION_CONFLICT"


class GoldStagingMissingError(GoldPublicationFailedError):
    error_code = "GOLD_STAGING_MISSING"


class GoldTargetLockTimeoutError(GoldPublicationFailedError):
    error_code = "GOLD_TARGET_LOCK_TIMEOUT"


# ─── Governed Execution Path Exceptions ──────────────────────────────────────

class GovernedFenceLostError(PipelineError):
    """
    Raised when the fencing CAS (ACTIVE → PUBLISHING or lease revalidation)
    fails before any target DML is attempted. Safe to retry from the beginning.
    """
    error_code = "GOVERNED_FENCE_LOST"


class PostCommitUncertainError(PipelineError):
    """
    Raised when the target transaction commits successfully but a subsequent
    post-commit metadata write (record_target_evidence or manifest CAS) fails.

    The target state is committed; the manifest may still be PENDING.
    The claim remains PUBLISHING — the caller MUST NOT release it.
    The reaper will transition it to RECOVERY_REQUIRED.
    """
    error_code = "GOVERNED_POST_COMMIT_UNCERTAIN"


class EvidenceMismatchError(PipelineError):
    """
    Raised when the observed 7-tuple (inside the open target transaction)
    does not match the expected manifest tuple. The target transaction is
    rolled back before this exception is raised.
    """
    error_code = "GOVERNED_EVIDENCE_MISMATCH"


class IntentMismatchError(PipelineError):
    """
    Raised when an idempotent manifest resume attempt finds a row whose
    expected_* fields differ from the current call's intent. The two callers
    have different execution plans — cannot safely resume.
    """
    error_code = "GOVERNED_INTENT_MISMATCH"

class ArtifactAlreadyCommittedError(PipelineError, ValueError):
    """
    Raised when an upstream artifact has already been published to the target layer
    and its state/watermark is fully committed.
    """
    error_code = "ARTIFACT_ALREADY_COMMITTED"

