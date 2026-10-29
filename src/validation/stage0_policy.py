"""Temporary fail-closed policy gates for the Stage 0 safety freeze."""

from src.exceptions import CDCPolicyNotMigratedError


def validate_stage0_execution_policy(meta) -> None:
    """Reject legacy incremental policies before any persistence occurs."""
    is_cdc = str(getattr(meta, "is_cdc", "")).strip().lower()
    if is_cdc == "n":
        return
    contract = getattr(meta, "table_contract", None)
    if is_cdc == "y" and contract and getattr(contract, "cdc_policy", None):
        return
    if is_cdc == "y":
        raise CDCPolicyNotMigratedError(
            f"Legacy CDC policy for {meta.source_table} must be migrated before execution."
        )
    raise CDCPolicyNotMigratedError(
        f"Invalid is_cdc value for {meta.source_table}: {getattr(meta, 'is_cdc', None)!r}."
    )


def has_stage0_trusted_business_key(meta) -> bool:
    """Return true only for an explicitly approved, non-empty execution key."""
    key_columns = getattr(meta, "canonical_source_business_key", None)
    return bool(tuple(key_columns or ()))


def is_source_identity_execution_enabled(meta) -> bool:
    """Allow identity-dependent persistence only for promoted governed CDC."""
    return bool(
        str(getattr(meta, "is_cdc", "")).strip().lower() == "y"
        and getattr(getattr(meta, "table_contract", None), "cdc_policy", None)
        and has_stage0_trusted_business_key(meta)
    )
