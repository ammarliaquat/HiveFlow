# src/metadata/target_key_resolver.py
"""
KEY-02 — Target Business Identity Authority & Attempt Contract Assembly.

Derives and certifies canonical target business identity from:
- KEY-01: Approved source business key
- MAP-01: Immutable resolved table mappings

Enforces:
- Exact composite key order preservation
- Unquoted database-neutral logical identifier formatting
- Fail-closed on unsafe transformations, missing mappings, collisions, and unapproved keys
- Non-mutating target physical constraint certification (Stage 0 freeze intact)
"""

from typing import Optional, Set, Tuple, List, Dict
import logging

from src.connections.connection_manager import ConnectionManager
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.metadata.policy_models import CandidateTableContract
from src.exceptions import (
    SourceBusinessKeyRequiredError,
    SourceKeyColumnMissingError,
    TargetKeyMappingMissingError,
    TargetKeyCollisionError,
    TargetKeyTransformationUnsafeError,
    TargetKeyColumnMissingError,
    TargetConstraintRequiredError,
)

logger = logging.getLogger("target_key_resolver")


class TargetKeyResolver:
    """Authority for resolving canonical target business keys and assembling table contracts."""

    def __init__(self, conn_manager: Optional[ConnectionManager] = None):
        self._conn_manager = conn_manager

    def resolve_target_business_key(
        self,
        table_id: int,
        approved_source_key: Optional[ApprovedBusinessKey],
        resolved_mappings: ResolvedTableMappings,
    ) -> ResolvedTargetBusinessKey:
        """
        Derive the canonical target business identity from an approved source key and mapping snapshot.
        Preserves source key order deterministically.
        """
        if approved_source_key is None or not approved_source_key.columns:
            raise SourceBusinessKeyRequiredError(
                f"An approved source business key is required to resolve target business identity for table_id {table_id}."
            )

        # Validate source key presence against frozen source schema if available
        if resolved_mappings.frozen_source_schema:
            frozen_cols_lower = {str(c).strip().lower() for c in resolved_mappings.frozen_source_schema}
            for src_col in approved_source_key.columns:
                if str(src_col).strip().lower() not in frozen_cols_lower:
                    raise SourceKeyColumnMissingError(
                        f"Source business key column '{src_col}' is absent from frozen source schema for table_id {table_id}."
                    )

        target_key_columns: Tuple[str, ...]

        if resolved_mappings.mapping_policy == MappingPolicy.IDENTITY:
            target_key_columns = tuple(str(c).strip() for c in approved_source_key.columns)
        else:
            # EXPLICIT mapping policy: lookup each key component in source key order
            mapping_lookup: Dict[str, object] = {
                str(m.source_column).strip().lower(): m
                for m in resolved_mappings.mappings
            }

            derived_targets: List[str] = []
            for src_col in approved_source_key.columns:
                src_key_norm = str(src_col).strip().lower()
                mapping = mapping_lookup.get(src_key_norm)

                if mapping is None:
                    raise TargetKeyMappingMissingError(
                        f"Explicit mapping missing for source business key column '{src_col}' on table_id {table_id}."
                    )

                # Reject unapproved / lossy transformations on key columns
                transform_expr = getattr(mapping, "transform_expr", None)
                if transform_expr and str(transform_expr).strip():
                    raise TargetKeyTransformationUnsafeError(
                        f"Lossy or unapproved transformation '{transform_expr}' on business key column '{src_col}' is not permitted for table_id {table_id}."
                    )

                tgt_col_raw = getattr(mapping, "target_column", None) or getattr(mapping, "source_column", "")
                tgt_col = str(tgt_col_raw).strip()
                derived_targets.append(tgt_col)

            # Detect collisions: two distinct source key columns collapsing to same normalized target column
            if len({t.lower() for t in derived_targets}) != len(derived_targets):
                raise TargetKeyCollisionError(
                    f"Target key collision: multiple source key columns collapse to duplicate target columns {derived_targets} on table_id {table_id}."
                )

            target_key_columns = tuple(derived_targets)

        return ResolvedTargetBusinessKey.build(
            table_id=table_id,
            source_key_columns=approved_source_key.columns,
            target_key_columns=target_key_columns,
            source_key_evidence_fingerprint=approved_source_key.evidence_fingerprint,
            mapping_fingerprint=resolved_mappings.fingerprint,
            identity_mode=approved_source_key.identity_mode,
            exception_values=approved_source_key.exception_values,
        )

    def assemble_candidate_contract(
        self,
        table_id: int,
        approved_source_key: Optional[ApprovedBusinessKey],
        resolved_mappings: ResolvedTableMappings,
    ) -> CandidateTableContract:
        """
        Assemble the candidate table contract for operator review and promotion validation.
        """
        resolved_target_key = self.resolve_target_business_key(
            table_id=table_id,
            approved_source_key=approved_source_key,
            resolved_mappings=resolved_mappings,
        )
        assert approved_source_key is not None
        return CandidateTableContract.build(
            table_id=table_id,
            mapping_policy=resolved_mappings.mapping_policy,
            approved_source_business_key=approved_source_key,
            resolved_mappings=resolved_mappings,
            canonical_target_business_key=resolved_target_key,
        )

    # Backwards-compatible alias for existing callers
    assemble_table_contract = assemble_candidate_contract


    def validate_target_physical_schema(
        self,
        target_tag: str,
        target_schema: str,
        target_table: str,
        resolved_target_key: ResolvedTargetBusinessKey,
        require_unique_constraint: bool = True,
    ) -> bool:
        """
        Certify physical target schema readiness without mutating it.
        Confirms target key columns exist and optionally checks compatible UNIQUE/PK constraints exist.
        Executes zero DDL.
        """
        if not self._conn_manager:
            raise RuntimeError("ConnectionManager is required for physical target schema validation.")

        conn_cfg = self._conn_manager.get(target_tag, expected_role="target")
        db_type = getattr(conn_cfg, "db_type", "").lower()

        with self._conn_manager.get_db_connection(target_tag, expected_role="target") as conn:
            with conn.cursor() as cur:
                # 1. Verify all target key columns exist physically
                if db_type in ("postgres", "postgresql", "mysql"):
                    cur.execute("""
                        SELECT LOWER(column_name)
                        FROM information_schema.columns
                        WHERE LOWER(table_schema) = LOWER(%s)
                          AND LOWER(table_name) = LOWER(%s)
                    """, [target_schema, target_table])
                    physical_columns: Set[str] = {r[0] for r in cur.fetchall()}
                else:
                    # Fallback generic query
                    cur.execute(f"SELECT * FROM {target_schema}.{target_table} WHERE 1=0")
                    physical_columns = {d[0].lower() for d in cur.description}

                for tgt_col in resolved_target_key.target_key_columns:
                    if tgt_col.lower() not in physical_columns:
                        raise TargetKeyColumnMissingError(
                            f"Derived target key column '{tgt_col}' does not physically exist in target table '{target_schema}.{target_table}'."
                        )

                # 2. Verify compatible PK or UNIQUE constraint exists if required
                if db_type in ("postgres", "postgresql"):
                    cur.execute("""
                        SELECT c.conname, c.contype, ARRAY_AGG(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS con_cols
                        FROM pg_constraint c
                        JOIN pg_namespace n ON n.oid = c.connamespace
                        JOIN pg_class cl ON cl.oid = c.conrelid
                        JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum = ANY(c.conkey)
                        WHERE LOWER(n.nspname) = LOWER(%s)
                          AND LOWER(cl.relname) = LOWER(%s)
                          AND c.contype IN ('p', 'u')
                        GROUP BY c.conname, c.contype
                    """, [target_schema, target_table])
                    constraints = cur.fetchall()
                    target_key_set = {k.lower() for k in resolved_target_key.target_key_columns}

                    has_compatible = False
                    for conname, contype, con_cols in constraints:
                        con_cols_set = {str(col).lower() for col in con_cols}
                        if con_cols_set == target_key_set:
                            has_compatible = True
                            break

                    if require_unique_constraint and not has_compatible:
                        raise TargetConstraintRequiredError(
                            f"Target table '{target_schema}.{target_table}' lacks a compatible PRIMARY KEY or UNIQUE constraint for target key {resolved_target_key.target_key_columns}."
                        )

        return True
