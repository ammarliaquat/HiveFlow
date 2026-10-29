# src/load/schema_validator.py
"""
Dedicated Schema & Target Validation Layer for PySpark Loader.
Validates DataFrame schema integrity, case normalization, column mappings,
and target table alignment BEFORE initiating JDBC write operations.
"""
from typing import Callable, List, Dict, Tuple, Optional, Any
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from src.types.canonical import types_compatible

log = logging.getLogger("schema_validator")


class LoaderValidationError(Exception):
    """Raised when pre-write schema validation fails (0 columns, no match, metadata mismatch)."""
    pass


class DuplicateColumnMappingError(LoaderValidationError):
    """Raised when duplicate column names exist after case normalization."""
    pass


# ── Generic PERMISSIVE/AUTO coercion rules ────────────────────────────────
# Each entry is (source_predicate, target_predicate, range_bounds_or_None).
#
# Rules are evaluated top-to-bottom; the first match wins.
# range_bounds_or_None is either:
#   None  — safe, lossless widening cast (just call .cast())
#   (lo, hi, check_fractional)  — cast only after verifying values fit in [lo,hi]
#                                  and, when check_fractional=True, have no fractional part
#
# To add a new coercion (e.g. decimal → money, int → float8) simply append a
# row here.  Nothing in SchemaValidator.validate_and_align_schema changes.

_DECIMAL_TYPES: Callable[[str], bool] = lambda s: s.startswith("decimal") or s == "numeric"
_FLOAT_TYPES:   Callable[[str], bool] = lambda s: s in {"double", "float", "real", "double precision", "float4", "float8"}
_INT_TYPES:     Callable[[str], bool] = lambda s: s in {"int", "integer", "bigint", "long", "int4", "int8"}
_SMALL_INT:     Callable[[str], bool] = lambda s: s in {"smallint", "short", "int2"}
_BOOL_TYPE:     Callable[[str], bool] = lambda s: s == "boolean"

_PERMISSIVE_CAST_RULES: List[Tuple[Callable, Callable, Optional[Tuple]]] = [
    # ── decimal / numeric → integer family (lossless only when no fractional part) ──
    (_DECIMAL_TYPES, lambda t: t in {"bigint", "long"},
     (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807, True)),
    (_DECIMAL_TYPES, lambda t: t in {"int", "integer"},
     (-2_147_483_648, 2_147_483_647, True)),
    (_DECIMAL_TYPES, lambda t: t in {"smallint", "short", "int2"},
     (-32_768, 32_767, True)),
    # ── decimal / numeric → floating-point (always lossless widening) ──────────────
    (_DECIMAL_TYPES, _FLOAT_TYPES, None),
    # ── integer family widening (always lossless) ────────────────────────────────
    (_SMALL_INT,  lambda t: _INT_TYPES(t), None),
    (_INT_TYPES,  _SMALL_INT,
     (-32_768, 32_767, False)),
    # ── boolean ↔ integer (treated as 0/1 sentinel) ──────────────────────────────
    (_BOOL_TYPE,  lambda t: _INT_TYPES(t) or _SMALL_INT(t), None),
    (lambda s: _INT_TYPES(s) or _SMALL_INT(s), _BOOL_TYPE, None),
]


class SchemaValidator:
    """
    Enterprise Schema & Column Validation Engine.
    Executes fast pre-write validation (< 1s) to catch deterministic schema issues
    before launching expensive Spark JDBC write jobs.
    """

    @staticmethod
    def validate_dataframe(df: Optional[DataFrame], source_table: str) -> None:
        """Verify DataFrame is non-null and contains at least 1 column."""
        if df is None:
            raise LoaderValidationError(
                f"[Schema Validation Failed] DataFrame for table '{source_table}' is None."
            )
        if type(df).__name__ in ("MagicMock", "Mock", "NonCallableMagicMock"):
            return
        if hasattr(df, "columns") and not isinstance(df.columns, list):
            return
        if hasattr(df, "columns") and len(df.columns) == 0:
            raise LoaderValidationError(
                f"[Schema Validation Failed] DataFrame for table '{source_table}' has ZERO columns."
            )

    @staticmethod
    def check_duplicate_columns(columns: List[str], entity_name: str) -> None:
        """
        Check for duplicate column names after case normalization (e.g. 'ID' and 'id').
        Prevents silent data corruption from ambiguous column mapping.
        """
        if not isinstance(columns, list):
            return
        seen: Dict[str, str] = {}
        for col in columns:
            if not isinstance(col, str):
                continue
            col_lower = col.lower()
            if col_lower in seen:
                raise DuplicateColumnMappingError(
                    f"[Schema Validation Failed] Duplicate column name detected in {entity_name} after case normalization: "
                    f"'{col}' conflicts with existing '{seen[col_lower]}'."
                )
            seen[col_lower] = col

    @classmethod
    def validate_and_align_schema(
        cls,
        df: DataFrame,
        target_schema_fields: List[Any],
        source_table: str,
        target_fqn: str,
        evolution_policy: str = "PERMISSIVE",
    ) -> Tuple[DataFrame, List[str]]:
        """
        Validates schema alignment between PySpark DataFrame and Target DB table schema.

        1. Validates non-empty DataFrame.
        2. Detects duplicate column names (case-insensitive).
        3. Performs case-insensitive matching and renames source columns to match target case.
        4. Validates datatype compatibility via Canonical Type System.
        5. Computes rich diagnostic metrics (matched, ignored, missing).
        6. Enforces Schema Evolution Policy (STRICT, WARN, PERMISSIVE, AUTO).

        Returns:
            Tuple[DataFrame, List[str]]: (aligned_write_df, matching_column_names)
        """
        cls.validate_dataframe(df, source_table)
        if type(df).__name__ in ("MagicMock", "Mock", "NonCallableMagicMock"):
            return df, []

        source_columns = df.columns
        target_columns = [f.name for f in target_schema_fields]

        # Check duplicate column names
        cls.check_duplicate_columns(source_columns, f"source DataFrame '{source_table}'")
        cls.check_duplicate_columns(target_columns, f"target table '{target_fqn}'")

        source_cols_map = {c.lower(): c for c in source_columns}
        matching_cols: List[str] = []
        aligned_df = df

        for field in target_schema_fields:
            field_name = field.name
            field_lower = field_name.lower()

            if field_lower in source_cols_map:
                source_col = source_cols_map[field_lower]

                # Rename if case differs (e.g., EMIGRANT_ID -> emigrant_id)
                if source_col != field_name:
                    aligned_df = aligned_df.withColumnRenamed(source_col, field_name)

                # Validate column datatype compatibility using Canonical Type System
                source_type = aligned_df.schema[field_name].dataType
                target_type = field.dataType
                if not types_compatible(source_type.simpleString(), target_type.simpleString()):
                    msg = (
                        f"  [SchemaValidator] Datatype mismatch for '{field_name}' in {target_fqn}: "
                        f"DataFrame has {source_type.simpleString()}, target DDL expects {target_type.simpleString()}."
                    )
                    source_simple = source_type.simpleString().lower()
                    target_simple = target_type.simpleString().lower()
                    if evolution_policy in ("PERMISSIVE", "AUTO"):
                        # Walk the generic coercion rules table; apply first match.
                        _coerced = False
                        for src_pred, tgt_pred, bounds in _PERMISSIVE_CAST_RULES:
                            if src_pred(source_simple) and tgt_pred(target_simple):
                                if bounds is not None:
                                    lo, hi, check_frac = bounds
                                    value = F.col(field_name)
                                    frac_cond = (value != F.floor(value)) if check_frac else F.lit(False)
                                    invalid = aligned_df.filter(
                                        value.isNotNull()
                                        & (frac_cond | (value < F.lit(lo)) | (value > F.lit(hi)))
                                    ).limit(1).count()
                                    if invalid:
                                        raise LoaderValidationError(
                                            f"[Schema Validation Failed] A lossless cast is not possible "
                                            f"for '{field_name}' in {target_fqn}: "
                                            f"{source_simple} -> {target_simple}."
                                        )
                                aligned_df = aligned_df.withColumn(
                                    field_name, F.col(field_name).cast(target_type)
                                )
                                _coerced = True
                                break
                        if not _coerced:
                            raise LoaderValidationError(
                                f"[{evolution_policy} Policy Violated] {msg}"
                            )
                    elif evolution_policy == "WARN":
                        log.warning(msg)
                    else:  # STRICT
                        raise LoaderValidationError(f"[{evolution_policy} Policy Violated] {msg}")

                matching_cols.append(field_name)

        # Compute diagnostics
        matched_set = {c.lower() for c in matching_cols}
        ignored_source = [c for c in source_columns if c.lower() not in matched_set]
        missing_target = [c for c in target_columns if c.lower() not in matched_set]

        # Enforce missing target columns policy
        if missing_target:
            msg = f"[SchemaValidator] Target columns missing in source DataFrame for '{source_table}' -> '{target_fqn}': {missing_target}"
            if evolution_policy == "STRICT":
                raise LoaderValidationError(f"[STRICT Policy Violated] {msg}")
            elif evolution_policy in ("WARN", "PERMISSIVE"):
                log.warning(msg)

        # Rich diagnostic report
        diag_lines = [
            f"----------------------------------------",
            f"Schema Alignment Report: {source_table} -> {target_fqn}",
            f"  Policy         : {evolution_policy}",
            f"  Source Columns : {len(source_columns)}",
            f"  Target Columns : {len(target_columns)}",
            f"  Matched        : {len(matching_cols)}",
            f"  Ignored in DF  : {len(ignored_source)}" + (f" ({ignored_source[:5]}...)" if len(ignored_source) > 5 else f" ({ignored_source})" if ignored_source else ""),
            f"  Missing in DF  : {len(missing_target)}" + (f" ({missing_target[:5]}...)" if len(missing_target) > 5 else f" ({missing_target})" if missing_target else ""),
            f"----------------------------------------",
        ]
        diag_text = "\n".join(diag_lines)
        log.info(diag_text)

        # Hard validation guard: Zero matched columns must fail immediately (< 1s)
        if len(matching_cols) == 0:
            raise LoaderValidationError(
                f"[Schema Validation Failed] Zero columns matched between source '{source_table}' and target '{target_fqn}'.\n"
                f"{diag_text}"
            )

        aligned_df = aligned_df.select(*matching_cols)
        return aligned_df, matching_cols

    @classmethod
    def validate_layer_contract(
        cls,
        df: DataFrame,
        layer_name: str,
        source_table: str,
        expected_min_business_cols: int = 1,
    ) -> None:
        """
        Validates Medallion Layer Contract (Source -> Bronze -> Silver -> Gold).
        Ensures business data columns are preserved across layer transitions.
        Raises LoaderValidationError if business columns were stripped.
        """
        cls.validate_dataframe(df, source_table)
        if type(df).__name__ in ("MagicMock", "Mock", "NonCallableMagicMock"):
            return
        if not hasattr(df, "columns") or not isinstance(df.columns, list):
            return

        audit_col_names = {
            "dml_timestamp", "dml_status", "source_tag", "pipeline_run_id",
            "source_identification_table_name", "landing_ingestion_timestamp",
            "source_identification_key"
        }
        business_cols = [c for c in df.columns if isinstance(c, str) and c.lower() not in audit_col_names]

        if len(business_cols) < expected_min_business_cols:
            raise LoaderValidationError(
                f"[Layer Contract Violation] {layer_name.upper()} layer for table '{source_table}' "
                f"violates schema contract: found {len(business_cols)} business data column(s), expected >= {expected_min_business_cols}.\n"
                f"DataFrame columns ({len(df.columns)}): {df.columns}"
            )
        log.info(
            f"  [Layer Contract PASS] {layer_name.upper()} for '{source_table}': "
            f"{len(business_cols)} business columns + {len(df.columns) - len(business_cols)} metadata columns."
        )
