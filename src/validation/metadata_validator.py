# src/validation/metadata_validator.py
"""
MetadataValidator — Spark-free framework pre-flight validation.
===============================================================
Executes fast SQL queries directly against PostgreSQL metadata tables
(metadata.db_tables_etl & metadata.db_columns_etl) to catch configuration
defects before launching Spark tasks.
"""
from typing import List, Optional, Tuple
import logging
from src.connections.connection_manager import ConnectionManager
from src.constants import MetadataConstants

log = logging.getLogger("MetadataValidator")


class MetadataValidator:
    """
    Validates active tables in metadata.db_tables_etl and metadata.db_columns_etl.
    Can be run standalone (--validate-only) or integrated into ETL execution paths.
    """

    def __init__(self, conn_manager: ConnectionManager):
        self.conn_manager = conn_manager

    def validate(
        self,
        source_tag: Optional[str] = None,
        source_table: Optional[str] = None,
        target_tag: Optional[str] = None,
        raise_on_error: bool = False,
        quiet: bool = False,
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Run all pre-flight metadata checks.

        Args:
            source_tag: Optional source tag to filter validation scope.
            source_table: Optional source table to filter validation scope.
            target_tag: Optional target tag to filter validation scope.
            raise_on_error: If True, raises ValueError if validation fails.
            quiet: If True, suppresses console printing.

        Returns:
            Tuple of (is_valid: bool, errors: List[str], warnings: List[str])
        """
        errors: List[str] = []
        warnings: List[str] = []

        tag_filters = []
        params_list = []
        if source_tag:
            tag_filters.append("source_tag = %s")
            params_list.append(source_tag)
        if source_table:
            tag_filters.append("source_table = %s")
            params_list.append(source_table)
        if target_tag:
            tag_filters.append("target_tag = %s")
            params_list.append(target_tag)
        tag_clause = f" AND {' AND '.join(tag_filters)}" if tag_filters else ""
        params: tuple = tuple(params_list)

        col_filters = []
        col_params_list = []
        if source_tag:
            col_filters.append("t.source_tag = %s")
            col_params_list.append(source_tag)
        if source_table:
            col_filters.append("t.source_table = %s")
            col_params_list.append(source_table)
        if target_tag:
            col_filters.append("t.target_tag = %s")
            col_params_list.append(target_tag)
        col_tag_clause = f" AND {' AND '.join(col_filters)}" if col_filters else ""
        col_params: tuple = tuple(col_params_list)

        with self.conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # ── 1. Check Unknown Connection Tags ─────────────────────────
                cur.execute(
                    f"""
                    SELECT DISTINCT source_tag, target_tag
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    WHERE LOWER(is_active) = '{MetadataConstants.IS_ACTIVE}' {tag_clause}
                    """,
                    params,
                )
                for st, tt in cur.fetchall():
                    if not self.conn_manager.has_connection(st):
                        errors.append(f"Unknown source_tag configured in active metadata: '{st}'")
                    if tt and not self.conn_manager.has_connection(tt):
                        errors.append(f"Unknown target_tag configured in active metadata: '{tt}' (source_tag: {st})")

                # ── 2. Check Duplicate Active Tables ──────────────────────────
                cur.execute(
                    f"""
                    SELECT source_tag, source_table, target_tag, COUNT(*)
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    WHERE LOWER(is_active) = '{MetadataConstants.IS_ACTIVE}' {tag_clause}
                    GROUP BY source_tag, source_table, target_tag
                    HAVING COUNT(*) > 1
                    """,
                    params,
                )
                for st, tbl, tt, cnt in cur.fetchall():
                    errors.append(
                        f"Duplicate active table definition: {st}.{tbl} -> {tt} appears {cnt} times."
                    )

                # ── 3. Check Missing Landing/History/Target Names ─────────────
                cur.execute(
                    f"""
                    SELECT source_tag, source_table
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    WHERE LOWER(is_active) = '{MetadataConstants.IS_ACTIVE}' {tag_clause}
                      AND (
                          landing_table IS NULL OR TRIM(landing_table) = '' OR
                          history_table IS NULL OR TRIM(history_table) = '' OR
                          target_table IS NULL OR TRIM(target_table) = ''
                      )
                    """,
                    params,
                )
                for st, tbl in cur.fetchall():
                    errors.append(
                        f"Missing destination table name (landing_table/history_table/target_table) for {st}.{tbl}"
                    )

                # ── 4. Check Invalid Priority ─────────────────────────────────
                cur.execute(
                    f"""
                    SELECT source_tag, source_table, priority
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    WHERE LOWER(is_active) = '{MetadataConstants.IS_ACTIVE}' {tag_clause}
                      AND (priority IS NULL OR priority NOT IN (1, 2, 3))
                    """,
                    params,
                )
                for st, tbl, prio in cur.fetchall():
                    errors.append(
                        f"Invalid priority '{prio}' for table {st}.{tbl} (must be 1, 2, or 3)"
                    )

                # ── 5. Check Missing CDC / Business Keys ──────────────────────
                cur.execute(
                    f"""
                    SELECT source_tag, source_table, table_type, is_cdc, cdc_columns, approved_source_business_key
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    WHERE LOWER(is_active) = '{MetadataConstants.IS_ACTIVE}' {tag_clause}
                      AND (LOWER(COALESCE(table_type, '')) = 'cdc' OR LOWER(COALESCE(is_cdc, '')) = 'y')
                    """,
                    params,
                )
                for st, tbl, tt, is_cdc, cdc_cols, approved_key in cur.fetchall():
                    if not cdc_cols or not str(cdc_cols).strip():
                        errors.append(
                            f"CDC enabled on table {st}.{tbl} but cdc_columns is missing or empty."
                        )
                    if not approved_key or not str(approved_key).strip():
                        errors.append(
                            f"CDC enabled on table {st}.{tbl} but approved_source_business_key is missing or empty."
                        )
                    try:
                        if self.conn_manager.has_connection(st):
                            conn_cfg = self.conn_manager.get(st)
                            db_type = getattr(conn_cfg, "db_type", "").lower()
                            conn_type = getattr(conn_cfg, "connection_type", "DATABASE").upper()
                            if db_type in ("file", "csv", "json", "parquet") or conn_type == "FILE":
                                errors.append(
                                    f"CDC incremental load (is_cdc='y') is only supported for database sources, but table {st}.{tbl} is configured with non-database type '{db_type or conn_type}'."
                                )
                    except Exception:
                        pass

                # ── 6. Check Duplicate Column Mappings ────────────────────────
                cur.execute(
                    f"""
                    SELECT t.source_tag, t.source_table, c.target_column, COUNT(*)
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.COLUMNS_ETL_TABLE} c
                    JOIN {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE} t ON c.table_id = t.table_id
                    WHERE LOWER(c.is_active) = '{MetadataConstants.IS_ACTIVE}'
                      AND LOWER(t.is_active) = '{MetadataConstants.IS_ACTIVE}' {col_tag_clause}
                    GROUP BY t.source_tag, c.table_id, t.source_table, c.target_column
                    HAVING COUNT(*) > 1
                    """,
                    col_params,
                )
                for st, tbl, tcol, cnt in cur.fetchall():
                    errors.append(
                        f"Duplicate target column mapping in {st}.{tbl}: target_column '{tcol}' mapped by {cnt} source columns."
                    )

                # ── 7. Check Malformed Column Datatypes ───────────────────────
                cur.execute(
                    f"""
                    SELECT t.source_tag, t.source_table, c.source_column, c.target_column_datatype
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.COLUMNS_ETL_TABLE} c
                    JOIN {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE} t ON c.table_id = t.table_id
                    WHERE LOWER(c.is_active) = '{MetadataConstants.IS_ACTIVE}'
                      AND LOWER(t.is_active) = '{MetadataConstants.IS_ACTIVE}' {col_tag_clause}
                      AND (c.target_column_datatype IS NULL OR TRIM(c.target_column_datatype) = '')
                    """,
                    col_params,
                )
                for st, tbl, scol, ctype in cur.fetchall():
                    errors.append(
                        f"Malformed or empty target_column_datatype on {st}.{tbl}.{scol}: '{ctype}'"
                    )

                # ── 8. Check Missing Deduplication Keys ─────────────────────────────
                # GOVERNED execution uses the frozen, approved key as its authority.
                # Legacy execution continues to require columns_unique_key.
                cur.execute(
                    f"""
                    SELECT source_tag, source_table, columns_unique_key, skip_dedup,
                           execution_authority_mode, approved_source_business_key,
                           key_approval_state, cdc_strategy
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    WHERE LOWER(is_active) = '{MetadataConstants.IS_ACTIVE}' {tag_clause}
                      AND LOWER(COALESCE(skip_dedup, 'n')) != 'y'
                      AND (
                          (
                              UPPER(COALESCE(execution_authority_mode, '')) = 'GOVERNED'
                              AND UPPER(COALESCE(cdc_strategy, 'FULL_SNAPSHOT')) <> 'FULL_SNAPSHOT'
                              AND (
                                  UPPER(COALESCE(key_approval_state, '')) <> 'APPROVED'
                                  OR NULLIF(BTRIM(COALESCE(approved_source_business_key::text, '')), '') IS NULL
                                  OR approved_source_business_key::text IN ('[]', '""')
                              )
                          )
                          OR (
                              UPPER(COALESCE(execution_authority_mode, '')) <> 'GOVERNED'
                              AND (columns_unique_key IS NULL OR TRIM(columns_unique_key) = '')
                          )
                      )
                    """,
                    params,
                )
                for st, tbl, key, skip, mode, approved_key, approval_state, strategy in cur.fetchall():
                    if str(mode or "").upper() == "GOVERNED":
                        errors.append(
                            f"Governed table {st}.{tbl} has skip_dedup='n' but its approved_source_business_key is missing or not APPROVED."
                        )
                    else:
                        errors.append(
                            f"Active table {st}.{tbl} has skip_dedup='n' but columns_unique_key (natural key) is missing or empty."
                        )

        is_valid = len(errors) == 0

        if not quiet:
            self._print_report(is_valid, errors, warnings)

        if not is_valid and raise_on_error:
            raise ValueError(
                f"Metadata validation failed with {len(errors)} error(s). See logs for details."
            )

        return is_valid, errors, warnings

    def _print_report(
        self, is_valid: bool, errors: List[str], warnings: List[str]
    ) -> None:
        sep = "=" * 64
        print(sep)
        print("                 METADATA VALIDATION REPORT")
        print(sep)
        print(f"Status   : {'[ OK ] VALID' if is_valid else '[ FAILED ] INVALID'}")
        print(f"Errors   : {len(errors)}")
        print(f"Warnings : {len(warnings)}")
        print(sep)

        if errors:
            print("ERRORS DETECTED:")
            for i, err in enumerate(errors, 1):
                print(f"  {i}. {err}")
            print(sep)

        if warnings:
            print("WARNINGS DETECTED:")
            for i, warn in enumerate(warnings, 1):
                print(f"  {i}. {warn}")
            print(sep)
