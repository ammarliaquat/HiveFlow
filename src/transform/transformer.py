# src/transform/transformer.py
# -----------------------------------------------------------------------------
# STEP 2 IN OUR DATA PIPELINE: THE TRANSFORMER (SILVER CLEANING & QUALITY)
# -----------------------------------------------------------------------------
# Think of this file like our Chief Quality Inspector and Toy Cleaner!
# After the Extractor brings back the raw toys and books from the warehouse,
# the Transformer inspects them, wipes off any dirt, checks for missing nametags,
# removes duplicate twin copies, stamps official inspection badges,
# and makes sure every column is in the exact right shape before saving!
# -----------------------------------------------------------------------------
"""
Generic transformations applied to every DataFrame before writing.
Replaces the Oracle MERGE logic and deduplication patterns from the old pipeline.

All transforms are pure PySpark — no pandas, no UDFs unless necessary.
"""

# [Imports - gathering our tools for cleaning and measuring]
# uuid is for making unique random secret codes (like lottery tickets).
import uuid
# datetime helps our computer check what time and date it is right now.
from datetime import datetime
# DataFrame is Spark's giant spreadsheet held in computer memory.
from pyspark.sql import DataFrame
# F gives us lots of math, text, and date formulas for our spreadsheet.
from pyspark.sql import functions as F
# Window lets us group rows together (like grouping students in a classroom) to rank them.
from pyspark.sql.window import Window

# MetadataRow has the instruction card for the table.
from src.metadata.metadata_models import MetadataRow
# AuditColumns and DmlStatus hold the official standard names for our inspection stickers.
from src.constants import AuditColumns, DmlStatus, DefaultValues
# CanonicalType helps us translate messy database types into clean standard data types.
from src.types.canonical import (
    CanonicalType, resolve_canonical, get_spark_column_expr
)


# -----------------------------------------------------------------------------
# Custom Error: Raised when a table is missing its required unique ID column!
# -----------------------------------------------------------------------------
class NaturalKeyResolutionException(Exception):
    """Raised when configured natural key column(s) cannot be resolved against DataFrame columns."""
    pass


# -----------------------------------------------------------------------------
# The Transformer Class: Our Chief Quality Inspector
# -----------------------------------------------------------------------------
class Transformer:

    # The setup function: runs when we wake up our inspector
    def __init__(self, run_id: str):
        # Save the unique mission tracking number (run_id) for this whole ETL run
        self.run_id = run_id

    # -------------------------------------------------------------------------
    # Helper 1: Add a digital fingerprint (hash) to each row
    # -------------------------------------------------------------------------
    def add_business_hash(self, df: DataFrame, meta: MetadataRow) -> DataFrame:
        """
        Compute a SHA-256 business row hash across sorted payload columns.
        Excludes technical and transient audit columns.
        Uses F.to_json(F.struct(*sorted_cols)) to avoid delimiter collision risks.
        """
        # A list of technical sticker columns that we do NOT include in the fingerprint calculation.
        # (For example, we don't care what time the sticker was applied, only the actual data matters!)
        technical_columns = {
            AuditColumns.DML_TIMESTAMP,
            AuditColumns.LANDING_INGESTION_TIMESTAMP,
            AuditColumns.PIPELINE_RUN_ID,
            AuditColumns.DML_STATUS,
            AuditColumns.SOURCE_TAG,
            AuditColumns.SOURCE_IDENTIFICATION_KEY,
            AuditColumns.SOURCE_IDENTIFICATION_TABLE,
            "row_hash",
        }
        # Find all the actual business data columns and sort their names alphabetically from A to Z
        business_cols = sorted([c for c in df.columns if c not in technical_columns])
        # If there are no business columns at all, return the spreadsheet unchanged
        if not business_cols:
            return df

        # Pack all the business columns together into a clean JSON text parcel.
        # F.coalesce replaces any empty (NULL) values with empty text "" so it never crashes!
        json_struct = F.to_json(
            F.struct(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols])
        )
        # Calculate a SHA-256 math fingerprint from the JSON text and save it into a new column called "row_hash"
        return df.withColumn("row_hash", F.sha2(json_struct, 256))

    # -------------------------------------------------------------------------
    # Helper 2: Stamp official audit stickers onto every row
    # -------------------------------------------------------------------------
    def add_audit_columns(self, df: DataFrame, meta: MetadataRow) -> DataFrame:
        """
        Add standard audit columns to every output row.
        These mirror the dml_* columns in the Oracle target tables.
        """
        return (
            df
            # Sticker 1: Exact current clock timestamp when this record was processed
            .withColumn(AuditColumns.DML_TIMESTAMP, F.current_timestamp())
            # Sticker 2: DML status flag (by default 'I' for Insert)
            .withColumn(AuditColumns.DML_STATUS, F.lit(DefaultValues.DML_STATUS))
            # Sticker 3: Name tag of which source database it came from
            .withColumn(AuditColumns.SOURCE_TAG, F.lit(meta.source_tag))
            # Sticker 4: The unique tracking number for this pipeline run
            .withColumn(AuditColumns.PIPELINE_RUN_ID, F.lit(self.run_id))
            # Sticker 5: Full lineage tag combining source tag, table name, and business key names
            .withColumn(
                AuditColumns.SOURCE_IDENTIFICATION_TABLE,
                F.lit(f"{meta.source_tag}-{meta.source_table}-{'-'.join(meta.canonical_source_business_key or getattr(meta, 'canonical_business_key_columns', ()) or tuple())}")
            )
            # Sticker 6: Timestamp marking when the data was landed
            .withColumn(AuditColumns.LANDING_INGESTION_TIMESTAMP, F.current_timestamp())
        )

    # -------------------------------------------------------------------------
    # Helper 3: Find and verify natural key columns (like student roll numbers)
    # -------------------------------------------------------------------------
    def _parse_natural_keys(self, df: DataFrame, meta: MetadataRow) -> list:
        """
        Parses and resolves the natural key column configuration from metadata
        against the actual DataFrame columns using case-insensitive lookup.

        Supports:
          - Comma-separated: 'mrcode, adm_no'
          - Plus-separated: 'mrcode+adm_no'
          - Single column: 'emigrant_id'

        Raises:
            NaturalKeyResolutionException if any configured key token is missing.
        """
        # Grab the configured business key tuple from the metadata instruction card
        keys = (
            meta.canonical_source_business_key
            or getattr(meta, "canonical_business_key_columns", ())
            or tuple()
        )
        # If no keys were configured, return an empty list
        if not keys:
            return []

        # Store the configured keys
        configured_keys = keys

        # Build a lowercase lookup dictionary for columns so "ID", "id", and "Id" all match
        df_cols_map = {c.lower(): c for c in df.columns}
        # List of successfully matched column names
        resolved_keys = []
        # List of any missing column names that couldn't be found
        missing_keys = []

        # Check each required key column one by one
        for k in configured_keys:
            # If the lowercase column name exists in our spreadsheet:
            if k.lower() in df_cols_map:
                # Add the actual casing of the column name to our resolved list
                resolved_keys.append(df_cols_map[k.lower()])
            else:
                # Oops, this column is completely missing from the spreadsheet!
                missing_keys.append(k)

        # If even one key column was missing, raise a clear error so we don't corrupt the database!
        if missing_keys:
            raise NaturalKeyResolutionException(
                f"[Metadata Configuration Error] Natural key column(s) {missing_keys} "
                f"configured for table '{meta.source_table}' not found in DataFrame columns: {list(df.columns)}. "
                f"Check metadata.db_tables_etl configuration for '{meta.source_table}'."
            )

        # Return the verified list of key column names
        return resolved_keys

    # -------------------------------------------------------------------------
    # Helper 4: Build a combined unique business identifier
    # -------------------------------------------------------------------------
    def add_source_identification_key(self, df: DataFrame, meta: MetadataRow) -> DataFrame:
        """
        Build the source_identification_key column using the Standard Industry Entity-Scoped Hierarchy:
        Pattern: '{source_tag}_{source_table}_{key_values}'
        If natural_key consists of multiple columns (composite), they are concatenated with '_'
        e.g. 'msopforg_opforg_fayears_2024'
        For keyless tables, generates deterministic synthetic key:
        e.g. '{source_tag}_{source_table}_synthetic_{hash16}'
        """
        if str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT":
            return df

        # Find and verify our natural key columns using Helper 3
        key_cols = self._parse_natural_keys(df, meta)

        source_tag = getattr(meta, "source_tag", "") or ""
        source_table = getattr(meta, "source_table", "") or ""
        prefix = getattr(meta, "source_key_prefix", None)

        canonical_prefix = (
            f"{source_tag}_{source_table}_"
            if (source_tag and source_table)
            else (f"{source_tag}_" if source_tag else "")
        )

        # If prefix is missing, empty, or legacy prefix, upgrade to canonical prefix
        if not prefix or prefix in (
            f"{source_tag}-",
            f"{source_tag}-{source_table}-",
            f"urn_{source_tag}_{source_table}_",
            f"urn_{source_tag}_",
            "urn_",
        ):
            prefix = canonical_prefix

        # Handle keyless tables with deterministic synthetic identity
        if not key_cols:
            synthetic_prefix = (
                f"{source_tag}_{source_table}_synthetic_"
                if (source_tag and source_table)
                else "synthetic_"
            )
            if "row_hash" in df.columns:
                hash_expr = F.substring(F.col("row_hash"), 1, 16)
            else:
                technical_columns = {
                    AuditColumns.DML_TIMESTAMP,
                    AuditColumns.LANDING_INGESTION_TIMESTAMP,
                    AuditColumns.PIPELINE_RUN_ID,
                    AuditColumns.DML_STATUS,
                    AuditColumns.SOURCE_TAG,
                    AuditColumns.SOURCE_IDENTIFICATION_KEY,
                    AuditColumns.SOURCE_IDENTIFICATION_TABLE,
                    "row_hash",
                }
                business_cols = sorted([c for c in df.columns if c not in technical_columns])
                if business_cols:
                    json_struct = F.to_json(
                        F.struct(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols])
                    )
                    hash_expr = F.substring(F.sha2(json_struct, 256), 1, 16)
                else:
                    hash_expr = F.lit("0" * 16)

            return df.withColumn(
                AuditColumns.SOURCE_IDENTIFICATION_KEY,
                F.concat(F.lit(synthetic_prefix), hash_expr)
            )


        # Warn if the natural key column contains NULLs — indicates misconfigured metadata
        import logging as _logging
        _log = _logging.getLogger("transformer")
        null_check_col = key_cols[0]
        if _log.isEnabledFor(_logging.WARNING):
            try:
                null_count = df.filter(F.col(null_check_col).isNull()).count()
                if null_count > 0:
                    _log.warning(
                        f"[Transformer] {meta.source_table}: natural key '{null_check_col}' "
                        f"has {null_count} NULL value(s). Check metadata.columns_unique_key configuration. "
                        f"These rows will coalesce to 'null' in source_identification_key."
                    )
            except Exception:
                pass

        # Coalesce, trim, and format keys
        cleaned_key_cols = [
            F.coalesce(F.trim(F.col(k).cast("string")), F.lit("null"))
            for k in key_cols
        ]

        if len(cleaned_key_cols) > 1:
            key_expr = F.concat_ws('_', *cleaned_key_cols)
        else:
            key_expr = cleaned_key_cols[0]

        # Combine prefix with key expression
        if prefix:
            res_expr = F.concat(F.lit(prefix), key_expr)
        else:
            res_expr = key_expr

        return df.withColumn(
            AuditColumns.SOURCE_IDENTIFICATION_KEY,
            res_expr
        )

    # -------------------------------------------------------------------------
    # Helper 4b: Sanitize natural key columns to prevent NULLs in target PKs
    # -------------------------------------------------------------------------
    def sanitize_business_keys(self, df: DataFrame, meta: MetadataRow) -> DataFrame:
        """
        Sanitize business key columns in the DataFrame by replacing NULLs with standard
        sentinel values ('N/A' for string, 0 for numeric) to guarantee target-side
        relational integrity and non-null PRIMARY KEY conformance.
        """
        key_cols = self._parse_natural_keys(df, meta)
        if not key_cols or str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT":
            return df

        for col_name in key_cols:
            field = next((f for f in df.schema.fields if f.name.lower() == col_name.lower()), None)
            if field:
                dtype_str = str(field.dataType).lower()
                if "string" in dtype_str:
                    df = df.withColumn(field.name, F.coalesce(F.trim(F.col(field.name)), F.lit("N/A")))
                elif any(t in dtype_str for t in ["int", "long", "short", "byte", "decimal", "double", "float"]):
                    df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(0)))
        return df

    # -------------------------------------------------------------------------
    # Helper 5: Deduplicate - remove identical twin rows, keeping the newest
    # -------------------------------------------------------------------------
    def deduplicate(self, df: DataFrame, meta: MetadataRow, order_col: str = None) -> DataFrame:
        """
        Deduplication using ROW_NUMBER() OVER (PARTITION BY natural_keys ORDER BY order_col DESC).
        Keeps only the latest record per natural key / composite keys.
        """
        # Find which columns define the identity of a row (like student ID)
        key_cols = self._parse_natural_keys(df, meta)
        # If no keys were found, or the table explicitly asked to skip deduplication ('y'), return unchanged
        if not key_cols or getattr(meta, 'skip_dedup', 'n').lower() == 'y' or str(getattr(meta, 'landing_mode', 'METADATA')).upper() == 'DIRECT':
            return df

        # Figure out which column tells us which record is the newest
        order_column = order_col or AuditColumns.DML_TIMESTAMP
        # If the requested order column doesn't exist in the spreadsheet:
        if order_column not in df.columns:
            # Look for common date columns like 'updated_at' or 'created_at'
            for possible_col in ['updated_at', 'created_at', 'update_date', 'created_date']:
                if possible_col in df.columns:
                    order_column = possible_col
                    break
            else:
                # Fall back to ordering by the key column itself
                order_column = key_cols[0]

        # Define our ranking window: group by the key columns, and sort newest-first (descending)
        window = Window.partitionBy(*key_cols).orderBy(
            F.col(order_column).desc()
        )
        return (
            df
            # Assign row numbers: 1 = newest, 2 = second newest, 3 = third newest...
            .withColumn("_rn", F.row_number().over(window))
            # Keep ONLY row number 1 (the newest one!) and throw away any older twins
            .filter(F.col("_rn") == 1)
            # Drop the temporary "_rn" counter column so the table stays clean
            .drop("_rn")
        )

    # -------------------------------------------------------------------------
    # Helper 6: Cast columns to their proper data types (numbers, dates, text)
    # -------------------------------------------------------------------------
    def cast_columns(self, df: DataFrame, meta: MetadataRow) -> DataFrame:
        """
        Apply column-level casting from db_columns_etl metadata using case-insensitive column matching.
        """
        # If no column mapping rules exist, return the spreadsheet as-is
        if not meta.resolved_mappings:
            return df

        # If the mapping policy is IDENTITY (meaning: keep everything exactly as-is), return as-is
        from src.metadata.metadata_models import MappingPolicy
        if meta.resolved_mappings.mapping_policy == MappingPolicy.IDENTITY:
            return df

        # Get our list of column mapping instructions
        column_map = meta.resolved_mappings.mappings
        # Map lowercase column names to their actual names in the DataFrame
        existing_cols_map = {c.lower(): c for c in df.columns}
        # List of new transformed column formulas
        select_exprs = []
        # Track which target column names we've already created
        mapped_targets = set()

        # Loop through each column mapping rule
        for mapping in column_map:
            # The source column name
            src = mapping["source_column"]
            # The target column name (or default to source column name if blank)
            tgt = mapping["target_column"] or mapping["source_column"]
            # Target data type (like 'INT', 'VARCHAR')
            dtype = mapping.get("data_type")
            # Source data type
            src_dtype = mapping.get("source_column_datatype")
            # Custom SQL transform formula if provided
            expr = mapping.get("transform_expr")
            # Default fallback value if the field is empty
            default = mapping.get("default_value")

            # Lowercase version for matching
            src_lower = src.lower()
            # If this source column doesn't exist in our DataFrame, skip it
            if src_lower not in existing_cols_map:
                continue

            # The actual column name in the DataFrame
            actual_src = existing_cols_map[src_lower]

            # Build the column expression
            col_expr = None
            # If a custom transform formula was given, use it
            if expr:
                col_expr = F.expr(expr)
            else:
                # Otherwise, resolve canonical type representation
                canonical_str = mapping.get("canonical_type")
                canonical = None
                if canonical_str and canonical_str != "UNKNOWN":
                    try:
                        canonical = CanonicalType(canonical_str)
                    except ValueError:
                        pass
                if not canonical and src_dtype:
                    canonical = resolve_canonical(src_dtype, "oracle")

                # Get Spark's column casting expression for this data type
                col_expr = get_spark_column_expr(actual_src, canonical, dtype)

            # If a default value was configured, use F.coalesce to fill in blanks
            if default is not None:
                col_expr = F.coalesce(col_expr, F.lit(default))

            # Add this converted column to our list with its new target name
            select_exprs.append(col_expr.alias(tgt))
            # Remember that we finished this target column
            mapped_targets.add(tgt.lower())

        # Retain any audit columns or other unmapped columns currently in the DataFrame
        for col in df.columns:
            if col.lower() not in mapped_targets:
                select_exprs.append(F.col(col))

        # Apply all the selected column transformations to our spreadsheet
        return df.select(*select_exprs)

    # -------------------------------------------------------------------------
    # Helper 7: Pick only the final target columns plus audit badges
    # -------------------------------------------------------------------------
    def select_target_columns(self, df: DataFrame, meta: MetadataRow) -> DataFrame:
        """Select defined target columns plus audit columns using case-insensitive matching."""
        if str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT":
            return df

        # If no mapping rules exist, return the spreadsheet as-is
        if not meta.resolved_mappings:
            return df

        from src.metadata.metadata_models import MappingPolicy
        # If policy is IDENTITY, use the frozen source schema columns
        if meta.resolved_mappings.mapping_policy == MappingPolicy.IDENTITY:
            # Use the frozen source schema as the target columns
            target_cols = list(meta.resolved_mappings.frozen_source_schema or tuple())
            if not target_cols:
                # Fallback to current columns if for some reason frozen schema wasn't captured
                target_cols = df.columns
        else:
            # Collect the target column names from the mapping rules
            target_cols = [m["target_column"] or m["source_column"] for m in meta.resolved_mappings.mappings]

        # Collect all official audit column names from AuditColumns
        audit_cols = [c for c in vars(AuditColumns).values()
                      if isinstance(c, str) and not c.startswith("_")]
        # Combine target columns + audit columns, removing any duplicates while keeping order
        final_cols = list(dict.fromkeys(target_cols + audit_cols))

        # Map lowercase names to actual DataFrame column names
        df_cols_map = {c.lower(): c for c in df.columns}
        # Keep only the columns that actually exist in the current DataFrame
        existing = [df_cols_map[c.lower()] for c in final_cols if c.lower() in df_cols_map]
        # Return the clean spreadsheet with only the approved columns
        return df.select(*existing)

    # -------------------------------------------------------------------------
    # Helper 8: Convert every column into plain text (for Bronze landing)
    # -------------------------------------------------------------------------
    def cast_all_to_string(self, df: DataFrame) -> DataFrame:
        """
        Convert every column in the DataFrame to StringType.
        Used exclusively for Bronze (Landing) to preserve the raw JDBC string
        representation of the data, preventing Spark's CSV writer from applying
        numeric rounding, scientific notation, or date formatting hacks.
        """
        # Bring in Spark functions and StringType tool
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        
        # Turn every single column in the table into a plain text String
        string_cols = [F.col(c).cast(StringType()).alias(c) for c in df.columns]
        # Return the DataFrame with all text columns
        return df.select(*string_cols)
