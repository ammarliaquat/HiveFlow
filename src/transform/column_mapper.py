# src/transform/column_mapper.py
"""
Reads db_columns_etl from PostgreSQL and returns the column mapping for a table.
"""
from typing import List, Dict
import psycopg2

from src.constants import MetadataConstants
from src.connections.connection_manager import ConnectionManager
from src.metadata.metadata_models import ResolvedTableMappings, MappingPolicy


class AmbiguousColumnMappingError(Exception):
    """Raised when a table has multiple active mappings for the same source column."""
    pass

class TargetColumnCollisionError(Exception):
    """Raised when multiple source columns map to the same normalized target identifier."""
    pass

class MissingColumnMappingError(Exception):
    """Raised when no active mappings are found for a table (under EXPLICIT mapping policy)."""
    pass

class MappingTableNotFoundError(Exception):
    """Raised when the requested table identity does not exist."""
    pass

class ColumnMapper:

    def __init__(self, conn_manager: ConnectionManager):
        self._conn_manager = conn_manager

    def resolve_table_mappings(self, table_id: int, mapping_policy: MappingPolicy, frozen_source_schema: list = None) -> ResolvedTableMappings:
        """
        Retrieves explicitly ordered and identity-scoped column mappings for a table.
        Fails closed on missing mappings or ambiguous/duplicate configurations.
        """
        if not isinstance(mapping_policy, MappingPolicy):
            raise ValueError(f"Unknown mapping policy: {mapping_policy}")

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT table_id FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE} WHERE table_id = %s", [table_id])
                if not cur.fetchone():
                    raise MappingTableNotFoundError(f"Table identity {table_id} does not exist.")

                cur.execute(f"""
                    SELECT column_id, column_sequence, source_column, target_column, target_column_datatype AS data_type,
                           source_column_datatype, canonical_type,
                           NULL AS transform_expr, NULL AS default_value
                    FROM {MetadataConstants.SCHEMA}.{MetadataConstants.COLUMNS_ETL_TABLE}
                    WHERE table_id = %s AND is_active = %s
                    ORDER BY COALESCE(column_sequence, 9999), column_id
                """, [table_id, MetadataConstants.IS_ACTIVE])
                cols = [d[0] for d in cur.description]
                raw_mappings = [dict(zip(cols, row)) for row in cur.fetchall()]

        if not raw_mappings:
            if mapping_policy == MappingPolicy.EXPLICIT:
                raise MissingColumnMappingError(f"No active column mappings found for table_id {table_id}.")
            return ResolvedTableMappings.build(table_id, [], mapping_policy=mapping_policy, frozen_source_schema=tuple(frozen_source_schema) if frozen_source_schema else None)


        # MAP-01 target identifier normalization (Spark treats columns as case-insensitive)
        def normalize_identifier(ident: str) -> str:
            return ident.lower().strip() if ident else ""

        seen_source = set()
        seen_target = set()

        for mapping in raw_mappings:
            src = normalize_identifier(mapping["source_column"])
            if src in seen_source:
                raise AmbiguousColumnMappingError(f"Duplicate active source mapping for column '{src}' on table_id {table_id}")
            seen_source.add(src)

            # If target_column is not explicitly defined, it defaults to source_column
            tgt_raw = mapping.get("target_column")
            if not tgt_raw:
                tgt_raw = mapping["source_column"]
            
            tgt = normalize_identifier(tgt_raw)
            if tgt in seen_target:
                raise TargetColumnCollisionError(f"Target column collision: multiple mappings target '{tgt}' on table_id {table_id}")
            seen_target.add(tgt)

        return ResolvedTableMappings.build(table_id, raw_mappings, mapping_policy=mapping_policy, frozen_source_schema=tuple(frozen_source_schema) if frozen_source_schema else None)
