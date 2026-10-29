# src/metadata/metadata_reader.py
"""
Reads the control table db_tables_etl from PostgreSQL.
Returns a list of MetadataRow objects that the orchestrator processes.

Self-healing re-queue logic:
  A table is included in the run when ANY of these conditions is true:
    1. rerun_flag = 'y'            — explicit engineer override
    2. last_success_date IS NULL   — never successfully loaded
    3. last_success_date < TODAY   — not yet loaded today
    4. last batch load_pct < 100   — previous run was incomplete (data loss)
    5. last batch end_time is stale (> max_age_hours old) — data is too old
"""
from typing import List, Optional
import psycopg2

from src.constants import MetadataConstants
from src.metadata.metadata_models import MetadataRow
from src.connections.connection_manager import ConnectionManager


class MetadataReader:

    def __init__(self, conn_manager: ConnectionManager, max_age_hours: int = 3):
        self._conn_manager = conn_manager
        self._max_age_hours = max_age_hours

    @staticmethod
    def _base_select_sql(load_type_condition: str) -> str:
        return f"""
            WITH last_batch AS (
                -- Most recent completed batch run per (source_tag, source_table) for the relevant load types
                SELECT DISTINCT ON (source_tag, source_table)
                    source_tag,
                    source_table,
                    load_pct,
                    end_time
                FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                WHERE status IN ('SUCCESS', 'FAILED', 'PARTIAL')
                  AND load_type {load_type_condition}
                ORDER BY source_tag, source_table, end_time DESC
            )
            SELECT
                etl.table_id                                    AS id,
                etl.source_tag,
                etl.source_schema,
                etl.source_table,
                COALESCE(m.database_type, 'unknown')            AS source_db_type,
                etl.target_database_type,
                etl.landing_table,
                etl.history_table,
                etl.target_table,
                etl.target_schema,
                etl.target_tag,
                etl.is_active,
                etl.priority,
                etl.table_type                                  AS load_type,
                etl.is_cdc,
                etl.cdc_columns,
                etl.cdc_p_days,
                etl.delta_days,
                etl.last_success_timestamp,
                etl.is_reconcile,
                etl.check_reconcile_column                      AS reconcile_column,
                etl.reconcile_measure,
                etl.reconcile_threshold,
                etl.reconcile_action,
                etl.rerun_flag,
                etl.last_success_date,
                -- Self-tuning: fetchsize derived from catalog row count
                LEAST(
                    COALESCE(m.table_row_count, 100000),
                    500000
                )::integer                                      AS chunk_size,
                -- Adaptive Partitioning: num_partitions_override takes precedence; otherwise dynamically calculated
                COALESCE(
                    etl.num_partitions_override,
                    CASE
                        WHEN COALESCE(m.table_row_count, 100000) < COALESCE(etl.minimum_partition_threshold, 50000) THEN 1
                        ELSE LEAST(
                            CEIL(COALESCE(m.table_row_count, 100000)::numeric / COALESCE(etl.target_rows_per_partition, 250000))::integer,
                            COALESCE(etl.maximum_partitions, 32)
                        )
                    END
                )                                               AS num_partitions,
                NULL::varchar                                  AS partition_column,
                etl.source_tag || '_' || etl.source_table || '_' AS source_key_prefix,
                NULL::varchar                                  AS natural_key_column,


                etl.key_source,
                etl.key_confidence,
                m.key_candidate_columns,
                m.key_evidence_type,
                m.key_candidate_nullable,
                m.key_candidate_compatible,
                m.key_evidence_fingerprint AS current_key_evidence_fingerprint,
                etl.key_approval_state,
                etl.approved_source_business_key,
                etl.approved_key_evidence,
                etl.approved_key_evidence_fingerprint,
                etl.key_approved_by,
                etl.key_approved_at,
                etl.key_approval_revision,
                etl.cdc_strategy,
                etl.cdc_source,
                etl.cdc_confidence,
                COALESCE(etl.skip_dedup, 'n')                   AS skip_dedup,
                etl.notes,
                COALESCE(etl.landing_mode, 'METADATA')          AS landing_mode,
                COALESCE(etl.mapping_policy, 'IDENTITY')        AS mapping_policy,
                COALESCE(etl.schema_evolution_policy, 'PERMISSIVE') AS schema_evolution_policy,
                etl.partition_lower_bound,
                etl.partition_upper_bound,
                COALESCE(etl.target_rows_per_partition, 250000)  AS target_rows_per_partition,
                COALESCE(etl.minimum_partition_threshold, 50000) AS minimum_partition_threshold,
                COALESCE(etl.maximum_partitions, 32)            AS maximum_partitions,
                -- Observability: expose last batch health for downstream logging
                lb.load_pct                                     AS last_load_pct,
                lb.end_time                                     AS last_run_end_time
            FROM {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE} etl
            LEFT JOIN {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_CATALOG} m
                   ON  etl.source_tag   = m.database_tag
                   AND etl.source_schema = m.schema_name
                   AND etl.source_table = m.table_name
            LEFT JOIN last_batch lb
                   ON  LOWER(lb.source_tag)   = LOWER(etl.source_tag)
                   AND lb.source_table = etl.source_table
        """

    def get_tables(
        self,
        source_tag: str,
        priority: Optional[int] = None,
        table_name: Optional[str] = None,
        load_type: Optional[str] = None,
        bypass_skip: bool = False,
    ) -> List[MetadataRow]:
        """
        Main query: get all active tables for a source_tag.
        Mirrors the old POSTGRESQL_SELECT_QUERY from Constants.py.

        Priority filters: 1=high, 2=medium, 3=low (can pass None for all)
        Table_name: filter to a specific table (for ad-hoc/debug runs)
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                query = self._base_select_sql("IN ('bronze', 'silver')") + f"""
                    WHERE etl.is_active = %s
                      AND LOWER(etl.source_tag) = LOWER(%s)
                      AND (
                          COALESCE(etl.mapping_policy, 'IDENTITY') = 'IDENTITY'
                          OR etl.table_id IN (
                              SELECT table_id
                              FROM {MetadataConstants.SCHEMA}.{MetadataConstants.COLUMNS_ETL_TABLE}
                              WHERE is_active = %s
                          )
                      )
                """
                params = [MetadataConstants.IS_ACTIVE, source_tag, MetadataConstants.IS_ACTIVE]

                if table_name is not None:
                    query += " AND etl.source_table = %s"
                    params.append(table_name)
                elif not bypass_skip:
                    query += """
                      -- Self-healing skip logic:
                      AND (
                          etl.is_cdc = 'y'  -- CDC tables are always evaluated dynamically in Python should_skip
                          OR etl.rerun_flag = 'y'
                          OR etl.last_success_date IS NULL
                          OR (COALESCE(etl.delta_days, 1) <= 1 AND etl.last_success_date < CURRENT_DATE)
                          OR (COALESCE(etl.delta_days, 1) > 1 AND etl.last_success_date <= CURRENT_DATE - (etl.delta_days * INTERVAL '1 day'))
                          OR etl.last_success_date < NOW() - (GREATEST(COALESCE(etl.delta_days, 1) * 24, %s) * INTERVAL '1 hour')
                          OR lb.load_pct < 100   -- previous run incomplete (target < source)
                          OR lb.load_pct > 100   -- previous run corrupt (target > source: duplicates or bad merge)
                          OR lb.end_time < NOW() - (GREATEST(COALESCE(etl.delta_days, 1) * 24, %s) * INTERVAL '1 hour')
                      )
                    """
                    params.extend([self._max_age_hours, self._max_age_hours])

                if table_name is None and priority is not None:
                    query += " AND priority = %s"
                    params.append(priority)

                if load_type is not None:
                    query += " AND table_type = %s"
                    params.append(load_type)

                query += " ORDER BY priority ASC, table_id ASC"

                cur.execute(query, params)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                return [MetadataRow(**dict(zip(cols, row))) for row in rows]

    def get_gold_tables(self, source_tag: str, table_name: Optional[str] = None, priority: Optional[int] = None, bypass_skip: bool = False) -> List[MetadataRow]:
        """
        Returns ALL active tables for the Gold (Target) layer, regardless of is_cdc.
        is_cdc is read by the ETL worker to choose the write strategy:
          is_cdc = 'y' -> incremental watermark merge
          is_cdc = 'n' -> full truncate-and-reload
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                query = self._base_select_sql("= 'gold'") + f"""
                    WHERE etl.is_active = %s
                      AND LOWER(etl.source_tag) = LOWER(%s)
                      AND (
                          COALESCE(etl.mapping_policy, 'IDENTITY') = 'IDENTITY'
                          OR etl.table_id IN (
                              SELECT table_id
                              FROM {MetadataConstants.SCHEMA}.{MetadataConstants.COLUMNS_ETL_TABLE}
                              WHERE is_active = %s
                          )
                      )
                """
                params = [MetadataConstants.IS_ACTIVE, source_tag, MetadataConstants.IS_ACTIVE]

                if table_name is not None:
                    query += " AND etl.source_table = %s"
                    params.append(table_name)
                elif not bypass_skip:
                    # Self-healing: only include tables that are due for a refresh
                    # CDC tables are always included — their freshness is checked in Python
                    query += """
                      AND (
                          etl.is_cdc = 'y'
                          OR etl.rerun_flag = 'y'
                          OR etl.last_success_date IS NULL
                          OR etl.last_success_date < CURRENT_DATE
                          OR lb.load_pct < 100
                          OR lb.end_time < NOW() - (%s * INTERVAL '1 hour')
                      )
                    """
                    params.append(self._max_age_hours)

                if table_name is None and priority is not None:
                    query += " AND etl.priority = %s"
                    params.append(priority)

                query += " ORDER BY etl.priority ASC, etl.table_id ASC"

                cur.execute(query, params)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                return [MetadataRow(**dict(zip(cols, row))) for row in rows]



    def update_watermark(self, table_id: int, timestamp: str):
        """Reject legacy timestamp watermark writes during the Stage 0 freeze."""
        from src.exceptions import CDCPolicyNotMigratedError

        raise CDCPolicyNotMigratedError(
            f"Legacy watermark updates are disabled for table_id={table_id}."
        )

    def update_table_success(self, table_id: int):
        """
        Mark table as successfully loaded today.
        Sets last_success_date = CURRENT_TIMESTAMP, rerun_flag = 'n'.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    SET last_success_date = CURRENT_TIMESTAMP,
                        rerun_flag = 'n',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE table_id = %s
                """, [table_id])
            conn.commit()

    def get_table_by_schema_and_name(self, schema: Optional[str], table: str, source_tag: str) -> Optional[MetadataRow]:
        """Return a single MetadataRow for a given source schema, table name, and source tag.
        Used by the retry logic to reconstruct the metadata for a failed table.
        """
        rows = self.get_tables(source_tag=source_tag, priority=None, table_name=table, load_type=None, bypass_skip=True)
        if not rows:
            return None
        if schema is None:
            return rows[0]
        for row in rows:
            if row.source_schema == schema and row.source_table == table:
                return row
        return rows[0]

    def clear_retry_flags(self) -> None:
        """Reset all rerun_flag='y' to 'n' after retry processing.
        This prevents infinite retry loops.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE} SET rerun_flag = 'n' WHERE rerun_flag = 'y'"
                )
            conn.commit()

    def update_table_failure(self, table_id: int):
        """
        On failure (count mismatch or exception), flag the table for automatic retry.
        Sets rerun_flag = 'y' so it is picked up on the next scheduled run.
        Does NOT modify last_success_date — the table did not succeed, so the
        date should remain what it was before this run.
        """
        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.TABLES_ETL_TABLE}
                    SET rerun_flag  = 'y',
                        updated_at  = CURRENT_TIMESTAMP
                    WHERE table_id = %s
                """, [table_id])
            conn.commit()
