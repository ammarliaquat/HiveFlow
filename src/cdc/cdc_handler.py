# src/cdc/cdc_handler.py
"""
Handles Change Data Capture (CDC) logic.

Strategy: Timestamp-based CDC with a safety lookback window.
- Read last_success_timestamp from metadata table (the watermark)
- Build a WHERE clause: WHERE cdc_col >= last_run - cdc_p_days
- After successful load: update last_success_timestamp = NOW()

This mirrors the old refresh_tables_cdc_12AM() method.
"""
from datetime import datetime, timedelta
from typing import Optional

from src.metadata.metadata_models import MetadataRow
from src.metadata.metadata_reader import MetadataReader
from src.constants import DefaultValues
from src.exceptions import CDCPolicyNotMigratedError


import logging

logger = logging.getLogger(__name__)


class CDCHandler:

    def __init__(self, metadata_reader: MetadataReader, spark=None):
        self._metadata_reader = metadata_reader
        self._spark = spark

    def build_cdc_filter(self, meta: MetadataRow) -> Optional[str]:
        """
        Build the SQL WHERE clause fragment for CDC extraction.

        Returns None if CDC is not enabled or no watermark exists
        (in which case a full load should be done).

        Example output:
          "updated_at >= '2024-01-08 00:00:00' OR created_at >= '2024-01-08 00:00:00'"
        """
        if meta.use_cdc:
            raise CDCPolicyNotMigratedError(
                f"Legacy CDC policy for {meta.source_table} must be migrated before execution."
            )
        if not meta.use_cdc:
            return None

        if not meta.cdc_column_list:
            return None

        if getattr(meta, "cdc_strategy", "TIMESTAMP") == "INSERT_ONLY":
            logger.info(f"[CDC Warning] {meta.source_table}: Strategy is INSERT_ONLY (column={meta.cdc_columns}). Updates to existing rows will not be captured.")

        lookback_days = meta.cdc_p_days or DefaultValues.CDC_LOOKBACK_DAYS

        if meta.last_success_timestamp:
            cutoff = meta.last_success_timestamp - timedelta(days=lookback_days)
            cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

            db_type = meta.source_db_type.lower() if meta.source_db_type else "unknown"
            if db_type == "mysql":
                def quote_col(c): return f"`{c}`"
            elif db_type in ("oracle", "ora"):
                def quote_col(c): return c
            else:
                def quote_col(c): return f'"{c}"'

            # Build OR clause across all CDC columns
            conditions = [
                f"{quote_col(col)} >= '{cutoff_str}'" for col in meta.cdc_column_list
            ]
            filter_clause = " OR ".join(conditions)
            logger.info(
                f"[CDC] {meta.source_table}: watermark={meta.last_success_timestamp}, "
                f"cutoff={cutoff_str} (lookback={lookback_days}d). Filter: {filter_clause}"
            )
            print(f"[CDC] {meta.source_table}: watermark={meta.last_success_timestamp}, cutoff={cutoff_str} (lookback={lookback_days}d) -> CDC filter applied: {filter_clause}")
            return filter_clause
        else:
            # No watermark = first run = full load (no filter)
            logger.info(
                f"[CDC] {meta.source_table}: no watermark found (first run) "
                f"→ performing full load"
            )
            print(f"[CDC] {meta.source_table}: no watermark found -> performing full load (first-run)")
            return None

    def update_watermark(self, meta: MetadataRow, success_timestamp: Optional[datetime] = None):
        """
        Update the watermark after a successful run.
        Called by the orchestrator after a CDC load completes.
        """
        raise CDCPolicyNotMigratedError(
            f"Legacy watermark updates are disabled for {meta.source_table}."
        )

    def get_max_source_timestamp(self, meta: MetadataRow) -> Optional[datetime]:
        """Query the source database for the maximum timestamp across CDC columns."""
        if not meta.use_cdc or not meta.cdc_column_list:
            return None

        spark = self._spark
        if not spark:
            # Fallback to getOrCreate if not provided (e.g. in legacy tests)
            from pyspark.sql import SparkSession
            try:
                spark = SparkSession.builder.getOrCreate()
            except Exception:
                return None
        if not spark:
            return None

        try:
            conn = self._metadata_reader._conn_manager.get(meta.source_tag, expected_role="source")
            db_type = conn.db_type.lower()

            max_ts_exprs = []
            if db_type == "mysql":
                def quote_col(c): return f"`{c}`"
            elif db_type in ("oracle", "ora"):
                def quote_col(c): return c
            else:
                def quote_col(c): return f'"{c}"'

            for col in meta.cdc_column_list:
                max_ts_exprs.append(f"MAX({quote_col(col)}) AS {quote_col(col + '_max')}")

            select_expr = ", ".join(max_ts_exprs)
            from src.utils.db_utils import quote_table_name, apply_jdbc_options
            alias_kw = " " if conn.db_type == "oracle" else " AS "
            query = f"(SELECT {select_expr} FROM {quote_table_name(meta.full_source_table, conn.db_type)}){alias_kw}max_ts_q"

            df = (
                apply_jdbc_options(spark.read.format("jdbc"), conn)
                .option("dbtable", query)
                .load()
            )
            row = df.collect()[0]
            row_dict = row.asDict() if hasattr(row, "asDict") else dict(row)
            row_dict_lower = {str(k).lower(): v for k, v in row_dict.items()}
            max_vals = []
            for col in meta.cdc_column_list:
                val = row_dict_lower.get((col + '_max').lower())
                if val:
                    if isinstance(val, str):
                        try:
                            val = datetime.strptime(val.split('.')[0], "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            pass
                    if isinstance(val, datetime):
                        max_vals.append(val)
            if max_vals:
                return max(max_vals)
        except Exception as e:
            logger.warning(f"Could not query maximum source timestamp for {meta.source_table}: {e}")
        return None

    def should_skip(self, meta: MetadataRow) -> bool:
        """Check if this table should be skipped (i.e. not due for CDC run)."""
        if meta.use_cdc:
            raise CDCPolicyNotMigratedError(
                f"Legacy CDC policy for {meta.source_table} must be migrated before execution."
            )
        # If rerun flag is set, never skip
        if meta.rerun_flag == 'y':
            return False

        # If there is no last success timestamp, we must run (never skip)
        if not meta.last_success_timestamp:
            return False

        # If delta_days is configured (> 1), check if enough days have elapsed
        delta_days = getattr(meta, "delta_days", 1) or 1
        if delta_days > 1:
            elapsed = datetime.now() - meta.last_success_timestamp
            if elapsed.days < delta_days:
                logger.info(f"[CDC] {meta.source_table}: delta_days={delta_days} not met (elapsed={elapsed.days}d). Skipping.")
                return True

        max_source_ts = self.get_max_source_timestamp(meta)
        if max_source_ts is None:
            # If we cannot fetch the timestamp (empty table or query error), skip to be safe
            logger.info(f"[CDC] {meta.source_table}: No new updates or empty source table. Skipping.")
            return True

        # Strip timezone for naive datetime comparison
        max_ts = max_source_ts.replace(tzinfo=None)
        watermark = meta.last_success_timestamp.replace(tzinfo=None)

        if max_ts > watermark:
            logger.info(f"[CDC] {meta.source_table}: New updates found (Max Source TS: {max_ts} > Watermark: {watermark}). Processing.")
            return False
        else:
            logger.info(f"[CDC] {meta.source_table}: No new updates (Max Source TS: {max_ts} <= Watermark: {watermark}). Skipping.")
            return True
