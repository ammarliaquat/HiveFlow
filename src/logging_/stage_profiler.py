# src/logging_/stage_profiler.py
"""
StageProfiler — Fine-grained sub-stage performance telemetry.
Logs per-stage timing, row counts, throughput (rows/sec), persists records to metadata.batch_log.stage_metrics,
and generates formatted execution summary reports.
"""
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from src.connections.connection_manager import ConnectionManager
from src.constants import MetadataConstants

log = logging.getLogger("StageProfiler")


class StageProfiler:

    # 11 Standardized Lifecycle Sub-Stages
    STAGE_01_ORACLE_COUNT_BOUNDS = "01_ORACLE_COUNT_BOUNDS"
    STAGE_02_ORACLE_FETCH_PARTITION = "02_ORACLE_FETCH_PARTITION"
    STAGE_03_PYSPARK_DF_INIT = "03_PYSPARK_DF_INIT"
    STAGE_04_BRONZE_WRITE = "04_BRONZE_WRITE"
    STAGE_05_BRONZE_PARQUET_INSPECT = "05_BRONZE_PARQUET_INSPECT"
    STAGE_06_SILVER_READ = "06_SILVER_READ"
    STAGE_07_SILVER_TRANSFORM = "07_SILVER_TRANSFORM"
    STAGE_08_SILVER_WRITE = "08_SILVER_WRITE"
    STAGE_09_GOLD_TRANSFORM = "09_GOLD_TRANSFORM"
    STAGE_10_GOLD_JDBC_WRITE = "10_GOLD_JDBC_WRITE"
    STAGE_11_RECONCILIATION_CHECK = "11_RECONCILIATION_CHECK"

    def __init__(
        self,
        conn_manager: ConnectionManager,
        run_id: str,
        source_tag: str,
        source_table: str,
        layer: str,
        log_id: Optional[int] = None,
    ):
        self._conn_manager = conn_manager
        self.run_id = run_id
        self.source_tag = source_tag
        self.source_table = source_table
        self.layer = layer
        self.log_id = log_id
        # PERF-01 uses a monotonic clock so wall-clock changes cannot produce a
        # negative per-table duration or hide framework overhead.
        self._pipeline_start_monotonic = time.perf_counter()
        self._stage_start_times: Dict[str, tuple] = {}
        self._completed_stages: Dict[str, Dict[str, Any]] = {}
        self._completed_stage_elapsed: Dict[str, float] = {}

    @staticmethod
    def _stage_key(stage_name: Any) -> str:
        """Normalize Enum lifecycle phases before using them as JSON keys."""
        value = getattr(stage_name, "value", stage_name)
        return str(value)

    def start_stage(self, stage_name: Any) -> None:
        """Mark start timestamp for a sub-stage."""
        self._stage_start_times[self._stage_key(stage_name)] = (time.perf_counter(), datetime.now())

    def end_stage(
        self,
        stage_name: Any,
        row_count: Optional[int] = None,
    ) -> float:
        """
        Mark completion timestamp for a sub-stage, compute throughput,
        log to stdout, and persist JSON metrics into metadata.batch_log.stage_metrics.
        Returns elapsed seconds.
        """
        end_wall = time.perf_counter()
        end_dt = datetime.now()

        stage_key = self._stage_key(stage_name)
        if stage_key not in self._stage_start_times:
            start_wall, start_dt = end_wall, end_dt
        else:
            start_wall, start_dt = self._stage_start_times.pop(stage_key)

        elapsed = max(0.0, end_wall - start_wall)
        rps = (row_count / elapsed) if (row_count is not None and elapsed > 0) else None

        safe_elapsed = float(round(elapsed, 6))
        safe_rows = int(row_count) if row_count is not None else None
        safe_rps = float(round(rps, 2)) if rps is not None else None

        self._completed_stages[stage_key] = {
            "sec": safe_elapsed,
            "rows": safe_rows,
            "rps": safe_rps,
            "layer": self.layer,
        }
        self._completed_stage_elapsed[stage_key] = elapsed

        # Format console log line
        rows_str = f"{row_count:,} rows" if row_count is not None else "N/A"
        rps_str = f"({rps:,.2f} rows/sec)" if rps is not None else ""
        log.info(
            f"[Stage Profiler] {self.source_table} [{self.layer.upper()}] "
            f"Stage '{stage_name}': {rows_str} in {elapsed:.3f}s {rps_str}"
        )

        # Persist JSON payload into metadata.batch_log.stage_metrics
        if self.log_id:
            payload = {
                stage_key: self._completed_stages[stage_key]
            }
            safe_log_id = int(self.log_id)
            try:
                import json
                with self._conn_manager.get_metadata_db_connection() as conn:
                    with conn.cursor() as cur:
                        if "EXTRACT" in stage_key.upper():
                            cur.execute(
                                f"""
                                UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                                SET stage_metrics = COALESCE(stage_metrics, '{{}}'::jsonb) || %s::jsonb,
                                    current_phase = %s,
                                    extracted_count = COALESCE(%s, extracted_count),
                                    last_progress_update = CURRENT_TIMESTAMP
                                WHERE id = %s
                                """,
                                (json.dumps(payload), stage_key.upper(), safe_rows, safe_log_id)
                            )
                        else:
                            cur.execute(
                                f"""
                                UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                                SET stage_metrics = COALESCE(stage_metrics, '{{}}'::jsonb) || %s::jsonb,
                                    current_phase = %s,
                                    last_progress_update = CURRENT_TIMESTAMP
                                WHERE id = %s
                                """,
                                (json.dumps(payload), stage_key.upper(), safe_log_id)
                            )
                    conn.commit()
            except Exception as e:
                log.warning(f"[Stage Profiler] Could not update stage_metrics: {e}")

        return elapsed

    def get_completed_stages(self) -> Dict[str, Dict[str, Any]]:
        """Return dict of completed stage metrics."""
        return dict(self._completed_stages)

    def complete_pipeline(self) -> Dict[str, Any]:
        """Return the schema-versioned PERF-01 telemetry envelope.

        This method deliberately does not trigger Spark actions.  Stage timings
        must wrap actions that the pipeline already requires; adding diagnostic
        ``count()`` calls would change the benchmark being measured.
        """
        total_sec = max(0.0, time.perf_counter() - self._pipeline_start_monotonic)
        measured_stage_sec = sum(self._completed_stage_elapsed.values())
        unaccounted_overhead_sec = total_sec - measured_stage_sec

        return {
            "schema_version": "perf-01.v1",
            "layer": self.layer,
            "source_tag": self.source_tag,
            "source_table": self.source_table,
            "total_sec": round(total_sec, 6),
            "measured_stage_sec": round(measured_stage_sec, 6),
            "unaccounted_overhead_sec": round(unaccounted_overhead_sec, 6),
            "stages": self.get_completed_stages(),
            "telemetry_persistence": "pending" if self.log_id else "not_requested",
        }

    def get_summary_report(self) -> str:
        """Generate a formatted ASCII breakdown table of all completed sub-stages."""
        if not self._completed_stages:
            return f"[Stage Profiler] No completed stages recorded for {self.source_table}."

        total_sec = sum(m["sec"] for m in self._completed_stages.values())
        lines = [
            f"\n==========================================================================================",
            f"STAGE PROFILER SUMMARY: {self.source_table} [{self.layer.upper()}] (Run ID: {self.run_id})",
            f"==========================================================================================",
            f"{'Stage Name':<35} | {'Duration (s)':<12} | {'% Total':<8} | {'Rows':<12} | {'Throughput (r/s)':<16}",
            f"------------------------------------------------------------------------------------------"
        ]

        for stage, m in self._completed_stages.items():
            pct = (m["sec"] / total_sec * 100) if total_sec > 0 else 0.0
            rows_str = f"{m['rows']:,}" if m["rows"] is not None else "N/A"
            rps_str = f"{m['rps']:,.2f}" if m["rps"] is not None else "N/A"
            lines.append(
                f"{stage:<35} | {m['sec']:<12.3f} | {pct:<7.1f}% | {rows_str:<12} | {rps_str:<16}"
            )

        lines.append(f"------------------------------------------------------------------------------------------")
        lines.append(f"{'TOTAL PIPELINE TIME':<35} | {total_sec:<12.3f} | 100.0%  |")
        lines.append(f"==========================================================================================\n")
        return "\n".join(lines)

    def print_summary_report(self) -> None:
        """Print formatted summary report to logger."""
        report = self.get_summary_report()
        for line in report.splitlines():
            log.info(line)
