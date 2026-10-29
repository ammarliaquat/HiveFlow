# src/runtime/runtime_optimizer.py
"""
Runtime Optimizer Service (Phase 2)
Computes execution parameters dynamically at runtime by combining:
- Metadata facts (connections, capabilities, db_columns)
- Historical statistics (performance_profile)
- Current dataset characteristics
- Cluster resources
"""
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import logging

from src.connections.connection_manager import ConnectionManager
from src.metadata.metadata_models import MetadataRow

log = logging.getLogger("RuntimeOptimizer")


@dataclass
class ExecutionPlan:
    source_tag: str
    source_table: str
    fetch_size: int
    num_partitions: int
    parallel_concurrency: int
    resource_tier: str  # 'SMALL', 'MEDIUM', 'LARGE'
    is_lob_throttled: bool
    estimated_size_mb: float
    estimated_rows: int = 0
    optimizer_version: str = "1.0.0"
    reason: str = "Standard execution plan"
    decisions: List[Dict[str, str]] = None

    def __post_init__(self):
        if self.decisions is None:
            self.decisions = []

# Backwards compatibility alias
OptimizationPlan = ExecutionPlan


class RuntimeOptimizer:
    """
    Decoupled Runtime Decision Engine.
    Ensures ConnectionManager remains focused purely on infrastructure and credentials.
    """

    def __init__(self, conn_manager: ConnectionManager):
        self.conn_manager = conn_manager

    def fetch_historical_stats(self, source_tag: str, table_name: str) -> Dict[str, Any]:
        """Fetch historical observed telemetry from metadata.performance_profile."""
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT average_row_count, average_size_mb, average_duration_sec,
                               average_extract_speed_rps, p95_duration_sec
                        FROM metadata.performance_profile
                        WHERE LOWER(source_tag) = LOWER(%s)
                          AND LOWER(source_table) = LOWER(%s)
                    """, [source_tag, table_name])
                    row = cur.fetchone()
                    if row:
                        return {
                            "average_row_count": row[0] or 0,
                            "average_size_mb": float(row[1] or 0.0),
                            "average_duration_sec": float(row[2] or 0.0),
                            "average_extract_speed_rps": float(row[3] or 0.0),
                            "p95_duration_sec": float(row[4] or 0.0),
                        }
        except Exception as e:
            log.warning(f"[RuntimeOptimizer] Could not fetch performance profile for '{table_name}': {e}")
        
        return {
            "average_row_count": 0,
            "average_size_mb": 0.0,
            "average_duration_sec": 0.0,
            "average_extract_speed_rps": 0.0,
            "p95_duration_sec": 0.0,
        }

    def calculate_fetch_size(
        self,
        source_tag: str,
        table_name: str,
        has_lob: bool = False,
        default_fetch: int = 10000,
        lob_count: int = 0,
        to_char_projected: bool = False
    ) -> int:
        """
        Calculate memory-safe JDBC fetch size.
        If TO_CHAR() SQL projection is active, Oracle returns VARCHAR2 strings (fetch_size = 2000).
        Otherwise, if LOB locators exist, throttle to 25-100 depending on LOB count.
        """
        if to_char_projected:
            log.info(
                f"[RuntimeOptimizer] TO_CHAR() SQL projection active for table '{table_name}' (count={lob_count}). "
                f"Oracle engine returns VARCHAR2 strings. Setting fetchsize to 2000 for high-throughput extraction."
            )
            return 2000

        if has_lob or lob_count > 0:
            if lob_count > 10:
                target_fetch = 25
            elif lob_count >= 6:
                target_fetch = 50
            elif lob_count >= 3:
                target_fetch = 100
            elif lob_count >= 1:
                target_fetch = 500
            else:
                target_fetch = 100

            log.info(
                f"[RuntimeOptimizer] LOB columns detected on table '{table_name}' (count={lob_count}). "
                f"Dynamically throttling fetchsize to {target_fetch} to protect Executor JVM heap."
            )
            return target_fetch

        # Check DB capabilities
        conn = self.conn_manager.get(source_tag)
        caps = self.conn_manager.get_capabilities(conn.db_type)
        if not caps.get("supports_parallel_read", True):
            return min(default_fetch, 5000)

        return default_fetch

    def calculate_partitions(
        self,
        estimated_size_mb: float,
        target_partition_size_mb: float = 128.0,
        default_partitions: int = 5
    ) -> int:
        """
        Calculate optimal Spark partition count dynamically based on target partition sizing.
        Formula: max(1, min(estimated_size_mb / 128MB, 200))
        """
        if estimated_size_mb <= 0:
            return default_partitions

        computed = int(estimated_size_mb / target_partition_size_mb)
        parts = max(1, min(computed, 200))
        return parts

    def calculate_parallelism(
        self,
        source_tag: str,
        spark_executor_slots: int = 10,
        airflow_pool_slots: int = 10
    ) -> int:
        """Calculate dynamic table parallelism pool bounds."""
        return self.conn_manager.get_dynamic_concurrency(
            source_tag=source_tag,
            spark_executor_slots=spark_executor_slots,
            airflow_pool_slots=airflow_pool_slots
        )

    def optimize_table_execution(
        self,
        meta: MetadataRow,
        has_lob: bool = False,
        lob_count: int = 0,
        to_char_projected: bool = False,
        spark_executor_slots: int = 10,
        airflow_pool_slots: int = 10
    ) -> OptimizationPlan:
        """
        Consolidate metadata facts, capabilities, and historical statistics to produce
        a complete execution OptimizationPlan.
        """
        stats = self.fetch_historical_stats(meta.source_tag, meta.source_table)
        est_mb = stats["average_size_mb"]
        est_rows = stats["average_row_count"]

        fetch_sz = self.calculate_fetch_size(
            source_tag=meta.source_tag,
            table_name=meta.source_table,
            has_lob=has_lob,
            lob_count=lob_count,
            to_char_projected=to_char_projected,
            default_fetch=meta.chunk_size or 10000
        )

        num_parts = self.calculate_partitions(
            estimated_size_mb=est_mb,
            default_partitions=meta.num_partitions or 5
        )

        concurrency = self.calculate_parallelism(
            source_tag=meta.source_tag,
            spark_executor_slots=spark_executor_slots,
            airflow_pool_slots=airflow_pool_slots
        )

        # Classify Resource Tier and build Explanatory Reason & Structured Decisions
        reasons = []
        decisions_list = []
        if to_char_projected:
            reasons.append(f"{lob_count} LOB column(s) converted via TO_CHAR(): fetch_size increased to {fetch_sz}")
            decisions_list.append({"rule": "TO_CHAR_ENGINE_CAST", "action": f"FETCHSIZE={fetch_sz}"})
        elif has_lob or lob_count > 0:
            if lob_count > 10:
                rule_name = "LOB_COUNT_GT_10"
            elif lob_count >= 6:
                rule_name = "LOB_COUNT_6_TO_10"
            elif lob_count >= 3:
                rule_name = "LOB_COUNT_3_TO_5"
            elif lob_count >= 1:
                rule_name = "LOB_COUNT_1_TO_2"
            else:
                rule_name = "LOB_DETECTED"

            reasons.append(f"{lob_count if lob_count > 0 else 'LOB'} column(s) detected: fetch_size reduced to {fetch_sz} to protect JVM heap")
            decisions_list.append({"rule": rule_name, "action": f"FETCHSIZE={fetch_sz}"})
        else:
            decisions_list.append({"rule": "STANDARD_SCHEMA", "action": f"FETCHSIZE={fetch_sz}"})

        if est_mb > 0:
            reasons.append(f"Historical dataset size: {est_mb:.2f} MB ({num_parts} partitions allocated)")
            decisions_list.append({"rule": "HISTORICAL_STATS_PRESENT", "action": f"NUM_PARTITIONS={num_parts}"})
        else:
            reasons.append("No historical stats: using default partition bounds")
            decisions_list.append({"rule": "NO_HISTORICAL_STATS", "action": f"DEFAULT_PARTITIONS={num_parts}"})
        
        reason_str = "; ".join(reasons)

        if est_mb > 500 or est_rows > 1000000:
            tier = "LARGE"
        elif est_mb > 50 or est_rows > 100000:
            tier = "MEDIUM"
        else:
            tier = "SMALL"

        log.info(
            f"\n[Runtime Execution Plan v1 — '{meta.source_table}']\n"
            f"  Fetch Size         : {fetch_sz} (LOB Throttled: {has_lob})\n"
            f"  Partition Count    : {num_parts} (Est Size: {est_mb:.2f} MB)\n"
            f"  Parallel Pool      : {concurrency}\n"
            f"  Resource Tier      : {tier}\n"
            f"  Reason             : {reason_str}"
        )

        return ExecutionPlan(
            source_tag=meta.source_tag,
            source_table=meta.source_table,
            fetch_size=fetch_sz,
            num_partitions=num_parts,
            parallel_concurrency=concurrency,
            resource_tier=tier,
            is_lob_throttled=has_lob,
            estimated_size_mb=est_mb,
            estimated_rows=est_rows,
            optimizer_version="1.0.0",
            reason=reason_str,
            decisions=decisions_list
        )
