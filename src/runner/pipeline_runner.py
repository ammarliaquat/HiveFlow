"""Compatibility boundary for the retired mutable ``PipelineRunner`` path.

Actual execution is owned by ``ETL.run_frozen_table``. The historical runner
only compiled a plan and then reported success, so it must never be used as a
pipeline execution authority.
"""

from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import List, Optional
import uuid

from src.connections.connection_manager import ConnectionManager
from src.contracts.claim_models import FrozenTableEntry


log = logging.getLogger("PipelineRunner")


@dataclass
class PipelineRunResult:
    """Compatibility result returned by the retired PipelineRunner interface."""

    run_id: str
    source_tag: str
    source_table: str
    status: str  # FAILED for the retired runner
    rows_read: int = 0
    rows_written: int = 0
    duration_sec: float = 0.0
    execution_plan: Optional[object] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class PipelineRunner:
    """Retired mutable execution interface.

    Kept only so stale callers fail with a classified error instead of
    manufacturing metadata, planning without execution, and reporting success.
    """

    def __init__(
        self,
        conn_manager: Optional[ConnectionManager] = None,
        config_path: Optional[str] = None,
    ):
        # Preserve the public constructor while callers migrate. This boundary
        # must not resolve mutable metadata or create execution defaults.
        self.conn_manager = conn_manager
        self.config_path = config_path

    def run_frozen(
        self, frozen_table: FrozenTableEntry, run_id: str
    ) -> PipelineRunResult:
        """Reject planning-only governed execution before any data operation."""
        result = PipelineRunResult(
            run_id=run_id,
            source_tag=frozen_table.source_tag,
            source_table=frozen_table.source_table,
            status="FAILED",
        )
        result.errors.append(
            "GOVERNED_EXECUTION_NOT_IMPLEMENTED: PipelineRunner.run_frozen is "
            "planning-only and must not be used for governed execution. "
            "Use ETL.run_frozen_table()."
        )
        log.error(result.errors[0])
        return result

    def run(
        self,
        source_tag: str,
        table_name: str,
        run_id: Optional[str] = None,
        governed: bool = False,
    ) -> PipelineRunResult:
        """Reject the retired mutable path before it can claim a false success."""
        start_time = datetime.now()
        result = PipelineRunResult(
            run_id=run_id or str(uuid.uuid4()),
            source_tag=source_tag,
            source_table=table_name,
            status="FAILED",
        )
        result.duration_sec = round((datetime.now() - start_time).total_seconds(), 2)
        result.errors.append(
            "LEGACY_PIPELINE_RUNNER_RETIRED: mutable PipelineRunner.run() does not "
            "execute Bronze/Silver/Gold. Submit a frozen governed run through "
            "ExecutionService and execute it with ETL.run_frozen_table()."
        )
        log.error(result.errors[0])
        return result
