import logging
import uuid
from typing import Optional, Dict, Any
import os

from src.connections.connection_manager import ConnectionManager
from src.orchestrator.claim_manager import ScopeClaimManager
from src.contracts.claim_models import ExecutionFence
from src.models.principal import AuthenticatedPrincipal
from src.metadata.key_contract import ResolvedTableContract
from src.load.gold_atomic_publisher import GoldAtomicPublisher, GoldPublicationResult
from src.contracts.runtime_settings import RuntimeSettings

log = logging.getLogger(__name__)

class RecoveryDispatcher:
    """
    Internal recovery dispatcher service.
    Acts as the single authorized system-level caller of acquire_recovery_claim().
    """

    def __init__(self, conn_manager: ConnectionManager, runtime_settings: RuntimeSettings):
        self._conn_manager = conn_manager
        self._runtime_settings = runtime_settings
        self._claim_manager = ScopeClaimManager(conn_manager)
        self._publisher = GoldAtomicPublisher(conn_manager, runtime_settings)
        
        # System-derived principal for recovery operations
        self._system_principal = AuthenticatedPrincipal(
            user_id="system_recovery",
            username="System Recovery",
            roles=["admin"],
            actor_type="SYSTEM",
            project_context=None
        )

    def recover_publication(
        self,
        publication_id: uuid.UUID
    ) -> GoldPublicationResult:
        """
        Executes a dedicated recovery work item for a stalled publication.
        1. Acquires a new recovery claim (fencing out stalled workers).
        2. Reconstructs contract from metadata.
        3. Invokes GoldAtomicPublisher.recover_publication_attempt with the acquired fence.
        """
        owner_id = f"recovery_dispatcher_{os.getpid()}"
        log.info(f"RecoveryDispatcher: Acquiring recovery claim for publication_id={publication_id}")
        
        try:
            # 1. Acquire recovery claim under row lock, deriving identity directly from metadata
            recovery_fence = self._claim_manager.acquire_recovery_claim(
                publication_id=str(publication_id),
                owner_instance_id=owner_id,
                runtime_settings=self._runtime_settings,
            )
            
            # 2. Derive contract from metadata
            from src.contracts.claim_models import FrozenJobExecution
            import json
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT parameters 
                        FROM metadata.gui_job_registry
                        WHERE run_id = %s
                    """, [recovery_fence.run_id])
                    row = cur.fetchone()
                    if not row:
                        raise Exception(f"Cannot find gui_job_registry for run_id={recovery_fence.run_id}")
                    params_dict = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    frozen_job = FrozenJobExecution.from_dict(params_dict)
                    
            frozen_table = next((t for t in frozen_job.tables if t.table_id == recovery_fence.table_id), None)
            if not frozen_table:
                raise Exception(f"Cannot find table_id={recovery_fence.table_id} in frozen contract.")
            
            from src.metadata.key_contract import MetadataRepository
            meta_repo = MetadataRepository(self._conn_manager)
            cur_contract = meta_repo.get_table_contract(frozen_table.source_tag, frozen_table.source_table)
            if not cur_contract:
                raise Exception(f"Cannot resolve contract for {frozen_table.source_tag}.{frozen_table.source_table}")
                
            # 3. Execute publisher recovery
            log.info(f"RecoveryDispatcher: Dispatching publisher recovery for publication {publication_id}")
            result = self._publisher.recover_publication_attempt(
                publication_id=publication_id,
                recovery_fence=recovery_fence,
                contract=cur_contract,
                target_tag=frozen_table.target_tag
            )
            
            log.info(f"RecoveryDispatcher: Recovery successful for publication {publication_id}")
            return result
            
        except Exception as e:
            log.error(f"RecoveryDispatcher: Failed to recover publication {publication_id}: {e}")
            raise