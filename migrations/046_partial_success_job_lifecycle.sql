-- migrations/046_partial_success_job_lifecycle.sql
-- Description: Record governed multi-table runs that completed with a mix of
-- successful and failed table attempts without misreporting the parent run.

BEGIN;

-- Keep the registry vocabulary aligned with the lifecycle state machine.
-- PARTIAL_SUCCESS is terminal: all selected tables were attempted, at least
-- one completed, and one or more retain their own batch_log failure evidence.
ALTER TABLE metadata.gui_job_registry
    DROP CONSTRAINT IF EXISTS chk_job_status;

ALTER TABLE metadata.gui_job_registry
    ADD CONSTRAINT chk_job_status CHECK (
        status IN (
            'QUEUED',
            'DISPATCHED',
            'RUNNING',
            'SUCCESS',
            'PARTIAL_SUCCESS',
            'FAILED',
            'RECONCILIATION_FAILED',
            'CANCELLED',
            'ABANDONED',
            'RECOVERY_REQUIRED'
        )
    );

COMMIT;
