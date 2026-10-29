-- migrations/020_execution_state_model.sql
-- Adds Execution State Model columns to metadata.batch_log for phase-level operational visibility.

ALTER TABLE metadata.batch_log
    ADD COLUMN IF NOT EXISTS processed_count      BIGINT,
    ADD COLUMN IF NOT EXISTS progress_pct         NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS current_phase        VARCHAR(50) DEFAULT 'PENDING',
    ADD COLUMN IF NOT EXISTS last_progress_update TIMESTAMP,
    ADD COLUMN IF NOT EXISTS rows_per_second      NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS estimated_completion TIMESTAMP;
