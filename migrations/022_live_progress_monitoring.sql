-- Migration 022: Live Accurate Progress & Telemetry Monitoring
-- Adds extracted_count, last_heartbeat, chunk metrics, rolling_rows_per_second,
-- reconciliation status, and attempt_number columns to metadata.batch_log and metadata.batch_summary.

ALTER TABLE metadata.batch_log
    ADD COLUMN IF NOT EXISTS extracted_count            BIGINT          DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS last_heartbeat             TIMESTAMP       DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS current_chunk              INTEGER         DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS total_chunks               INTEGER         DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS chunks_completed           INTEGER         DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS rolling_rows_per_second    NUMERIC(12,2)   DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS reconciliation_status     VARCHAR(50)     DEFAULT 'UNOBSERVED',
    ADD COLUMN IF NOT EXISTS reconciliation_difference BIGINT          DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS attempt_number             INTEGER         DEFAULT 1;

ALTER TABLE metadata.batch_summary
    ADD COLUMN IF NOT EXISTS extracted_count            BIGINT          DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS reconciliation_status     VARCHAR(50)     DEFAULT 'UNOBSERVED',
    ADD COLUMN IF NOT EXISTS rolling_rows_per_second    NUMERIC(12,2)   DEFAULT NULL;

COMMENT ON COLUMN metadata.batch_log.extracted_count IS 'Total rows successfully extracted/read from source system.';
COMMENT ON COLUMN metadata.batch_log.processed_count IS 'Total rows successfully persisted/committed to active target stage.';
COMMENT ON COLUMN metadata.batch_log.last_heartbeat IS 'Last timestamp worker process sent liveness ping.';
COMMENT ON COLUMN metadata.batch_log.rolling_rows_per_second IS 'Rolling EWMA committed write throughput (rows/sec).';
