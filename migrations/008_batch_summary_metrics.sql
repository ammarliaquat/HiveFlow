-- Migration 008: Add priority, action, run_date, elapsed_seconds, and rows_per_second columns to metadata.batch_summary
-- Creates a composite index idx_bs_routing_ts to optimize reporting query patterns.

BEGIN;

-- Add new reporting and optimization columns
ALTER TABLE metadata.batch_summary
    ADD COLUMN IF NOT EXISTS priority         INTEGER,
    ADD COLUMN IF NOT EXISTS action           VARCHAR(50),
    ADD COLUMN IF NOT EXISTS run_date         DATE,
    ADD COLUMN IF NOT EXISTS elapsed_seconds   NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS rows_per_second   NUMERIC(12,2);

-- Add column comments for Power BI metadata integration
COMMENT ON COLUMN metadata.batch_summary.priority IS 'Capacity/scheduling tier of the run (1=High, 2=Medium, 3=Low).';
COMMENT ON COLUMN metadata.batch_summary.action IS 'Pipeline entry action name (e.g., run_pipeline, run_landing).';
COMMENT ON COLUMN metadata.batch_summary.run_date IS 'Calendar dimension extracted from start_ts (date level granularity).';
COMMENT ON COLUMN metadata.batch_summary.elapsed_seconds IS 'Total duration in seconds including retry durations.';
COMMENT ON COLUMN metadata.batch_summary.rows_per_second IS 'Throughput rate metric (total_rows / elapsed_seconds).';

-- Create optimization index for filtering and sorting
CREATE INDEX IF NOT EXISTS idx_bs_routing_ts ON metadata.batch_summary(source_tag, target_tag, layer, start_ts DESC);

COMMIT;
