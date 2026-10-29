-- migrations/021_batch_stage_log_schema.sql
-- Dedicated stage timing and telemetry table for queryable execution analytics.

CREATE TABLE IF NOT EXISTS metadata.batch_stage_log (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    source_tag VARCHAR(100) NOT NULL,
    source_table VARCHAR(150) NOT NULL,
    layer VARCHAR(50) NOT NULL,
    stage_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    elapsed_seconds NUMERIC(10, 3) NOT NULL,
    row_count BIGINT,
    rows_per_second NUMERIC(12, 2),
    memory_mb NUMERIC(10, 2),
    has_spill BOOLEAN DEFAULT FALSE,
    details TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bsl_run_id ON metadata.batch_stage_log(run_id);
CREATE INDEX IF NOT EXISTS idx_bsl_stage_name ON metadata.batch_stage_log(stage_name, source_tag);
CREATE INDEX IF NOT EXISTS idx_bsl_created_at ON metadata.batch_stage_log(created_at DESC);
