-- migrations/021_add_stage_metrics_column.sql
-- Add JSONB stage_metrics column to metadata.batch_log for simplified single-row stage telemetry.

ALTER TABLE metadata.batch_log ADD COLUMN IF NOT EXISTS stage_metrics JSONB;
