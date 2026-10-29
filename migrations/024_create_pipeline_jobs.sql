-- ==============================================================================
-- MIGRATION 024: CREATE METADATA.PIPELINE_JOBS (AIRFLOW ORCHESTRATION POLICY)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS metadata.pipeline_jobs (
    job_id SERIAL PRIMARY KEY,
    source_tag VARCHAR(100) NOT NULL,
    table_name VARCHAR(150) NOT NULL,
    execution_mode VARCHAR(50) DEFAULT 'FULL', -- FULL, INCREMENTAL, CDC
    schedule VARCHAR(100) DEFAULT '0 0 * * *',
    priority INT DEFAULT 1,
    owner VARCHAR(100) DEFAULT 'data_team',
    sla_seconds INT DEFAULT 3600,
    pool VARCHAR(100) DEFAULT 'default_pool',
    depends_on JSONB DEFAULT '[]'::jsonb,
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_pipeline_job UNIQUE (source_tag, table_name)
);

ALTER TABLE metadata.pipeline_jobs ALTER COLUMN job_name DROP NOT NULL;
ALTER TABLE metadata.pipeline_jobs ADD COLUMN IF NOT EXISTS execution_mode VARCHAR(50) DEFAULT 'FULL';
ALTER TABLE metadata.pipeline_jobs ADD COLUMN IF NOT EXISTS depends_on JSONB DEFAULT '[]'::jsonb;

-- Seed initial jobs from db_tables_etl for active tables
INSERT INTO metadata.pipeline_jobs (source_tag, table_name, execution_mode, is_enabled)
SELECT source_tag, source_table, 'FULL', (is_active = 'y')
FROM metadata.db_tables_etl
ON CONFLICT (source_tag, table_name) DO NOTHING;
