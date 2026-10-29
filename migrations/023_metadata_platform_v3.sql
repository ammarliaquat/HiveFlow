-- ==============================================================================
-- MIGRATION 023: METADATA PLATFORM V3.0 ARCHITECTURE
-- ==============================================================================

-- 1. Refactor metadata.connections (Infrastructure Facts)
ALTER TABLE metadata.connections 
  DROP COLUMN IF EXISTS max_parallel_tables,
  ADD COLUMN IF NOT EXISTS max_concurrent_connections INT DEFAULT 10,
  ADD COLUMN IF NOT EXISTS connection_timeout INT DEFAULT 30,
  ADD COLUMN IF NOT EXISTS read_timeout INT DEFAULT 300,
  ADD COLUMN IF NOT EXISTS supports_parallel_read BOOLEAN DEFAULT TRUE;

-- 2. Create metadata.pipeline_jobs (Airflow Orchestration Policy)
CREATE TABLE IF NOT EXISTS metadata.pipeline_jobs (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(150) NOT NULL,
    source_tag VARCHAR(100) NOT NULL,
    table_name VARCHAR(150) NOT NULL,
    schedule VARCHAR(100) DEFAULT '0 0 * * *',
    priority INT DEFAULT 1,
    owner VARCHAR(100) DEFAULT 'data_team',
    sla_seconds INT DEFAULT 3600,
    pool VARCHAR(100) DEFAULT 'default_pool',
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_pipeline_job UNIQUE (source_tag, table_name)
);

-- 3. Create metadata.system_capabilities (Connector Capability Matrix)
CREATE TABLE IF NOT EXISTS metadata.system_capabilities (
    db_type VARCHAR(50) PRIMARY KEY,
    supports_parallel_read BOOLEAN DEFAULT TRUE,
    supports_incremental BOOLEAN DEFAULT TRUE,
    supports_partitioning BOOLEAN DEFAULT TRUE,
    supports_merge BOOLEAN DEFAULT FALSE,
    supports_bulk_load BOOLEAN DEFAULT FALSE,
    supports_snapshot BOOLEAN DEFAULT TRUE,
    supports_cdc BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial connector capabilities
INSERT INTO metadata.system_capabilities (db_type, supports_parallel_read, supports_incremental, supports_partitioning, supports_merge, supports_bulk_load, supports_snapshot, supports_cdc)
VALUES 
    ('oracle', true, true, true, false, false, true, false),
    ('mysql', true, true, false, false, false, true, false),
    ('postgres', true, true, true, true, true, true, true)
ON CONFLICT (db_type) DO UPDATE SET
    supports_parallel_read = EXCLUDED.supports_parallel_read,
    supports_incremental = EXCLUDED.supports_incremental,
    supports_partitioning = EXCLUDED.supports_partitioning,
    supports_merge = EXCLUDED.supports_merge,
    supports_bulk_load = EXCLUDED.supports_bulk_load,
    supports_snapshot = EXCLUDED.supports_snapshot,
    supports_cdc = EXCLUDED.supports_cdc;

-- 4. Create metadata.performance_profile (Observed Telemetry Facts)
CREATE TABLE IF NOT EXISTS metadata.performance_profile (
    profile_id SERIAL PRIMARY KEY,
    source_tag VARCHAR(100) NOT NULL,
    source_table VARCHAR(150) NOT NULL,
    average_row_count BIGINT DEFAULT 0,
    average_size_mb NUMERIC(10,2) DEFAULT 0.00,
    average_duration_sec NUMERIC(10,2) DEFAULT 0.00,
    average_rows_per_second NUMERIC(12,2) DEFAULT 0.00,
    average_extract_speed_rps NUMERIC(12,2) DEFAULT 0.00,
    average_merge_speed_rps NUMERIC(12,2) DEFAULT 0.00,
    compression_ratio NUMERIC(5,2) DEFAULT 1.00,
    p95_duration_sec NUMERIC(10,2) DEFAULT 0.00,
    last_successful_run TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_perf_profile UNIQUE (source_tag, source_table)
);

-- 5. Bump system settings schema_version to 3.0.0
INSERT INTO metadata.system_settings (setting_key, setting_value, description)
VALUES ('schema_version', '3.0.0', 'Metadata Platform Architecture Version')
ON CONFLICT (setting_key) DO UPDATE 
SET setting_value = '3.0.0', updated_at = NOW();
