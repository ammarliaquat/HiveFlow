-- 009_dynamic_connections.sql
-- Phase A Enterprise Migration: Database-driven connection management (`metadata.connections`)
-- and Git-style JSON snapshot audit history (`metadata.connection_history`).
-- Keeps [RepositoryDB] in config/databases.ini as the immutable bootstrap key.

DROP TABLE IF EXISTS metadata.connection_history;
DROP TABLE IF EXISTS metadata.connections;

CREATE TABLE IF NOT EXISTS metadata.connections (
    tag VARCHAR(100) PRIMARY KEY,                                      -- e.g., 'msopforg_opforg', 'land_n'
    connection_type VARCHAR(50) NOT NULL DEFAULT 'DATABASE',           -- 'DATABASE', 'FILE', 'API', etc.
    supports_source BOOLEAN NOT NULL DEFAULT true,
    supports_target BOOLEAN NOT NULL DEFAULT false,
    environment VARCHAR(20) NOT NULL DEFAULT 'PROD' CHECK (environment IN ('DEV', 'TEST', 'UAT', 'PROD')),
    db_type VARCHAR(50) NOT NULL,                                      -- 'postgres', 'mysql', 'oracle', 'mssql'
    server VARCHAR(200) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(200) NOT NULL,
    schema_name VARCHAR(200),
    username VARCHAR(100) NOT NULL,
    credential_reference VARCHAR(255) NOT NULL,                        -- e.g. '${MSOPFORG_DB_PASSWORD}' or 'env://POSTGRES_PASSWORD'
    service_name VARCHAR(100),                                         -- For Oracle DSN or service string
    connection_options JSONB DEFAULT '{}'::jsonb,                      -- e.g. {"sslmode": "require", "fetchSize": 10000}
    
    -- Health & Pre-Save Test Metadata
    last_tested_at TIMESTAMP,
    last_test_status VARCHAR(20) DEFAULT 'UNTESTED',                   -- 'SUCCESS', 'FAILED', 'UNTESTED'
    last_error TEXT,
    last_latency_ms INTEGER,
    
    -- Governance & Ownership
    owner VARCHAR(100) DEFAULT 'Admin',
    team VARCHAR(100) DEFAULT 'Data Engineering',
    purpose TEXT,
    max_parallel_tables INTEGER DEFAULT 5,

    
    -- Lifecycle & Soft Deletes
    is_active CHAR(1) DEFAULT 'y' CHECK (is_active IN ('y', 'n')),
    deleted_at TIMESTAMP,
    deleted_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on active connections by role support for fast startup queries
CREATE INDEX IF NOT EXISTS idx_metadata_connections_active ON metadata.connections(supports_source, supports_target, is_active);

-- Git-Style JSON Snapshot Audit History Table
CREATE TABLE IF NOT EXISTS metadata.connection_history (
    history_id SERIAL PRIMARY KEY,
    tag VARCHAR(100) NOT NULL,
    change_type VARCHAR(20) NOT NULL,                                  -- 'CREATE', 'UPDATE', 'SOFT_DELETE'
    old_value JSONB,                                                   -- Null on CREATE
    new_value JSONB,                                                   -- Null on SOFT_DELETE
    changed_by VARCHAR(100) NOT NULL,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metadata_connection_history_tag ON metadata.connection_history(tag, changed_at DESC);
