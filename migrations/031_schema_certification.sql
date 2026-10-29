-- ============================================================
-- FILE: migrations/031_schema_certification.sql
-- PURPOSE: SCHEMA-CERT-01: Establish Migration Ledger for Certification
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.schema_migrations (
    version VARCHAR(10) PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
