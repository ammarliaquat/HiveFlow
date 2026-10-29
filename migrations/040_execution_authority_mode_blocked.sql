-- Migration 040: Execution Authority BLOCKED Safe Default
-- This migration acts as an intentional safety freeze, converting all existing LEGACY rows to BLOCKED.
-- Explicit reauthorization is required afterward for legacy processing.

UPDATE metadata.db_tables_etl
SET execution_authority_mode = 'BLOCKED'
WHERE execution_authority_mode = 'LEGACY';

ALTER TABLE metadata.db_tables_etl
ALTER COLUMN execution_authority_mode SET DEFAULT 'BLOCKED';

ALTER TABLE metadata.db_tables_etl
ADD CONSTRAINT chk_execution_authority_mode
CHECK (execution_authority_mode IN ('GOVERNED', 'LEGACY', 'BLOCKED'));
