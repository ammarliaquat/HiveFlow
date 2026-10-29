-- migrations/039_execution_authority_mode.sql

ALTER TABLE metadata.db_tables_etl
ADD COLUMN execution_authority_mode VARCHAR(20) NOT NULL DEFAULT 'LEGACY';

COMMENT ON COLUMN metadata.db_tables_etl.execution_authority_mode IS 'Determines which execution regime owns this table: GOVERNED (PipelineRunner) or LEGACY (load_with_db_link). A governed table can never become legacy merely because its policy metadata is missing or damaged.';
