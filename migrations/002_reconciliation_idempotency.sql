-- Reconciliation controls
ALTER TABLE metadata.db_tables_etl ADD COLUMN IF NOT EXISTS is_reconcile CHAR(1) DEFAULT 'n';
ALTER TABLE metadata.db_tables_etl ADD COLUMN IF NOT EXISTS reconcile_threshold NUMERIC(5,2) DEFAULT 0.00;
ALTER TABLE metadata.db_tables_etl ADD COLUMN IF NOT EXISTS reconcile_action VARCHAR(20) DEFAULT 'FAIL';

-- Rerun & Idempotency controls
ALTER TABLE metadata.db_tables_etl ADD COLUMN IF NOT EXISTS rerun_flag CHAR(1) DEFAULT 'n';
ALTER TABLE metadata.db_tables_etl ADD COLUMN IF NOT EXISTS last_success_date DATE;

-- Audit timestamps (should already exist, but IF NOT EXISTS keeps it safe)
ALTER TABLE metadata.db_tables_etl ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE metadata.db_tables_etl ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Optional: Create an index for the skip logic query
CREATE INDEX IF NOT EXISTS idx_etl_skip_logic ON metadata.db_tables_etl (is_active, rerun_flag, last_success_date);

-- Update batch_log status column to VARCHAR(50) for longer statuses (e.g. RECONCILIATION_FAILED)
ALTER TABLE metadata.batch_log ALTER COLUMN status TYPE VARCHAR(50);
