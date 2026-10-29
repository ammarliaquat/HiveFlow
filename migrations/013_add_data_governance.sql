-- migrations/013_add_data_governance.sql

-- 1. Add governance columns to db_columns_etl
ALTER TABLE metadata.db_columns_etl 
ADD COLUMN IF NOT EXISTS classification_level VARCHAR(50) DEFAULT 'PUBLIC',
ADD COLUMN IF NOT EXISTS log_masking CHAR(1) DEFAULT 'N',
ADD COLUMN IF NOT EXISTS display_masking CHAR(1) DEFAULT 'N',
ADD COLUMN IF NOT EXISTS export_masking CHAR(1) DEFAULT 'N';

-- 2. Create column_classification_history table
CREATE TABLE IF NOT EXISTS metadata.column_classification_history (
    history_id SERIAL PRIMARY KEY,
    column_id INTEGER REFERENCES metadata.db_columns_etl(column_id),
    old_classification_level VARCHAR(50),
    new_classification_level VARCHAR(50),
    old_log_masking CHAR(1),
    new_log_masking CHAR(1),
    old_display_masking CHAR(1),
    new_display_masking CHAR(1),
    old_export_masking CHAR(1),
    new_export_masking CHAR(1),
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason TEXT
);

-- 3. Create a trigger to automatically audit changes
CREATE OR REPLACE FUNCTION metadata.audit_column_classification_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if any of the governance fields changed
    IF (NEW.classification_level IS DISTINCT FROM OLD.classification_level) OR
       (NEW.log_masking IS DISTINCT FROM OLD.log_masking) OR
       (NEW.display_masking IS DISTINCT FROM OLD.display_masking) OR
       (NEW.export_masking IS DISTINCT FROM OLD.export_masking) THEN
        
        INSERT INTO metadata.column_classification_history (
            column_id, 
            old_classification_level, new_classification_level,
            old_log_masking, new_log_masking,
            old_display_masking, new_display_masking,
            old_export_masking, new_export_masking,
            changed_by, reason
        ) VALUES (
            NEW.column_id,
            OLD.classification_level, NEW.classification_level,
            OLD.log_masking, NEW.log_masking,
            OLD.display_masking, NEW.display_masking,
            OLD.export_masking, NEW.export_masking,
            'system', -- default unless updated explicitly before
            'Automatic audit trigger'
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_column_classification ON metadata.db_columns_etl;
CREATE TRIGGER trg_audit_column_classification
AFTER UPDATE ON metadata.db_columns_etl
FOR EACH ROW
EXECUTE FUNCTION metadata.audit_column_classification_changes();
