-- migrations/010_system_settings.sql
CREATE TABLE IF NOT EXISTS metadata.system_settings (
    setting_key VARCHAR(100) PRIMARY KEY,
    setting_value VARCHAR(500) NOT NULL,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO metadata.system_settings (setting_key, setting_value, description)
VALUES ('schema_version', '1.0.0', 'Current version of the metadata schema')
ON CONFLICT (setting_key) DO UPDATE SET 
    setting_value = EXCLUDED.setting_value,
    description = EXCLUDED.description,
    updated_at = CURRENT_TIMESTAMP;
