-- Alter last_success_date in db_tables_etl to TIMESTAMP
ALTER TABLE metadata.db_tables_etl ALTER COLUMN last_success_date TYPE TIMESTAMP USING last_success_date::timestamp;
