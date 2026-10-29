-- migrations/018_canonical_types_and_strategies.sql
-- Adds vendor-neutral canonical type contract fields and metadata-driven execution parameters.

-- 1. Extend db_columns_etl with canonical type and constraint contracts
ALTER TABLE metadata.db_columns_etl
    ADD COLUMN IF NOT EXISTS canonical_type     VARCHAR(20),
    ADD COLUMN IF NOT EXISTS target_native_type VARCHAR(100),
    ADD COLUMN IF NOT EXISTS is_nullable        CHAR(1) DEFAULT 'Y',
    ADD COLUMN IF NOT EXISTS max_length         INTEGER;

-- Backfill canonical_type from source_column_datatype
UPDATE metadata.db_columns_etl
SET canonical_type = CASE LOWER(COALESCE(source_column_datatype, ''))
    WHEN 'clob'         THEN 'TEXT'
    WHEN 'nclob'        THEN 'TEXT'
    WHEN 'blob'         THEN 'BINARY'
    WHEN 'raw'          THEN 'BINARY'
    WHEN 'long raw'     THEN 'BINARY'
    WHEN 'varchar2'     THEN 'STRING'
    WHEN 'nvarchar2'    THEN 'STRING'
    WHEN 'char'         THEN 'STRING'
    WHEN 'nchar'        THEN 'STRING'
    WHEN 'number'       THEN 'DECIMAL'
    WHEN 'integer'      THEN 'INTEGER'
    WHEN 'date'         THEN 'TIMESTAMP'
    WHEN 'timestamp'    THEN 'TIMESTAMP'
    WHEN 'timestamp(6)' THEN 'TIMESTAMP'
    WHEN 'text'         THEN 'TEXT'
    WHEN 'varchar'      THEN 'STRING'
    WHEN 'int'          THEN 'INTEGER'
    WHEN 'bigint'       THEN 'LONG'
    ELSE 'STRING'
END
WHERE canonical_type IS NULL;

-- 2. Extend db_tables_etl with landing strategy, evolution policy, and pre-defined partition bounds
ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS landing_mode             VARCHAR(20) DEFAULT 'METADATA',
    ADD COLUMN IF NOT EXISTS schema_evolution_policy  VARCHAR(20),
    ADD COLUMN IF NOT EXISTS partition_lower_bound    BIGINT,
    ADD COLUMN IF NOT EXISTS partition_upper_bound    BIGINT;

-- Layer-specific evolution policy defaults (Bronze=AUTO, Silver=WARN, Gold=STRICT)
UPDATE metadata.db_tables_etl
SET schema_evolution_policy = CASE LOWER(COALESCE(table_type, 'gold'))
    WHEN 'bronze' THEN 'AUTO'
    WHEN 'silver' THEN 'WARN'
    WHEN 'gold'   THEN 'STRICT'
    ELSE 'PERMISSIVE'
END
WHERE schema_evolution_policy IS NULL;

-- Backfill pre-defined partition bounds for large Oracle source tables to avoid live MIN/MAX JDBC roundtrips
UPDATE metadata.db_tables_etl SET partition_lower_bound = 20000, partition_upper_bound = 360000
WHERE source_table = 'tblop_jannov161225';

UPDATE metadata.db_tables_etl SET partition_lower_bound = 20000, partition_upper_bound = 550000
WHERE source_table = 'tblop_jandec25100626';

UPDATE metadata.db_tables_etl SET partition_lower_bound = 500, partition_upper_bound = 140000
WHERE source_table = 'tblop';
