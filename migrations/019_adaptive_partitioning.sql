-- migrations/019_adaptive_partitioning.sql
-- Adds adaptive partitioning parameters for dynamic row-count driven Spark JDBC partitioning.

ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS target_rows_per_partition   INTEGER DEFAULT 250000,
    ADD COLUMN IF NOT EXISTS minimum_partition_threshold INTEGER DEFAULT 50000,
    ADD COLUMN IF NOT EXISTS maximum_partitions          INTEGER DEFAULT 32;

-- Backfill defaults for existing active tables
UPDATE metadata.db_tables_etl
SET target_rows_per_partition   = COALESCE(target_rows_per_partition, 250000),
    minimum_partition_threshold = COALESCE(minimum_partition_threshold, 50000),
    maximum_partitions          = COALESCE(maximum_partitions, 32)
WHERE target_rows_per_partition IS NULL
   OR minimum_partition_threshold IS NULL
   OR maximum_partitions IS NULL;
