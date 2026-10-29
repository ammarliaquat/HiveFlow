-- migrations/022_benchmark_history_schema.sql
-- Table for persisting historical benchmark performance trends across pipeline runs.

CREATE TABLE IF NOT EXISTS metadata.benchmark_history (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    source_tag VARCHAR(100) NOT NULL,
    source_table VARCHAR(150) NOT NULL,
    oracle_read_sec NUMERIC(10, 2),
    bronze_write_sec NUMERIC(10, 2),
    silver_write_sec NUMERIC(10, 2),
    gold_write_sec NUMERIC(10, 2),
    total_sec NUMERIC(10, 2),
    total_rows BIGINT,
    rows_per_second NUMERIC(12, 2),
    spark_version VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bh_table_created ON metadata.benchmark_history(source_tag, source_table, created_at DESC);
