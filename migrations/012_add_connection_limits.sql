-- migrations/012_add_connection_limits.sql

-- Add max_parallel_tables to connections table to govern Python thread concurrency
ALTER TABLE metadata.connections 
ADD COLUMN IF NOT EXISTS max_parallel_tables INTEGER DEFAULT 5;

-- Optional: update specific connections if needed
-- UPDATE metadata.connections SET max_parallel_tables = 3 WHERE tag = 'oracle_hr';
