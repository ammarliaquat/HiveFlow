-- migrations/017_data_lineage.sql

CREATE TABLE IF NOT EXISTS metadata.data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    source_table_id INTEGER REFERENCES metadata.db_tables_etl(table_id),
    target_table_id INTEGER REFERENCES metadata.db_tables_etl(table_id),
    transformation_type VARCHAR(50) DEFAULT 'DIRECT_COPY', -- DIRECT_COPY, JOIN, AGGREGATION, CUSTOM
    relationship_strength VARCHAR(20) DEFAULT 'MANDATORY', -- MANDATORY, OPTIONAL
    transformation_logic TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_lineage_edge UNIQUE(source_table_id, target_table_id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_source ON metadata.data_lineage(source_table_id);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON metadata.data_lineage(target_table_id);
