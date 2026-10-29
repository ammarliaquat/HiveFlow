-- migrations/014_add_data_quality.sql

-- 1. Rules Definition Table
CREATE TABLE IF NOT EXISTS metadata.data_quality_rules (
    rule_id SERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES metadata.db_tables_etl(table_id),
    column_name VARCHAR(100),
    rule_type VARCHAR(50) NOT NULL, -- NOT_NULL, UNIQUE, ACCEPTED_VALUES, REGEX, ROW_COUNT, RECON_COUNT, RECON_SUM, SQL_EXPRESSION
    rule_expression TEXT,
    expected_value VARCHAR(255),
    tolerance VARCHAR(50), -- e.g., '5%'
    execution_layer VARCHAR(20) DEFAULT 'SILVER', -- BRONZE, SILVER, GOLD
    failure_action VARCHAR(50) DEFAULT 'CONTINUE', -- CONTINUE, WARN, SKIP_TABLE, FAIL_SOURCE, FAIL_PIPELINE
    severity VARCHAR(20) DEFAULT 'WARNING', -- INFO, WARNING, CRITICAL
    weight INTEGER DEFAULT 10,
    owner VARCHAR(100),
    depends_on_rule_id INTEGER REFERENCES metadata.data_quality_rules(rule_id),
    execution_order INTEGER DEFAULT 10,
    enabled CHAR(1) DEFAULT 'Y',
    rule_version INTEGER DEFAULT 1,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retired_at TIMESTAMP NULL
);

-- 2. Results Logging Table
CREATE TABLE IF NOT EXISTS metadata.data_quality_results (
    result_id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    rule_id INTEGER REFERENCES metadata.data_quality_rules(rule_id),
    status VARCHAR(20), -- PASS, FAIL, ERROR
    expected_value VARCHAR(255),
    actual_value VARCHAR(255),
    failed_rows INTEGER DEFAULT 0,
    failure_pct NUMERIC(8,4) DEFAULT 0,
    execution_ms INTEGER DEFAULT 0,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dq_results_run_id ON metadata.data_quality_results(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_rule_id ON metadata.data_quality_results(rule_id);

-- 3. Quarantine (Failures) Table
CREATE TABLE IF NOT EXISTS metadata.data_quality_failures (
    failure_id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    rule_id INTEGER REFERENCES metadata.data_quality_rules(rule_id),
    pk_value VARCHAR(255),
    reason TEXT,
    sample_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dq_failures_run_id ON metadata.data_quality_failures(run_id);

-- 4. Overall Table Quality Scores
CREATE TABLE IF NOT EXISTS metadata.table_quality_scores (
    score_id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    table_id INTEGER REFERENCES metadata.db_tables_etl(table_id),
    execution_layer VARCHAR(20),
    quality_score NUMERIC(5,2) DEFAULT 100.0,
    status VARCHAR(20) -- PASS, FAIL
);
CREATE INDEX IF NOT EXISTS idx_table_quality_run_id ON metadata.table_quality_scores(run_id);

