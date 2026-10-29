-- ============================================================
-- FILE: migrations/032_job01_authoritative_execution_lifecycle.sql
-- PURPOSE: JOB-01: Authoritative Execution Lifecycle, Scope Claims, and Submission Audit
-- ============================================================

-- Phase A: Rename legacy gui_job_registry if present without run_id PK
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'metadata' AND table_name = 'gui_job_registry'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'metadata' AND table_name = 'gui_job_registry' AND column_name = 'run_id'
        ) THEN
            ALTER TABLE metadata.gui_job_registry RENAME TO gui_job_registry_legacy;
        END IF;
    END IF;
END $$;

-- Phase B: Create Authoritative Job Registry
CREATE TABLE IF NOT EXISTS metadata.gui_job_registry (
    run_id                      UUID PRIMARY KEY,
    action                      VARCHAR(50) NOT NULL,
    source_tag                  VARCHAR(100) NOT NULL,
    target_tag                  VARCHAR(100),
    priority                    INTEGER,
    parameters                  JSONB,
    status                      VARCHAR(30) DEFAULT 'QUEUED' NOT NULL,
    pid                         INTEGER,
    submitted_at                TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    dispatched_at               TIMESTAMPTZ,
    started_at                  TIMESTAMPTZ,
    last_heartbeat              TIMESTAMPTZ,
    completed_at                TIMESTAMPTZ,
    dispatcher_id               VARCHAR(100),
    dispatch_lease_expires_at   TIMESTAMPTZ,
    worker_attempt_number       INTEGER DEFAULT 1 NOT NULL,
    actor_type                  VARCHAR(20) DEFAULT 'USER' NOT NULL,
    actor_id                    VARCHAR(100) DEFAULT 'anonymous' NOT NULL,
    error_code                  VARCHAR(50),
    error_message               TEXT,
    CONSTRAINT chk_job_status CHECK (
        status IN ('QUEUED', 'DISPATCHED', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED', 'ABANDONED')
    ),
    CONSTRAINT chk_actor_type CHECK (
        actor_type IN ('USER', 'SYSTEM', 'SCHEDULER')
    )
);

CREATE INDEX IF NOT EXISTS idx_gui_job_registry_status ON metadata.gui_job_registry (status, submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_gui_job_registry_source_tag ON metadata.gui_job_registry (source_tag, submitted_at DESC);

-- Phase C: Create Append-Only Job Submission Audit Ledger
CREATE TABLE IF NOT EXISTS metadata.job_submission_audit (
    request_id              UUID PRIMARY KEY,
    requested_at            TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    actor_type              VARCHAR(20) DEFAULT 'USER' NOT NULL,
    actor_id                VARCHAR(100) DEFAULT 'anonymous' NOT NULL,
    action                  VARCHAR(50) NOT NULL,
    source_tag              VARCHAR(100) NOT NULL,
    target_tag              VARCHAR(100),
    priority                INTEGER,
    outcome                 VARCHAR(20) NOT NULL,
    run_id                  UUID REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    rejection_code          VARCHAR(50),
    existing_active_run_id  UUID,
    parameters              JSONB,
    CONSTRAINT chk_submission_outcome CHECK (
        outcome IN ('ACCEPTED', 'REJECTED')
    )
);

CREATE INDEX IF NOT EXISTS idx_submission_audit_requested_at ON metadata.job_submission_audit (requested_at DESC);

-- Phase D: Create Job Scope Claims for Database-Enforced Concurrency
CREATE TABLE IF NOT EXISTS metadata.job_scope_claim (
    claim_id                BIGSERIAL PRIMARY KEY,
    run_id                  UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    source_table_id         INTEGER NOT NULL,
    action                  VARCHAR(50) NOT NULL,
    created_at              TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    released_at             TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_job_scope_claim 
ON metadata.job_scope_claim (source_table_id, action) 
WHERE released_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_job_scope_claim_run_id ON metadata.job_scope_claim (run_id);

-- Phase E: Create Append-Only Lifecycle Event History
CREATE TABLE IF NOT EXISTS metadata.job_event (
    event_id                BIGSERIAL PRIMARY KEY,
    run_id                  UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    event_type              VARCHAR(50) NOT NULL,
    from_status             VARCHAR(30),
    to_status               VARCHAR(30) NOT NULL,
    event_timestamp         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    actor_type              VARCHAR(20) DEFAULT 'SYSTEM' NOT NULL,
    actor_id                VARCHAR(100) DEFAULT 'system' NOT NULL,
    details                 JSONB
);

CREATE INDEX IF NOT EXISTS idx_job_event_run_id ON metadata.job_event (run_id, event_timestamp ASC);
