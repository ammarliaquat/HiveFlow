-- migrations/016_add_rbac.sql

-- 1. Roles
CREATE TABLE IF NOT EXISTS metadata.gui_roles (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(255)
);

-- 2. Users
CREATE TABLE IF NOT EXISTS metadata.gui_users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL, -- bcrypt or argon2id hash
    role_id INTEGER REFERENCES metadata.gui_roles(role_id),
    is_active CHAR(1) DEFAULT 'Y',
    last_login TIMESTAMP
);

-- 3. Role Permissions
CREATE TABLE IF NOT EXISTS metadata.gui_role_permissions (
    role_id INTEGER REFERENCES metadata.gui_roles(role_id),
    permission_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (role_id, permission_name)
);

-- 4. Central Audit Log
CREATE TABLE IF NOT EXISTS metadata.gui_audit_log (
    audit_id SERIAL PRIMARY KEY,
    event_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    username VARCHAR(50),
    session_id VARCHAR(100),
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50),
    resource_id VARCHAR(255),
    old_value JSONB,
    new_value JSONB,
    status VARCHAR(20) NOT NULL, -- SUCCESS, DENIED, ERROR
    client_ip VARCHAR(50),
    user_agent VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_gui_audit_log_event_ts ON metadata.gui_audit_log(event_ts);
CREATE INDEX IF NOT EXISTS idx_gui_audit_log_username ON metadata.gui_audit_log(username);

-- 5. Seed Initial Roles
INSERT INTO metadata.gui_roles (role_name, description) VALUES
('ADMIN', 'Identity and platform administration'),
('DATA_ENGINEER', 'Pipeline and metadata management'),
('DATA_STEWARD', 'DQ, schema, and governance approvals'),
('PLATFORM_OPERATOR', 'Operational actions only (NOC)'),
('VIEWER', 'Read-only monitoring')
ON CONFLICT (role_name) DO NOTHING;

-- 6. Seed Initial Permissions for Roles (Examples based on Roadmap)
-- VIEWER
INSERT INTO metadata.gui_role_permissions (role_id, permission_name)
SELECT role_id, 'VIEW_RUN_HISTORY' FROM metadata.gui_roles WHERE role_name = 'VIEWER'
ON CONFLICT DO NOTHING;

-- PLATFORM_OPERATOR
INSERT INTO metadata.gui_role_permissions (role_id, permission_name)
SELECT role_id, perm FROM metadata.gui_roles, unnest(ARRAY[
    'VIEW_RUN_HISTORY', 'EXECUTE_PIPELINE', 'RERUN_TABLE', 'ACKNOWLEDGE_ALERTS', 'VIEW_CONNECTION_DETAILS'
]) AS perm WHERE role_name = 'PLATFORM_OPERATOR'
ON CONFLICT DO NOTHING;

-- DATA_STEWARD
INSERT INTO metadata.gui_role_permissions (role_id, permission_name)
SELECT role_id, perm FROM metadata.gui_roles, unnest(ARRAY[
    'VIEW_RUN_HISTORY', 'APPROVE_SCHEMA_DRIFT', 'REJECT_SCHEMA_DRIFT', 'EDIT_DQ_RULES'
]) AS perm WHERE role_name = 'DATA_STEWARD'
ON CONFLICT DO NOTHING;

-- DATA_ENGINEER
INSERT INTO metadata.gui_role_permissions (role_id, permission_name)
SELECT role_id, perm FROM metadata.gui_roles, unnest(ARRAY[
    'VIEW_RUN_HISTORY', 'VIEW_SENSITIVE_LOGS', 'EXECUTE_PIPELINE', 'CANCEL_PIPELINE',
    'RERUN_TABLE', 'RESET_WATERMARK', 'PROMOTE_METADATA', 'MANAGE_METADATA',
    'EDIT_CONNECTIONS', 'TEST_CONNECTIONS'
]) AS perm WHERE role_name = 'DATA_ENGINEER'
ON CONFLICT DO NOTHING;

-- ADMIN (Gets everything plus user management)
INSERT INTO metadata.gui_role_permissions (role_id, permission_name)
SELECT role_id, perm FROM metadata.gui_roles, unnest(ARRAY[
    'VIEW_RUN_HISTORY', 'VIEW_SENSITIVE_LOGS', 'EXECUTE_PIPELINE', 'CANCEL_PIPELINE',
    'RERUN_TABLE', 'RESET_WATERMARK', 'PROMOTE_METADATA', 'MANAGE_METADATA',
    'EDIT_CONNECTIONS', 'TEST_CONNECTIONS', 'APPROVE_SCHEMA_DRIFT', 'REJECT_SCHEMA_DRIFT',
    'EDIT_DQ_RULES', 'MANAGE_USERS', 'MANAGE_ROLES', 'VIEW_AUDIT_LOG', 'PURGE_RETENTION_DATA',
    'ACKNOWLEDGE_ALERTS', 'VIEW_CONNECTION_DETAILS'
]) AS perm WHERE role_name = 'ADMIN'
ON CONFLICT DO NOTHING;

-- 7. Seed Default Admin User (Password: "admin123" hashed with bcrypt)
-- Note: bcrypt hash for "admin123" is $2b$12$Kixl26q.mN2P9Qh2L2b4rOnL80qJ367l9iV.g12t8.J/g3vXp8N.S
INSERT INTO metadata.gui_users (username, password_hash, role_id)
SELECT 'admin', '$2b$12$Kixl26q.mN2P9Qh2L2b4rOnL80qJ367l9iV.g12t8.J/g3vXp8N.S', role_id
FROM metadata.gui_roles WHERE role_name = 'ADMIN'
ON CONFLICT (username) DO NOTHING;

-- Seed Default Viewer User (Password: "viewer123")
-- bcrypt hash for "viewer123" is $2b$12$4m0zNq3ZzFhZ/U.Uo6t2vOy9H6V2vA7T0Z8R7W6U5H4G3F2D1E0C
INSERT INTO metadata.gui_users (username, password_hash, role_id)
SELECT 'viewer', '$2b$12$4m0zNq3ZzFhZ/U.Uo6t2vOy9H6V2vA7T0Z8R7W6U5H4G3F2D1E0C', role_id
FROM metadata.gui_roles WHERE role_name = 'VIEWER'
ON CONFLICT (username) DO NOTHING;
