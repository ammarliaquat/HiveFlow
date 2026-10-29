// frontend/src/api/types.ts

export interface ResponseMeta {
  request_id: string;
  timestamp: string;
  project_context?: string | null;
}

export interface ErrorDetail {
  code: string;
  message: string;
  field?: string | null;
}

export interface ResponseEnvelope<T> {
  success: boolean;
  data: T | null;
  error: ErrorDetail | null;
  meta: ResponseMeta;
}

export interface UserResponse {
  username: string;
  role: string;
  permissions: string[];
  allowed_projects: string[];
}

export interface KPIData {
  running: number;
  success_today: number;
  failed_today: number;
  last_run_end: string | null;
  stalled_count: number;
  project_scope: string | null;
}

export interface RecentActivityItem {
  run_id: string | null;
  layer: string;
  source: string;
  target_tag: string | null;
  table: string;
  target_table: string | null;
  status: string;
  source_count: number;
  target_count: number;
  elapsed_sec: number;
  end_time: string | null;
}

export interface DashboardActivityResponse {
  activities: RecentActivityItem[];
}

export interface PipelineItem {
  table_id?: number;
  source_tag: string;
  source_table?: string;
  table_name: string;
  target_tag?: string | null;
  target_table?: string | null;
  target_table_name?: string | null;
  priority: number;
  is_active: boolean;
  is_cdc: boolean;
  cdc_strategy: string | null;
  columns_unique_key?: string | null;
  execution_authority_mode?: string | null;
  last_success_date?: string | null;
  last_success_timestamp: string | null;
  landing_mode?: string;
  watermark_column?: string | null;
  key_strategy?: string | null;
  business_keys?: string[];
  last_status?: string | null;
  last_row_count?: number | null;
}

export interface ColumnMappingItem {
  source_column: string;
  target_column: string;
  canonical_type: string;
  transformation: string | null;
  is_business_key: boolean;
  is_nullable: boolean;
  is_surrogate_key: boolean;
}

export interface PipelineDetailResponse {
  pipeline: PipelineItem;
  columns: ColumnMappingItem[];
}

export interface PipelineListResponse {
  pipelines: PipelineItem[];
  total_count: number;
}

export interface RunSummaryItem {
  run_id: string;
  action: string | null;
  source_tag: string | null;
  target_tag: string | null;
  source_table: string | null;
  target_table: string | null;
  priority: number | null;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  elapsed_seconds: number;
  source_count: number;
  target_count: number;
  rows_per_minute?: number;
  error_message: string | null;
}

export interface RunStageItem {
  stage_name: string;
  status: string;
  rows_processed: number;
  throughput_rows_sec: number;
  throughput_rows_min?: number;
}

export interface RunDetailResponse {
  run_id: string;
  parent_run_id: string | null;
  retry_attempt: number;
  project_id: string;
  environment: string;
  pipeline_id: string | null;
  dataset_id: string | null;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  last_heartbeat_at: string | null;
  elapsed_seconds: number;
  rows_read: number;
  extracted_count: number;
  rows_written: number;
  throughput_rows_sec: number;
  rolling_rows_per_second: number;
  rows_per_minute: number;
  rolling_rows_per_minute: number;
  current_chunk: number | null;
  total_chunks: number | null;
  chunks_completed: number | null;
  chunk_progress_pct: number | null;
  is_stalled: boolean;
  last_heartbeat_age_seconds: number | null;
  estimated_remaining_seconds: number | null;
  reconciliation_status: string;
  error_message: string | null;
  stages: RunStageItem[];
}

export interface ProbeDetail {
  name: string;
  status: 'HEALTHY' | 'DEGRADED' | 'UNREACHABLE';
  message: string;
  latency_ms: number;
}

export interface SystemHealthData {
  overall_status: 'HEALTHY' | 'DEGRADED' | 'UNREACHABLE';
  checked_at: string;
  probes: ProbeDetail[];
}

export interface TriggerRunRequest {
  source_tag: string;
  target_tag?: string | null;
  source_table_ids?: number[] | null;
  source_table?: string | null;
  execution_scope?: 'FULL' | 'BRONZE' | 'SILVER' | 'GOLD';
  priority?: number | null;
  request_id?: string | null;
}

export interface TriggerRunResponse {
  accepted: boolean;
  request_id: string;
  run_id?: string | null;
  status: string;
  submitted_at?: string | null;
  rejection_code?: string | null;
  rejection_reason?: string | null;
  existing_active_run_id?: string | null;
}

export interface CancelRunResponse {
  success: boolean;
  run_id: string;
  message: string;
}

export interface RunLogsResponse {
  run_id: string;
  logs: string;
  lines_returned: number;
}

export interface IntegrationDetail {
  tag: string;
  connection_type: string;
  supports_source: boolean;
  supports_target: boolean;
  environment: string;
  db_type: string;
  server: string;
  port: number;
  database_name: string;
  schema_name?: string | null;
  username?: string | null;
  is_active: string;
  purpose?: string | null;
  last_tested_at?: string | null;
  last_test_status?: string | null;
  last_latency_ms?: number | null;
}

export interface IntegrationsSummary {
  total_count: number;
  source_count: number;
  target_count: number;
  active_count: number;
  integrations: IntegrationDetail[];
}

export interface ConnectionCreateRequest {
  tag: string;
  connection_type?: string;
  supports_source: boolean;
  supports_target: boolean;
  environment: string;
  db_type: string;
  server: string;
  port: number;
  database_name: string;
  schema_name?: string | null;
  username: string;
  credential_reference: string;
  service_name?: string | null;
  connection_options?: Record<string, any> | null;
  owner?: string | null;
  team?: string | null;
  purpose?: string | null;
}

export interface ConnectionTestResult {
  status: 'SUCCESS' | 'FAILED';
  latency_ms: number;
  error?: string | null;
}

export interface ConnectionSaveResponse {
  tag: string;
  status: string;
  message: string;
}

// ── Discovery & Promotion Interfaces ──────────────────────────────────────────

export interface CrawlRequest {
  source_tag: string;
}

export interface CrawlResponse {
  run_id: string;
  status: string;
  message: string;
}

export interface CrawlStatusResponse {
  run_id: string;
  status: string;
  elapsed_seconds: number;
  error_message?: string | null;
}

export interface CatalogTableItem {
  table_master_key: number;
  source_tag: string;
  database_type?: string | null;
  schema_name?: string | null;
  source_table: string;
  table_type: string;
  row_count: number;
  column_count: number;
  business_key?: string | null;
  key_source: string;
  key_confidence: number;
  cdc_columns?: string | null;
  cdc_strategy: string;
  cdc_confidence: number;
  cdc_health?: string | null;
  cdc_health_details?: string | null;
  insert_date?: string | null;
}

export interface CatalogSummaryResponse {
  total_tables: number;
  total_columns: number;
  total_rows: number;
  tables: CatalogTableItem[];
}

export interface CatalogColumnItem {
  column_name: string;
  data_type: string;
  data_length?: number | null;
  data_precision?: number | null;
  data_scale?: number | null;
  column_seq?: number | null;
  is_nullable?: string | null;
}

export interface PromoteRequest {
  source_tag: string;
  target_tag: string;
  table_names: string[];
  priority: number;
  refresh_existing?: boolean;
  steward_confirmed: boolean;
}

export interface PolicyRefreshSummary {
  updated: string[];
  unchanged: string[];
  skipped: string[];
  failed: Array<Record<string, any>>;
}

export interface PromoteResponse {
  successful: string[];
  failed: Array<Record<string, any>>;
  total: number;
  priority_updated?: number | null;
  priority?: number | null;
  policy_refresh?: PolicyRefreshSummary | null;
  message: string;
}

export interface DriftCheckItem {
  source_tag: string;
  source_table: string;
  etl_key?: string | null;
  catalog_key?: string | null;
  alert: string;
}

export interface DriftCheckResponse {
  source_tag?: string | null;
  total_mismatches: number;
  mismatches: DriftCheckItem[];
  status: string;
  message: string;
}

export interface Tier2SubscriptionItem {
  source_table: string;
  target_tag: string;
  execution_authority_mode: string;
  is_active: string;
  is_cdc: string;
  cdc_strategy: string;
  business_key?: string | null;
  execution_status: string;
  load_method: string;
}

export interface UnpromotedTablesResponse {
  source_tag: string;
  target_tag: string;
  unpromoted_tables: string[];
  catalog_tables: string[];
  promoted_tables: string[];
}

// ── User & Access Management Types ──────────────────────────────────────────

export interface UserItem {
  user_id: number;
  username: string;
  role_id: number | null;
  role: string;
  is_active: boolean;
  full_name: string;
  email: string;
  allowed_projects: string[];
  created_at: string | null;
  created_by: string;
  last_login: string | null;
}

export interface CreateUserPayload {
  username: string;
  password: string;
  role_id: number;
  full_name?: string;
  email?: string;
  allowed_projects?: string[];
}

export interface UpdateUserPayload {
  role_id?: number;
  is_active?: boolean;
  full_name?: string;
  email?: string;
  allowed_projects?: string[];
}

export interface PermissionItem {
  id: string;
  category: string;
  description: string;
}

export interface RoleItem {
  role_id: number;
  role_name: string;
  description: string;
  permissions: string[];
  user_count: number;
  is_system_role: boolean;
}

export interface CreateRolePayload {
  role_name: string;
  description?: string;
  permissions: string[];
}

export interface UpdateRolePayload {
  role_name?: string;
  description?: string;
  permissions?: string[];
}

export interface AuditLogItem {
  audit_id: number;
  event_ts: string | null;
  username: string;
  session_id: string;
  action: string;
  resource_type: string;
  resource_id: string;
  old_value: any;
  new_value: any;
  status: string;
  client_ip: string;
  user_agent: string;
}

export interface AuditLogsResponse {
  total: number;
  limit: number;
  offset: number;
  items: AuditLogItem[];
}

export interface AuditSummaryResponse {
  total_operations: number;
  active_users_count: number;
  top_users: Array<{ username: string; count: number; last_active?: string | null }>;
  top_actions: Array<{ action: string; count: number }>;
}



