import type {
  ResponseEnvelope,
  UserResponse,
  KPIData,
  DashboardActivityResponse,
  PipelineItem,
  PipelineDetailResponse,
  RunSummaryItem,
  RunDetailResponse,
  SystemHealthData,
  TriggerRunRequest,
  TriggerRunResponse,
  CancelRunResponse,
  RunLogsResponse,
  IntegrationsSummary,
  ConnectionCreateRequest,
  ConnectionTestResult,
  ConnectionSaveResponse,
  CrawlResponse,
  CrawlStatusResponse,
  CatalogSummaryResponse,
  CatalogColumnItem,
  PromoteRequest,
  PromoteResponse,
  DriftCheckResponse,
  Tier2SubscriptionItem,
  UnpromotedTablesResponse,
  UserItem,
  CreateUserPayload,
  UpdateUserPayload,
  PermissionItem,
  RoleItem,
  CreateRolePayload,
  UpdateRolePayload,
  AuditLogsResponse,
  AuditSummaryResponse,
} from './types';



export class ApiError extends Error {
  code: string;
  field?: string | null;
  requestId?: string;

  constructor(message: string, code: string = 'UNKNOWN', field?: string | null, requestId?: string) {
    super(message);
    this.name = 'ApiError';
    this.code = code;
    this.field = field;
    this.requestId = requestId;
  }
}

function getCsrfTokenFromCookie(): string | null {
  if (typeof document === 'undefined') return null;
  const match = document.cookie.match(new RegExp('(^|;\\s*)etl_csrf=([^;]+)'));
  return match ? decodeURIComponent(match[2]) : null;
}

async function request<T>(path: string, options: RequestInit = {}, projectScope?: string | null): Promise<T> {
  const headers = new Headers(options.headers || {});
  headers.set('Accept', 'application/json');
  if (!(options.body instanceof FormData)) {
    headers.set('Content-Type', 'application/json');
  }
  if (projectScope) {
    headers.set('X-Project-Context', projectScope);
  }

  // Inject double-submit CSRF token on mutating requests
  const method = (options.method || 'GET').toUpperCase();
  if (['POST', 'PUT', 'DELETE', 'PATCH'].includes(method)) {
    const csrfToken = getCsrfTokenFromCookie();
    if (csrfToken) {
      headers.set('X-CSRF-Token', csrfToken);
    }
  }

  const response = await fetch(path, {
    ...options,
    headers,
    credentials: 'include', // sends and receives HttpOnly session cookies
  });

  let envelope: ResponseEnvelope<T>;
  try {
    envelope = await response.json();
  } catch {
    throw new ApiError(`Invalid server response from ${path} (${response.status})`, 'INVALID_RESPONSE');
  }

  if (!envelope.success || !response.ok) {
    const errorMsg = envelope.error?.message || `Request failed with status ${response.status}`;
    const errorCode = envelope.error?.code || `HTTP_${response.status}`;
    throw new ApiError(errorMsg, errorCode, envelope.error?.field, envelope.meta?.request_id);
  }

  return envelope.data as T;
}

export const api = {
  // Auth
  login: (credentials: { username: string; password: string }) =>
    request<UserResponse>('/api/v1/auth/login', {
      method: 'POST',
      body: JSON.stringify(credentials),
    }),

  logout: () =>
    request<{ logged_out: boolean }>('/api/v1/auth/logout', {
      method: 'POST',
    }),

  getMe: () => request<UserResponse>('/api/v1/auth/me'),

  // Dashboard
  getKPIs: (projectScope?: string | null) =>
    request<KPIData>('/api/v1/dashboard/kpis', {}, projectScope),

  getActivity: (limit = 15, projectScope?: string | null) =>
    request<DashboardActivityResponse>(`/api/v1/dashboard/activity?limit=${limit}`, {}, projectScope),

  // Pipelines
  getPipelines: (projectScope?: string | null) => {
    const url = projectScope ? `/api/v1/pipelines?source_tag=${encodeURIComponent(projectScope)}` : '/api/v1/pipelines';
    return request<PipelineItem[]>(url, {}, projectScope);
  },

  getPipelineDetail: (sourceTag: string, tableName: string) =>
    request<PipelineDetailResponse>(`/api/v1/pipelines/${encodeURIComponent(sourceTag)}/${encodeURIComponent(tableName)}`),

  // Runs
  getRuns: (limit = 25, projectScope?: string | null) =>
    request<RunSummaryItem[]>(`/api/v1/runs?limit=${limit}`, {}, projectScope),

  getRunDetail: (runId: string) =>
    request<RunDetailResponse>(`/api/v1/runs/${encodeURIComponent(runId)}`),

  triggerRun: (payload: TriggerRunRequest, projectScope?: string | null) =>
    request<TriggerRunResponse>(
      '/api/v1/runs/trigger',
      {
        method: 'POST',
        body: JSON.stringify(payload),
      },
      projectScope
    ),

  cancelRun: (runId: string, projectScope?: string | null) =>
    request<CancelRunResponse>(
      `/api/v1/runs/${encodeURIComponent(runId)}/cancel`,
      {
        method: 'POST',
      },
      projectScope
    ),

  getRunLogs: (runId: string, maxLines = 200, projectScope?: string | null) =>
    request<RunLogsResponse>(
      `/api/v1/runs/${encodeURIComponent(runId)}/logs?max_lines=${maxLines}`,
      {},
      projectScope
    ),

  // System & Integrations
  getHealth: () => request<SystemHealthData>('/api/v1/system/health'),

  getIntegrations: (projectScope?: string | null) => {
    const url = projectScope ? `/api/v1/system/integrations?scope=${encodeURIComponent(projectScope)}` : '/api/v1/system/integrations';
    return request<IntegrationsSummary>(url, {}, projectScope);
  },

  testConnection: (profile: ConnectionCreateRequest, projectScope?: string | null) =>
    request<ConnectionTestResult>(
      '/api/v1/system/connections/test',
      {
        method: 'POST',
        body: JSON.stringify(profile),
      },
      projectScope
    ),

  saveConnection: (profile: ConnectionCreateRequest, projectScope?: string | null) =>
    request<ConnectionSaveResponse>(
      '/api/v1/system/connections',
      {
        method: 'POST',
        body: JSON.stringify(profile),
      },
      projectScope
    ),

  clearCache: () =>
    request<{ status: string; message: string }>('/api/v1/system/cache/clear', {
      method: 'POST',
    }),

  // Discovery & Promotion
  crawlSource: (sourceTag: string, projectScope?: string | null) =>
    request<CrawlResponse>(
      '/api/v1/discovery/crawl',
      {
        method: 'POST',
        body: JSON.stringify({ source_tag: sourceTag }),
      },
      projectScope
    ),

  getCrawlStatus: (runId: string) =>
    request<CrawlStatusResponse>(`/api/v1/discovery/crawl/${encodeURIComponent(runId)}/status`),

  getCrawlLogs: (runId: string) =>
    request<{ run_id: string; logs: string }>(`/api/v1/discovery/crawl/${encodeURIComponent(runId)}/logs`),

  getCatalog: (sourceTag?: string | null, projectScope?: string | null) => {
    const url = sourceTag
      ? `/api/v1/discovery/catalog?source_tag=${encodeURIComponent(sourceTag)}`
      : '/api/v1/discovery/catalog';
    return request<CatalogSummaryResponse>(url, {}, projectScope);
  },

  getCatalogColumns: (tableMasterKey: number) =>
    request<CatalogColumnItem[]>(`/api/v1/discovery/catalog/columns/${tableMasterKey}`),

  getTargets: () => request<string[]>('/api/v1/discovery/targets'),

  getUnpromotedTables: (sourceTag: string, targetTag: string, projectScope?: string | null) =>
    request<UnpromotedTablesResponse>(
      `/api/v1/discovery/unpromoted?source_tag=${encodeURIComponent(sourceTag)}&target_tag=${encodeURIComponent(targetTag)}`,
      {},
      projectScope
    ),

  getSubscriptions: (sourceTag: string, targetTag?: string | null, projectScope?: string | null) => {
    let url = `/api/v1/discovery/subscriptions?source_tag=${encodeURIComponent(sourceTag)}`;
    if (targetTag) {
      url += `&target_tag=${encodeURIComponent(targetTag)}`;
    }
    return request<Tier2SubscriptionItem[]>(url, {}, projectScope);
  },

  promoteDatasets: (payload: PromoteRequest, projectScope?: string | null) =>
    request<PromoteResponse>(
      '/api/v1/discovery/promote',
      {
        method: 'POST',
        body: JSON.stringify(payload),
      },
      projectScope
    ),

  auditDrift: (sourceTag?: string | null) => {
    const url = sourceTag
      ? `/api/v1/discovery/drift-check?source_tag=${encodeURIComponent(sourceTag)}`
      : '/api/v1/discovery/drift-check';
    return request<DriftCheckResponse>(url, { method: 'POST' });
  },

  // ── User Management ────────────────────────────────────────────────────────
  getUsers: () => request<UserItem[]>('/api/v1/users'),

  createUser: (payload: CreateUserPayload) =>
    request<UserItem>('/api/v1/users', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  updateUser: (userId: number, payload: UpdateUserPayload) =>
    request<UserItem>(`/api/v1/users/${userId}/update`, {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  resetUserPassword: (userId: number, newPassword: string) =>
    request<{ success: boolean; message: string }>(`/api/v1/users/${userId}/reset-password`, {
      method: 'POST',
      body: JSON.stringify({ new_password: newPassword }),
    }),

  dropUser: (userId: number) =>
    request<{ dropped: boolean; user_id: number }>(`/api/v1/users/${userId}/drop`, {
      method: 'POST',
    }),

  // ── Role & Access Management ───────────────────────────────────────────────
  getRoles: () => request<RoleItem[]>('/api/v1/roles'),

  getPermissions: () => request<PermissionItem[]>('/api/v1/roles/permissions'),

  createRole: (payload: CreateRolePayload) =>
    request<RoleItem>('/api/v1/roles', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  updateRole: (roleId: number, payload: UpdateRolePayload) =>
    request<RoleItem>(`/api/v1/roles/${roleId}/update`, {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  dropRole: (roleId: number) =>
    request<{ dropped: boolean; role_id: number }>(`/api/v1/roles/${roleId}/drop`, {
      method: 'POST',
    }),

  // ── User Activity & Audit Observer ─────────────────────────────────────────
  getAuditLogs: (params?: {
    username?: string;
    action?: string;
    resource_type?: string;
    status?: string;
    start_date?: string;
    end_date?: string;
    limit?: number;
    offset?: number;
  }) => {
    const q = new URLSearchParams();
    if (params?.username) q.set('username', params.username);
    if (params?.action) q.set('action', params.action);
    if (params?.resource_type) q.set('resource_type', params.resource_type);
    if (params?.status) q.set('status', params.status);
    if (params?.start_date) q.set('start_date', params.start_date);
    if (params?.end_date) q.set('end_date', params.end_date);
    if (params?.limit) q.set('limit', String(params.limit));
    if (params?.offset) q.set('offset', String(params.offset));
    const qs = q.toString();
    return request<AuditLogsResponse>(`/api/v1/audit/logs${qs ? `?${qs}` : ''}`);
  },

  getAuditSummary: () => request<AuditSummaryResponse>('/api/v1/audit/summary'),
};


