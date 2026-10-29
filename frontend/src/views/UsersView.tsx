// frontend/src/views/UsersView.tsx
import React, { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { api } from '../api/client';
import type {
  UserItem,
  RoleItem,
  PermissionItem,
  AuditLogItem,
  CreateUserPayload,
  UpdateUserPayload,
  CreateRolePayload,
  UpdateRolePayload,
} from '../api/types';
import {
  Users,
  Shield,
  ShieldCheck,
  Key,
  Lock,
  Eye,
  EyeOff,
  UserPlus,
  UserX,
  Search,
  RefreshCw,
  CheckCircle2,
  AlertTriangle,
  X,
  Plus,
  Trash2,
  Edit3,
  Sliders,
  Activity,
  Info,
  Terminal,
  Copy,
  Check,
  Filter,
} from 'lucide-react';

export const UsersView: React.FC = () => {
  const { user: currentUser } = useAuth();
  const queryClient = useQueryClient();

  // Active sub-tab
  const [activeTab, setActiveTab] = useState<'users' | 'roles' | 'audit'>('users');

  // ───────────────────────────────────────────────────────────────────────────
  // Queries
  // ───────────────────────────────────────────────────────────────────────────
  const {
    data: users = [],
    isLoading: isLoadingUsers,
    refetch: refetchUsers,
    isFetching: isFetchingUsers,
  } = useQuery({
    queryKey: ['adminUsers'],
    queryFn: () => api.getUsers(),
  });

  const {
    data: roles = [],
    isLoading: isLoadingRoles,
    refetch: refetchRoles,
    isFetching: isFetchingRoles,
  } = useQuery({
    queryKey: ['adminRoles'],
    queryFn: () => api.getRoles(),
  });

  const {
    data: permissions = [],
    isLoading: isLoadingPermissions,
  } = useQuery({
    queryKey: ['systemPermissions'],
    queryFn: () => api.getPermissions(),
  });

  const {
    data: auditSummary,
    refetch: refetchSummary,
  } = useQuery({
    queryKey: ['auditSummary'],
    queryFn: () => api.getAuditSummary(),
    refetchInterval: 30000,
  });

  // Audit filter state
  const [auditUserFilter, setAuditUserFilter] = useState<string>('');
  const [auditActionFilter, setAuditActionFilter] = useState<string>('');
  const [auditStatusFilter, setAuditStatusFilter] = useState<string>('');

  const {
    data: auditLogsData,
    isLoading: isLoadingAudit,
    refetch: refetchAudit,
    isFetching: isFetchingAudit,
  } = useQuery({
    queryKey: ['auditLogs', auditUserFilter, auditActionFilter, auditStatusFilter],
    queryFn: () =>
      api.getAuditLogs({
        username: auditUserFilter || undefined,
        action: auditActionFilter || undefined,
        status: auditStatusFilter || undefined,
        limit: 100,
      }),
    refetchInterval: 20000,
  });

  const auditLogs = auditLogsData?.items || [];

  // ───────────────────────────────────────────────────────────────────────────
  // User Filters
  // ───────────────────────────────────────────────────────────────────────────
  const [userSearchTerm, setUserSearchTerm] = useState('');
  const [userRoleFilter, setUserRoleFilter] = useState<string>('all');
  const [userStatusFilter, setUserStatusFilter] = useState<string>('all');

  const filteredUsers = useMemo(() => {
    return users.filter((u) => {
      if (userRoleFilter !== 'all' && u.role !== userRoleFilter) return false;
      if (userStatusFilter === 'active' && !u.is_active) return false;
      if (userStatusFilter === 'inactive' && u.is_active) return false;
      if (userSearchTerm) {
        const term = userSearchTerm.toLowerCase();
        const matchName = u.full_name?.toLowerCase().includes(term);
        const matchUser = u.username.toLowerCase().includes(term);
        const matchEmail = u.email?.toLowerCase().includes(term);
        if (!matchName && !matchUser && !matchEmail) return false;
      }
      return true;
    });
  }, [users, userSearchTerm, userRoleFilter, userStatusFilter]);

  // ───────────────────────────────────────────────────────────────────────────
  // Modal States
  // ───────────────────────────────────────────────────────────────────────────
  // 1. Create User Modal
  const [isCreateUserOpen, setIsCreateUserOpen] = useState(false);
  const [newUserUsername, setNewUserUsername] = useState('');
  const [newUserPassword, setNewUserPassword] = useState('');
  const [newUserFullName, setNewUserFullName] = useState('');
  const [newUserEmail, setNewUserEmail] = useState('');
  const [newUserRoleId, setNewUserRoleId] = useState<number>(roles[0]?.role_id || 1);
  const [newUserProjects, setNewUserProjects] = useState<string>('*');
  const [showNewUserPassword, setShowNewUserPassword] = useState(false);
  const [createUserError, setCreateUserError] = useState<string | null>(null);

  // 2. Edit User Modal
  const [editingUser, setEditingUser] = useState<UserItem | null>(null);
  const [editFullName, setEditFullName] = useState('');
  const [editEmail, setEditEmail] = useState('');
  const [editRoleId, setEditRoleId] = useState<number>(1);
  const [editIsActive, setEditIsActive] = useState<boolean>(true);
  const [editProjects, setEditProjects] = useState<string>('*');
  const [editUserError, setEditUserError] = useState<string | null>(null);

  // 3. Reset Password Modal
  const [resetUser, setResetUser] = useState<UserItem | null>(null);
  const [resetNewPassword, setResetNewPassword] = useState('');
  const [showResetPassword, setShowResetPassword] = useState(false);
  const [resetPasswordError, setResetPasswordError] = useState<string | null>(null);
  const [resetSuccessMsg, setResetSuccessMsg] = useState<string | null>(null);

  // 4. Drop User Dialog
  const [dropUserTarget, setDropUserTarget] = useState<UserItem | null>(null);
  const [dropUserError, setDropUserError] = useState<string | null>(null);

  // 5. Create Role Modal
  const [isCreateRoleOpen, setIsCreateRoleOpen] = useState(false);
  const [newRoleName, setNewRoleName] = useState('');
  const [newRoleDesc, setNewRoleDesc] = useState('');
  const [selectedPermissions, setSelectedPermissions] = useState<Set<string>>(new Set());
  const [createRoleError, setCreateRoleError] = useState<string | null>(null);

  // 6. Edit Role Modal
  const [editingRole, setEditingRole] = useState<RoleItem | null>(null);
  const [editRoleName, setEditRoleName] = useState('');
  const [editRoleDesc, setEditRoleDesc] = useState('');
  const [editRolePermissions, setEditRolePermissions] = useState<Set<string>>(new Set());
  const [editRoleError, setEditRoleError] = useState<string | null>(null);

  // 7. Drop Role Dialog
  const [dropRoleTarget, setDropRoleTarget] = useState<RoleItem | null>(null);
  const [dropRoleError, setDropRoleError] = useState<string | null>(null);

  // 8. Audit Event Diff Inspector Modal
  const [inspectedAuditItem, setInspectedAuditItem] = useState<AuditLogItem | null>(null);
  const [isCopiedDiff, setIsCopiedDiff] = useState(false);

  // ───────────────────────────────────────────────────────────────────────────
  // Mutations
  // ───────────────────────────────────────────────────────────────────────────
  const createUserMutation = useMutation({
    mutationFn: (payload: CreateUserPayload) => api.createUser(payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] });
      queryClient.invalidateQueries({ queryKey: ['auditLogs'] });
      queryClient.invalidateQueries({ queryKey: ['auditSummary'] });
      setIsCreateUserOpen(false);
      setNewUserUsername('');
      setNewUserPassword('');
      setNewUserFullName('');
      setNewUserEmail('');
      setCreateUserError(null);
    },
    onError: (err: any) => {
      setCreateUserError(err.message || 'Failed to create user');
    },
  });

  const updateUserMutation = useMutation({
    mutationFn: ({ userId, payload }: { userId: number; payload: UpdateUserPayload }) =>
      api.updateUser(userId, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] });
      queryClient.invalidateQueries({ queryKey: ['auditLogs'] });
      queryClient.invalidateQueries({ queryKey: ['auditSummary'] });
      setEditingUser(null);
      setEditUserError(null);
    },
    onError: (err: any) => {
      setEditUserError(err.message || 'Failed to update user');
    },
  });

  const resetPasswordMutation = useMutation({
    mutationFn: ({ userId, pass }: { userId: number; pass: string }) =>
      api.resetUserPassword(userId, pass),
    onSuccess: (res) => {
      queryClient.invalidateQueries({ queryKey: ['auditLogs'] });
      setResetSuccessMsg(res.message);
      setTimeout(() => {
        setResetUser(null);
        setResetNewPassword('');
        setResetSuccessMsg(null);
        setResetPasswordError(null);
      }, 1500);
    },
    onError: (err: any) => {
      setResetPasswordError(err.message || 'Failed to reset password');
    },
  });

  const dropUserMutation = useMutation({
    mutationFn: (userId: number) => api.dropUser(userId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] });
      queryClient.invalidateQueries({ queryKey: ['adminRoles'] });
      queryClient.invalidateQueries({ queryKey: ['auditLogs'] });
      queryClient.invalidateQueries({ queryKey: ['auditSummary'] });
      setDropUserTarget(null);
      setDropUserError(null);
    },
    onError: (err: any) => {
      setDropUserError(err.message || 'Failed to drop user');
    },
  });

  const createRoleMutation = useMutation({
    mutationFn: (payload: CreateRolePayload) => api.createRole(payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminRoles'] });
      queryClient.invalidateQueries({ queryKey: ['auditLogs'] });
      queryClient.invalidateQueries({ queryKey: ['auditSummary'] });
      setIsCreateRoleOpen(false);
      setNewRoleName('');
      setNewRoleDesc('');
      setSelectedPermissions(new Set());
      setCreateRoleError(null);
    },
    onError: (err: any) => {
      setCreateRoleError(err.message || 'Failed to create role');
    },
  });

  const updateRoleMutation = useMutation({
    mutationFn: ({ roleId, payload }: { roleId: number; payload: UpdateRolePayload }) =>
      api.updateRole(roleId, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminRoles'] });
      queryClient.invalidateQueries({ queryKey: ['auditLogs'] });
      queryClient.invalidateQueries({ queryKey: ['auditSummary'] });
      setEditingRole(null);
      setEditRoleError(null);
    },
    onError: (err: any) => {
      setEditRoleError(err.message || 'Failed to update role');
    },
  });

  const dropRoleMutation = useMutation({
    mutationFn: (roleId: number) => api.dropRole(roleId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminRoles'] });
      queryClient.invalidateQueries({ queryKey: ['auditLogs'] });
      queryClient.invalidateQueries({ queryKey: ['auditSummary'] });
      setDropRoleTarget(null);
      setDropRoleError(null);
    },
    onError: (err: any) => {
      setDropRoleError(err.message || 'Failed to drop role');
    },
  });

  // Group system permissions by category
  const permissionsByCategory = useMemo(() => {
    const groups: Record<string, PermissionItem[]> = {};
    for (const p of permissions) {
      if (!groups[p.category]) groups[p.category] = [];
      groups[p.category].push(p);
    }
    return groups;
  }, [permissions]);

  // Count active admins to enforce safeguard
  const activeAdminsCount = useMemo(() => {
    return users.filter((u) => u.role === 'ADMIN' && u.is_active).length;
  }, [users]);

  // Handlers for modal open
  const openEditUser = (u: UserItem) => {
    setEditingUser(u);
    setEditFullName(u.full_name || '');
    setEditEmail(u.email || '');
    setEditRoleId(u.role_id || roles[0]?.role_id || 1);
    setEditIsActive(u.is_active);
    setEditProjects(u.allowed_projects?.join(', ') || '*');
    setEditUserError(null);
  };

  const openResetPassword = (u: UserItem) => {
    setResetUser(u);
    setResetNewPassword('');
    setShowResetPassword(false);
    setResetPasswordError(null);
    setResetSuccessMsg(null);
  };

  const openEditRole = (r: RoleItem) => {
    setEditingRole(r);
    setEditRoleName(r.role_name);
    setEditRoleDesc(r.description || '');
    setEditRolePermissions(new Set(r.permissions || []));
    setEditRoleError(null);
  };

  const handleCopyDiff = (text: string) => {
    navigator.clipboard.writeText(text);
    setIsCopiedDiff(true);
    setTimeout(() => setIsCopiedDiff(false), 2000);
  };

  const isGlobalRefreshing = isFetchingUsers || isFetchingRoles || isFetchingAudit;

  const handleGlobalRefresh = () => {
    refetchUsers();
    refetchRoles();
    refetchAudit();
    refetchSummary();
  };

  return (
    <div className="p-8 max-w-[1600px] mx-auto space-y-8 font-sans">
      {/* ───────────────────────────────────────────────────────────────────── */}
      {/* View Header                                                           */}
      {/* ───────────────────────────────────────────────────────────────────── */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-800/80 pb-6">
        <div>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
              <ShieldCheck className="w-5 h-5" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-slate-100 tracking-tight">
                Access Control & Forensic Governance
              </h1>
              <p className="text-sm text-slate-400">
                Manage user credentials, granular role permissions, and observe live operator activity.
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={handleGlobalRefresh}
            disabled={isGlobalRefreshing}
            className="flex items-center gap-2 px-3 py-2 bg-slate-800/60 hover:bg-slate-800 text-slate-300 rounded-lg border border-slate-700/60 text-xs font-medium transition disabled:opacity-50"
            title="Refresh All Records"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isGlobalRefreshing ? 'animate-spin text-indigo-400' : ''}`} />
            <span>Refresh</span>
          </button>

          {activeTab === 'users' && (
            <button
              onClick={() => {
                setIsCreateUserOpen(true);
                setNewUserRoleId(roles[0]?.role_id || 1);
                setCreateUserError(null);
              }}
              className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-xs font-semibold shadow-sm transition"
            >
              <UserPlus className="w-4 h-4" />
              <span>New User</span>
            </button>
          )}

          {activeTab === 'roles' && (
            <button
              onClick={() => {
                setIsCreateRoleOpen(true);
                setSelectedPermissions(new Set());
                setCreateRoleError(null);
              }}
              className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-xs font-semibold shadow-sm transition"
            >
              <Plus className="w-4 h-4" />
              <span>Create Role</span>
            </button>
          )}

          {activeTab === 'audit' && (
            <div className="flex items-center gap-2 px-3 py-1.5 bg-cyan-950/40 border border-cyan-800/50 rounded-lg text-xs text-cyan-300">
              <span className="w-2 h-2 rounded-full bg-cyan-400 animate-pulse" />
              <span>Forensic Ledger Active</span>
            </div>
          )}
        </div>
      </div>

      {/* ───────────────────────────────────────────────────────────────────── */}
      {/* Sub-navigation Tabs                                                   */}
      {/* ───────────────────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-2 border-b border-slate-800/80">
        <button
          onClick={() => setActiveTab('users')}
          className={`flex items-center gap-2.5 px-4 py-3 text-sm font-medium border-b-2 transition ${
            activeTab === 'users'
              ? 'border-indigo-500 text-indigo-300 bg-indigo-500/5'
              : 'border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/30'
          }`}
        >
          <Users className="w-4 h-4" />
          <span>Users & Credentials</span>
          <span className="ml-1.5 px-2 py-0.5 rounded-full text-xs font-mono bg-slate-800 text-slate-300">
            {users.length}
          </span>
        </button>

        <button
          onClick={() => setActiveTab('roles')}
          className={`flex items-center gap-2.5 px-4 py-3 text-sm font-medium border-b-2 transition ${
            activeTab === 'roles'
              ? 'border-indigo-500 text-indigo-300 bg-indigo-500/5'
              : 'border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/30'
          }`}
        >
          <Shield className="w-4 h-4" />
          <span>Roles & Granular Access</span>
          <span className="ml-1.5 px-2 py-0.5 rounded-full text-xs font-mono bg-slate-800 text-slate-300">
            {roles.length}
          </span>
        </button>

        <button
          onClick={() => setActiveTab('audit')}
          className={`flex items-center gap-2.5 px-4 py-3 text-sm font-medium border-b-2 transition ${
            activeTab === 'audit'
              ? 'border-indigo-500 text-indigo-300 bg-indigo-500/5'
              : 'border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-800/30'
          }`}
        >
          <Eye className="w-4 h-4" />
          <span>Operator Activity Observer</span>
          {auditSummary && (
            <span className="ml-1.5 px-2 py-0.5 rounded-full text-xs font-mono bg-slate-800 text-slate-300">
              {auditSummary.total_operations}
            </span>
          )}
        </button>
      </div>

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* TAB 1: USERS & CREDENTIALS                                            */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {activeTab === 'users' && (
        <div className="space-y-6">
          {/* Quick Stats Strip */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                Total Users
              </div>
              <div className="text-2xl font-bold text-slate-100 font-mono">{users.length}</div>
              <div className="text-xs text-slate-500 mt-1">Platform user accounts</div>
            </div>

            <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                Active Accounts
              </div>
              <div className="text-2xl font-bold text-emerald-400 font-mono">
                {users.filter((u) => u.is_active).length}
              </div>
              <div className="text-xs text-slate-500 mt-1">Allowed login access</div>
            </div>

            <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                Roles Defined
              </div>
              <div className="text-2xl font-bold text-indigo-400 font-mono">{roles.length}</div>
              <div className="text-xs text-slate-500 mt-1">Active role definitions</div>
            </div>

            <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                Active Admins
              </div>
              <div className="text-2xl font-bold text-amber-400 font-mono">{activeAdminsCount}</div>
              <div className="text-xs text-slate-500 mt-1">System administrator tier</div>
            </div>
          </div>

          {/* Filter Bar */}
          <div className="flex flex-col md:flex-row gap-3 items-stretch md:items-center justify-between p-3 rounded-xl bg-slate-900/60 border border-slate-800/80">
            <div className="relative flex-1 max-w-md">
              <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
              <input
                type="text"
                placeholder="Search by username, full name, or email..."
                value={userSearchTerm}
                onChange={(e) => setUserSearchTerm(e.target.value)}
                className="w-full pl-9 pr-4 py-2 bg-slate-800/80 border border-slate-700/80 rounded-lg text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-indigo-500"
              />
            </div>

            <div className="flex items-center gap-3">
              <select
                value={userRoleFilter}
                onChange={(e) => setUserRoleFilter(e.target.value)}
                className="px-3 py-2 bg-slate-800/80 border border-slate-700/80 rounded-lg text-xs text-slate-300 focus:outline-none focus:border-indigo-500"
              >
                <option value="all">All Roles</option>
                {roles.map((r) => (
                  <option key={r.role_id} value={r.role_name}>
                    {r.role_name}
                  </option>
                ))}
              </select>

              <select
                value={userStatusFilter}
                onChange={(e) => setUserStatusFilter(e.target.value)}
                className="px-3 py-2 bg-slate-800/80 border border-slate-700/80 rounded-lg text-xs text-slate-300 focus:outline-none focus:border-indigo-500"
              >
                <option value="all">All Status</option>
                <option value="active">Active Only</option>
                <option value="inactive">Inactive Only</option>
              </select>
            </div>
          </div>

          {/* Users Table */}
          <div className="rounded-xl border border-slate-800/80 bg-slate-900/40 backdrop-blur overflow-hidden shadow-sm">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead className="bg-[#0b101d] text-slate-400 uppercase font-semibold tracking-wider border-b border-slate-800/80">
                  <tr>
                    <th className="px-5 py-3.5">User & Profile</th>
                    <th className="px-5 py-3.5">Assigned Role</th>
                    <th className="px-5 py-3.5">Status</th>
                    <th className="px-5 py-3.5">Allowed Projects</th>
                    <th className="px-5 py-3.5">Last Login</th>
                    <th className="px-5 py-3.5 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/50">
                  {isLoadingUsers ? (
                    <tr>
                      <td colSpan={6} className="py-12 text-center text-slate-400">
                        <RefreshCw className="w-6 h-6 animate-spin mx-auto mb-2 text-indigo-400" />
                        <span>Loading user accounts...</span>
                      </td>
                    </tr>
                  ) : filteredUsers.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="py-12 text-center text-slate-400">
                        <Users className="w-8 h-8 mx-auto mb-2 opacity-40" />
                        <span>No users matching current filters.</span>
                      </td>
                    </tr>
                  ) : (
                    filteredUsers.map((u) => {
                      const isSelf = currentUser?.username === u.username;
                      const isProtectedAdmin = u.role === 'ADMIN' && activeAdminsCount <= 1;

                      return (
                        <tr key={u.user_id} className="hover:bg-slate-800/30 transition">
                          <td className="px-5 py-4">
                            <div className="flex items-center gap-3">
                              <div className="w-8 h-8 rounded-full bg-gradient-to-br from-indigo-500/20 to-purple-500/20 border border-indigo-500/30 flex items-center justify-center font-bold text-indigo-300">
                                {u.username.substring(0, 2).toUpperCase()}
                              </div>
                              <div>
                                <div className="font-semibold text-slate-200 flex items-center gap-1.5">
                                  <span>{u.full_name || u.username}</span>
                                  {isSelf && (
                                    <span className="px-1.5 py-0.5 rounded text-[10px] bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">
                                      You
                                    </span>
                                  )}
                                </div>
                                <div className="text-[11px] text-slate-400 font-mono flex items-center gap-2">
                                  <span>@{u.username}</span>
                                  {u.email && <span>• {u.email}</span>}
                                </div>
                              </div>
                            </div>
                          </td>

                          <td className="px-5 py-4">
                            <span
                              className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-semibold ${
                                u.role === 'ADMIN'
                                  ? 'bg-purple-950/60 text-purple-300 border border-purple-800/60'
                                  : u.role === 'VIEWER'
                                  ? 'bg-blue-950/60 text-blue-300 border border-blue-800/60'
                                  : 'bg-indigo-950/60 text-indigo-300 border border-indigo-800/60'
                              }`}
                            >
                              <Shield className="w-3 h-3" />
                              {u.role}
                            </span>
                          </td>

                          <td className="px-5 py-4">
                            {u.is_active ? (
                              <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-medium bg-emerald-950/50 text-emerald-300 border border-emerald-800/60">
                                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                                Active
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-medium bg-slate-800/80 text-slate-400 border border-slate-700/60">
                                <span className="w-1.5 h-1.5 rounded-full bg-slate-500" />
                                Inactive
                              </span>
                            )}
                          </td>

                          <td className="px-5 py-4">
                            <div className="flex flex-wrap gap-1 max-w-xs">
                              {u.allowed_projects && u.allowed_projects.length > 0 ? (
                                u.allowed_projects.map((p, idx) => (
                                  <span
                                    key={idx}
                                    className="px-2 py-0.5 rounded text-[10px] font-mono bg-slate-800 text-slate-300 border border-slate-700/50"
                                  >
                                    {p === '*' ? 'All Projects (*)' : p}
                                  </span>
                                ))
                              ) : (
                                <span className="text-slate-500 text-[11px]">No projects assigned</span>
                              )}
                            </div>
                          </td>

                          <td className="px-5 py-4 text-slate-400 font-mono text-[11px]">
                            {u.last_login ? (
                              <span title={u.last_login}>{u.last_login.split('T')[0]}</span>
                            ) : (
                              <span className="text-slate-500 italic">Never</span>
                            )}
                          </td>

                          <td className="px-5 py-4 text-right">
                            <div className="flex items-center justify-end gap-1.5">
                              <button
                                onClick={() => openEditUser(u)}
                                className="p-1.5 rounded-lg text-slate-400 hover:text-indigo-300 hover:bg-slate-800 transition"
                                title="Edit User Profile & Role"
                              >
                                <Edit3 className="w-4 h-4" />
                              </button>

                              <button
                                onClick={() => openResetPassword(u)}
                                className="p-1.5 rounded-lg text-slate-400 hover:text-amber-300 hover:bg-slate-800 transition"
                                title="Reset User Password"
                              >
                                <Key className="w-4 h-4" />
                              </button>

                              <button
                                onClick={() => {
                                  setDropUserTarget(u);
                                  setDropUserError(null);
                                }}
                                disabled={isSelf || isProtectedAdmin}
                                className={`p-1.5 rounded-lg transition ${
                                  isSelf || isProtectedAdmin
                                    ? 'text-slate-600 cursor-not-allowed'
                                    : 'text-slate-400 hover:text-rose-400 hover:bg-rose-500/10'
                                }`}
                                title={
                                  isSelf
                                    ? 'Cannot drop your own logged-in account'
                                    : isProtectedAdmin
                                    ? 'Cannot drop the last remaining system admin'
                                    : 'Drop User Account'
                                }
                              >
                                <Trash2 className="w-4 h-4" />
                              </button>
                            </div>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* TAB 2: ROLES & GRANULAR PERMISSIONS                                   */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {activeTab === 'roles' && (
        <div className="space-y-6">
          <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 flex items-start gap-3">
            <Info className="w-5 h-5 text-indigo-400 flex-shrink-0 mt-0.5" />
            <div className="text-xs text-slate-300 leading-relaxed">
              <span className="font-semibold text-slate-100">Granular Role-Based Access Control (RBAC):</span>{' '}
              Custom roles allow fine-grained access assignment across execution, metadata governance, observability,
              and integrations. Built-in system roles (<code className="text-indigo-300 font-mono">ADMIN</code>,{' '}
              <code className="text-blue-300 font-mono">VIEWER</code>) are protected to guarantee platform integrity.
            </div>
          </div>

          {/* Roles Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {isLoadingRoles ? (
              <div className="col-span-full py-12 text-center text-slate-400">
                <RefreshCw className="w-6 h-6 animate-spin mx-auto mb-2 text-indigo-400" />
                <span>Loading role catalog...</span>
              </div>
            ) : (
              roles.map((r) => {
                const isSystem = r.is_system_role;
                const canDrop = !isSystem && r.user_count === 0;

                return (
                  <div
                    key={r.role_id}
                    className="flex flex-col justify-between rounded-xl bg-slate-900/60 border border-slate-800/80 hover:border-slate-700/80 transition p-5 shadow-sm"
                  >
                    <div>
                      <div className="flex items-center justify-between gap-2 mb-2">
                        <div className="flex items-center gap-2">
                          <div className="w-8 h-8 rounded-lg bg-indigo-600/10 border border-indigo-500/20 flex items-center justify-center text-indigo-400">
                            <Shield className="w-4 h-4" />
                          </div>
                          <div>
                            <h3 className="font-bold text-slate-100 text-sm tracking-tight">{r.role_name}</h3>
                            <div className="text-[11px] text-slate-400 font-mono">
                              ID: {r.role_id} • {r.user_count} {r.user_count === 1 ? 'user' : 'users'} assigned
                            </div>
                          </div>
                        </div>

                        {isSystem ? (
                          <span className="px-2 py-0.5 rounded text-[10px] font-semibold bg-amber-500/15 text-amber-300 border border-amber-500/30">
                            System Role
                          </span>
                        ) : (
                          <span className="px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-800 text-slate-400 border border-slate-700/50">
                            Custom Role
                          </span>
                        )}
                      </div>

                      <p className="text-xs text-slate-400 line-clamp-2 mb-4 leading-relaxed">
                        {r.description || 'No description provided.'}
                      </p>

                      <div className="space-y-2 mb-4">
                        <div className="text-[11px] font-semibold uppercase tracking-wider text-slate-500 flex items-center justify-between">
                          <span>Granted Permissions</span>
                          <span className="font-mono text-slate-400">{r.permissions.length} total</span>
                        </div>

                        <div className="flex flex-wrap gap-1 max-h-36 overflow-y-auto pr-1">
                          {r.permissions.length === 0 ? (
                            <span className="text-slate-500 text-[11px] italic">No permissions assigned</span>
                          ) : (
                            r.permissions.map((p) => (
                              <span
                                key={p}
                                className="px-2 py-0.5 rounded text-[10px] font-mono bg-indigo-950/50 text-indigo-300 border border-indigo-800/50"
                                title={p}
                              >
                                {p}
                              </span>
                            ))
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="pt-4 border-t border-slate-800/80 flex items-center justify-between">
                      <button
                        onClick={() => openEditRole(r)}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-800/70 hover:bg-slate-800 text-slate-200 rounded-lg text-xs font-medium transition"
                      >
                        <Sliders className="w-3.5 h-3.5 text-indigo-400" />
                        <span>Configure Access</span>
                      </button>

                      <button
                        onClick={() => {
                          setDropRoleTarget(r);
                          setDropRoleError(null);
                        }}
                        disabled={!canDrop}
                        className={`p-1.5 rounded-lg transition ${
                          !canDrop
                            ? 'text-slate-600 cursor-not-allowed'
                            : 'text-slate-400 hover:text-rose-400 hover:bg-rose-500/10'
                        }`}
                        title={
                          isSystem
                            ? 'System roles cannot be dropped'
                            : r.user_count > 0
                            ? `Cannot drop role with ${r.user_count} active user(s) assigned`
                            : 'Drop Role'
                        }
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* TAB 3: USER ACTIVITY & FORENSIC AUDIT OBSERVER                        */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {activeTab === 'audit' && (
        <div className="space-y-6">
          {/* Summary KPIs Strip */}
          {auditSummary && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                  Total Operations
                </div>
                <div className="text-2xl font-bold text-slate-100 font-mono">
                  {auditSummary.total_operations}
                </div>
                <div className="text-xs text-slate-500 mt-1">Forensic event entries recorded</div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                  Active Operators
                </div>
                <div className="text-2xl font-bold text-cyan-400 font-mono">
                  {auditSummary.active_users_count}
                </div>
                <div className="text-xs text-slate-500 mt-1">Users operating on platform</div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                  Primary Operator
                </div>
                <div className="text-lg font-bold text-indigo-300 font-mono truncate">
                  {auditSummary.top_users[0]?.username ? `@${auditSummary.top_users[0].username}` : 'N/A'}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  {auditSummary.top_users[0]?.count || 0} recorded operations
                </div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 backdrop-blur">
                <div className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                  Top Action Executed
                </div>
                <div className="text-lg font-bold text-emerald-400 font-mono truncate">
                  {auditSummary.top_actions[0]?.action || 'N/A'}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  {auditSummary.top_actions[0]?.count || 0} occurrences
                </div>
              </div>
            </div>
          )}

          {/* Forensic Observer Filter Strip */}
          <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800/80 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-400">
                <Filter className="w-3.5 h-3.5 text-indigo-400" />
                <span>Forensic Activity Observer Filters</span>
              </div>
              {(auditUserFilter || auditActionFilter || auditStatusFilter) && (
                <button
                  onClick={() => {
                    setAuditUserFilter('');
                    setAuditActionFilter('');
                    setAuditStatusFilter('');
                  }}
                  className="text-xs text-indigo-400 hover:text-indigo-300 transition"
                >
                  Clear Filters
                </button>
              )}
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              {/* Observe Specific User */}
              <div>
                <label className="block text-[11px] font-medium text-slate-400 mb-1">
                  Observe Operator (User)
                </label>
                <select
                  value={auditUserFilter}
                  onChange={(e) => setAuditUserFilter(e.target.value)}
                  className="w-full px-3 py-2 bg-slate-800/80 border border-slate-700/80 rounded-lg text-xs text-slate-200 focus:outline-none focus:border-indigo-500"
                >
                  <option value="">All Users (Unfiltered)</option>
                  {users.map((u) => (
                    <option key={u.user_id} value={u.username}>
                      {u.username} ({u.full_name || u.role})
                    </option>
                  ))}
                </select>
              </div>

              {/* Action Filter */}
              <div>
                <label className="block text-[11px] font-medium text-slate-400 mb-1">Action Type</label>
                <select
                  value={auditActionFilter}
                  onChange={(e) => setAuditActionFilter(e.target.value)}
                  className="w-full px-3 py-2 bg-slate-800/80 border border-slate-700/80 rounded-lg text-xs text-slate-200 focus:outline-none focus:border-indigo-500"
                >
                  <option value="">All Actions</option>
                  <option value="LOGIN">LOGIN</option>
                  <option value="CREATE_USER">CREATE_USER</option>
                  <option value="UPDATE_USER">UPDATE_USER</option>
                  <option value="RESET_PASSWORD">RESET_PASSWORD</option>
                  <option value="DROP_USER">DROP_USER</option>
                  <option value="CREATE_ROLE">CREATE_ROLE</option>
                  <option value="UPDATE_ROLE">UPDATE_ROLE</option>
                  <option value="DROP_ROLE">DROP_ROLE</option>
                  <option value="TRIGGER_RUN">TRIGGER_RUN</option>
                  <option value="UPDATE_INTEGRATION">UPDATE_INTEGRATION</option>
                </select>
              </div>

              {/* Status Filter */}
              <div>
                <label className="block text-[11px] font-medium text-slate-400 mb-1">Execution Status</label>
                <select
                  value={auditStatusFilter}
                  onChange={(e) => setAuditStatusFilter(e.target.value)}
                  className="w-full px-3 py-2 bg-slate-800/80 border border-slate-700/80 rounded-lg text-xs text-slate-200 focus:outline-none focus:border-indigo-500"
                >
                  <option value="">All Statuses</option>
                  <option value="SUCCESS">SUCCESS</option>
                  <option value="DENIED">DENIED (Security Policy)</option>
                  <option value="ERROR">ERROR</option>
                </select>
              </div>
            </div>

            {auditUserFilter && (
              <div className="flex items-center gap-2 pt-2 border-t border-slate-800/50 text-xs text-cyan-300">
                <Eye className="w-3.5 h-3.5" />
                <span>
                  Observing actions specifically executed by operator <strong className="font-mono">@{auditUserFilter}</strong>.
                </span>
              </div>
            )}
          </div>

          {/* Audit Ledger Table */}
          <div className="rounded-xl border border-slate-800/80 bg-slate-900/40 backdrop-blur overflow-hidden shadow-sm">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead className="bg-[#0b101d] text-slate-400 uppercase font-semibold tracking-wider border-b border-slate-800/80">
                  <tr>
                    <th className="px-5 py-3.5">Timestamp</th>
                    <th className="px-5 py-3.5">Operator</th>
                    <th className="px-5 py-3.5">Action Executed</th>
                    <th className="px-5 py-3.5">Target Resource</th>
                    <th className="px-5 py-3.5">Status</th>
                    <th className="px-5 py-3.5">Client IP</th>
                    <th className="px-5 py-3.5 text-right">Payload & Diff</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/50">
                  {isLoadingAudit ? (
                    <tr>
                      <td colSpan={7} className="py-12 text-center text-slate-400">
                        <RefreshCw className="w-6 h-6 animate-spin mx-auto mb-2 text-indigo-400" />
                        <span>Querying forensic audit ledger...</span>
                      </td>
                    </tr>
                  ) : auditLogs.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="py-12 text-center text-slate-400">
                        <Activity className="w-8 h-8 mx-auto mb-2 opacity-40" />
                        <span>No audit records matching specified filters.</span>
                      </td>
                    </tr>
                  ) : (
                    auditLogs.map((item) => {
                      const isSuccess = item.status === 'SUCCESS';
                      const isDenied = item.status === 'DENIED';

                      return (
                        <tr key={item.audit_id} className="hover:bg-slate-800/30 transition">
                          <td className="px-5 py-3.5 text-slate-400 font-mono text-[11px] whitespace-nowrap">
                            <span title={item.event_ts || ''}>
                              {item.event_ts
                                ? item.event_ts.replace('T', ' ').substring(0, 19)
                                : 'Just now'}
                            </span>
                          </td>

                          <td className="px-5 py-3.5">
                            <div className="flex items-center gap-2">
                              <div className="w-6 h-6 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center font-bold text-[10px] text-slate-300">
                                {item.username.substring(0, 2).toUpperCase()}
                              </div>
                              <span className="font-semibold text-slate-200 font-mono">
                                @{item.username}
                              </span>
                            </div>
                          </td>

                          <td className="px-5 py-3.5">
                            <span
                              className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-mono font-semibold ${
                                item.action.includes('DROP') || item.action.includes('DELETE')
                                  ? 'bg-rose-950/60 text-rose-300 border border-rose-800/60'
                                  : item.action.includes('CREATE')
                                  ? 'bg-emerald-950/60 text-emerald-300 border border-emerald-800/60'
                                  : item.action.includes('UPDATE') || item.action.includes('RESET')
                                  ? 'bg-amber-950/60 text-amber-300 border border-amber-800/60'
                                  : 'bg-indigo-950/60 text-indigo-300 border border-indigo-800/60'
                              }`}
                            >
                              {item.action}
                            </span>
                          </td>

                          <td className="px-5 py-3.5">
                            <div className="font-mono text-slate-300 text-[11px]">
                              <span className="text-slate-500 mr-1">{item.resource_type}:</span>
                              <span>{item.resource_id}</span>
                            </div>
                          </td>

                          <td className="px-5 py-3.5">
                            {isSuccess ? (
                              <span className="inline-flex items-center gap-1 text-emerald-400 text-[11px] font-medium">
                                <CheckCircle2 className="w-3.5 h-3.5" />
                                <span>SUCCESS</span>
                              </span>
                            ) : isDenied ? (
                              <span className="inline-flex items-center gap-1 text-rose-400 text-[11px] font-medium">
                                <Lock className="w-3.5 h-3.5" />
                                <span>DENIED</span>
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1 text-amber-400 text-[11px] font-medium">
                                <AlertTriangle className="w-3.5 h-3.5" />
                                <span>{item.status}</span>
                              </span>
                            )}
                          </td>

                          <td className="px-5 py-3.5 text-slate-400 font-mono text-[11px]">
                            {item.client_ip || 'UNKNOWN'}
                          </td>

                          <td className="px-5 py-3.5 text-right">
                            <button
                              onClick={() => setInspectedAuditItem(item)}
                              className="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-slate-800/80 hover:bg-slate-700 text-slate-300 text-xs font-medium transition border border-slate-700/60"
                            >
                              <Search className="w-3 h-3 text-indigo-400" />
                              <span>Inspect Diff</span>
                            </button>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 1: CREATE USER                                                  */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {isCreateUserOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-lg bg-[#0e1424] border border-slate-800 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="p-5 border-b border-slate-800/80 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
                  <UserPlus className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">Create New Platform User</h3>
                  <p className="text-xs text-slate-400">Provision credentials and assign access role</p>
                </div>
              </div>
              <button
                onClick={() => setIsCreateUserOpen(false)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <form
              onSubmit={(e) => {
                e.preventDefault();
                setCreateUserError(null);
                if (!newUserUsername.trim() || !newUserPassword.trim()) {
                  setCreateUserError('Username and password are required.');
                  return;
                }
                const projectsArr = newUserProjects
                  .split(',')
                  .map((p) => p.trim())
                  .filter(Boolean);

                createUserMutation.mutate({
                  username: newUserUsername.trim(),
                  password: newUserPassword,
                  role_id: newUserRoleId,
                  full_name: newUserFullName.trim() || undefined,
                  email: newUserEmail.trim() || undefined,
                  allowed_projects: projectsArr.length > 0 ? projectsArr : ['*'],
                });
              }}
              className="p-5 space-y-4 text-xs"
            >
              {createUserError && (
                <div className="p-3 rounded-lg bg-rose-950/50 border border-rose-800/60 text-rose-300 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{createUserError}</span>
                </div>
              )}

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-medium mb-1">
                    Username <span className="text-rose-400">*</span>
                  </label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. data_eng_1"
                    value={newUserUsername}
                    onChange={(e) => setNewUserUsername(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500 font-mono"
                  />
                </div>

                <div>
                  <label className="block text-slate-400 font-medium mb-1">
                    Initial Password <span className="text-rose-400">*</span>
                  </label>
                  <div className="relative">
                    <input
                      type={showNewUserPassword ? 'text' : 'password'}
                      required
                      placeholder="Min. 6 characters"
                      value={newUserPassword}
                      onChange={(e) => setNewUserPassword(e.target.value)}
                      className="w-full px-3 py-2 pr-9 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500 font-mono"
                    />
                    <button
                      type="button"
                      onClick={() => setShowNewUserPassword(!showNewUserPassword)}
                      className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200"
                    >
                      {showNewUserPassword ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
                    </button>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-medium mb-1">Full Name</label>
                  <input
                    type="text"
                    placeholder="e.g. Sarah Connor"
                    value={newUserFullName}
                    onChange={(e) => setNewUserFullName(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  />
                </div>

                <div>
                  <label className="block text-slate-400 font-medium mb-1">Email Address</label>
                  <input
                    type="email"
                    placeholder="sarah@lakehouse.io"
                    value={newUserEmail}
                    onChange={(e) => setNewUserEmail(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-medium mb-1">
                    Access Role <span className="text-rose-400">*</span>
                  </label>
                  <select
                    value={newUserRoleId}
                    onChange={(e) => setNewUserRoleId(Number(e.target.value))}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  >
                    {roles.map((r) => (
                      <option key={r.role_id} value={r.role_id}>
                        {r.role_name} {r.is_system_role ? '(System)' : ''}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-slate-400 font-medium mb-1">
                    Allowed Projects <span className="text-slate-500">(comma-separated)</span>
                  </label>
                  <input
                    type="text"
                    placeholder="* or school_system, land_prod"
                    value={newUserProjects}
                    onChange={(e) => setNewUserProjects(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500 font-mono"
                  />
                </div>
              </div>

              <div className="pt-4 border-t border-slate-800/80 flex items-center justify-end gap-3">
                <button
                  type="button"
                  onClick={() => setIsCreateUserOpen(false)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={createUserMutation.isPending}
                  className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg font-semibold shadow-sm transition disabled:opacity-50"
                >
                  {createUserMutation.isPending && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                  <span>Create User Account</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 2: EDIT USER                                                    */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {editingUser && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-lg bg-[#0e1424] border border-slate-800 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="p-5 border-b border-slate-800/80 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
                  <Edit3 className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">Edit User: @{editingUser.username}</h3>
                  <p className="text-xs text-slate-400">Modify profile metadata, role assignment, and active status</p>
                </div>
              </div>
              <button
                onClick={() => setEditingUser(null)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <form
              onSubmit={(e) => {
                e.preventDefault();
                setEditUserError(null);
                const projectsArr = editProjects
                  .split(',')
                  .map((p) => p.trim())
                  .filter(Boolean);

                updateUserMutation.mutate({
                  userId: editingUser.user_id,
                  payload: {
                    role_id: editRoleId,
                    is_active: editIsActive,
                    full_name: editFullName.trim(),
                    email: editEmail.trim(),
                    allowed_projects: projectsArr.length > 0 ? projectsArr : ['*'],
                  },
                });
              }}
              className="p-5 space-y-4 text-xs"
            >
              {editUserError && (
                <div className="p-3 rounded-lg bg-rose-950/50 border border-rose-800/60 text-rose-300 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{editUserError}</span>
                </div>
              )}

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-medium mb-1">Full Name</label>
                  <input
                    type="text"
                    value={editFullName}
                    onChange={(e) => setEditFullName(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  />
                </div>

                <div>
                  <label className="block text-slate-400 font-medium mb-1">Email Address</label>
                  <input
                    type="email"
                    value={editEmail}
                    onChange={(e) => setEditEmail(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-medium mb-1">Access Role</label>
                  <select
                    value={editRoleId}
                    onChange={(e) => setEditRoleId(Number(e.target.value))}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  >
                    {roles.map((r) => (
                      <option key={r.role_id} value={r.role_id}>
                        {r.role_name} {r.is_system_role ? '(System)' : ''}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-slate-400 font-medium mb-1">Account Status</label>
                  <div className="flex items-center gap-3 h-9">
                    <button
                      type="button"
                      onClick={() => setEditIsActive(true)}
                      className={`flex-1 py-1.5 rounded-lg text-xs font-semibold border transition ${
                        editIsActive
                          ? 'bg-emerald-950/60 text-emerald-300 border-emerald-700/60'
                          : 'bg-slate-900 text-slate-400 border-slate-800 hover:text-slate-200'
                      }`}
                    >
                      Active
                    </button>
                    <button
                      type="button"
                      onClick={() => setEditIsActive(false)}
                      className={`flex-1 py-1.5 rounded-lg text-xs font-semibold border transition ${
                        !editIsActive
                          ? 'bg-rose-950/60 text-rose-300 border-rose-700/60'
                          : 'bg-slate-900 text-slate-400 border-slate-800 hover:text-slate-200'
                      }`}
                    >
                      Inactive
                    </button>
                  </div>
                </div>
              </div>

              <div>
                <label className="block text-slate-400 font-medium mb-1">
                  Allowed Projects <span className="text-slate-500">(comma-separated)</span>
                </label>
                <input
                  type="text"
                  value={editProjects}
                  onChange={(e) => setEditProjects(e.target.value)}
                  className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500 font-mono"
                />
              </div>

              <div className="pt-4 border-t border-slate-800/80 flex items-center justify-end gap-3">
                <button
                  type="button"
                  onClick={() => setEditingUser(null)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={updateUserMutation.isPending}
                  className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg font-semibold shadow-sm transition disabled:opacity-50"
                >
                  {updateUserMutation.isPending && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                  <span>Save Changes</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 3: RESET PASSWORD                                               */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {resetUser && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-md bg-[#0e1424] border border-slate-800 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="p-5 border-b border-slate-800/80 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-amber-500/20 border border-amber-500/30 flex items-center justify-center text-amber-400">
                  <Key className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">Reset Password</h3>
                  <p className="text-xs text-slate-400">Target user: @{resetUser.username}</p>
                </div>
              </div>
              <button
                onClick={() => setResetUser(null)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <form
              onSubmit={(e) => {
                e.preventDefault();
                setResetPasswordError(null);
                if (resetNewPassword.length < 6) {
                  setResetPasswordError('Password must be at least 6 characters long.');
                  return;
                }
                resetPasswordMutation.mutate({
                  userId: resetUser.user_id,
                  pass: resetNewPassword,
                });
              }}
              className="p-5 space-y-4 text-xs"
            >
              {resetPasswordError && (
                <div className="p-3 rounded-lg bg-rose-950/50 border border-rose-800/60 text-rose-300 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{resetPasswordError}</span>
                </div>
              )}

              {resetSuccessMsg && (
                <div className="p-3 rounded-lg bg-emerald-950/50 border border-emerald-800/60 text-emerald-300 flex items-start gap-2">
                  <CheckCircle2 className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{resetSuccessMsg}</span>
                </div>
              )}

              <div>
                <label className="block text-slate-400 font-medium mb-1">
                  New Password <span className="text-rose-400">*</span>
                </label>
                <div className="relative">
                  <input
                    type={showResetPassword ? 'text' : 'password'}
                    required
                    placeholder="Enter new secure password (min. 6 chars)"
                    value={resetNewPassword}
                    onChange={(e) => setResetNewPassword(e.target.value)}
                    className="w-full px-3 py-2 pr-9 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-amber-500 font-mono"
                  />
                  <button
                    type="button"
                    onClick={() => setShowResetPassword(!showResetPassword)}
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-200"
                  >
                    {showResetPassword ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
                  </button>
                </div>
              </div>

              <div className="p-3 rounded-lg bg-slate-900/60 border border-slate-800 text-[11px] text-slate-400 leading-relaxed">
                Password will be securely hashed with bcrypt using high-work-factor salt. Active sessions
                will require re-authentication.
              </div>

              <div className="pt-4 border-t border-slate-800/80 flex items-center justify-end gap-3">
                <button
                  type="button"
                  onClick={() => setResetUser(null)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={resetPasswordMutation.isPending}
                  className="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-500 text-white rounded-lg font-semibold shadow-sm transition disabled:opacity-50"
                >
                  {resetPasswordMutation.isPending && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                  <span>Update Password</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 4: DROP USER CONFIRMATION                                       */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {dropUserTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-md bg-[#0e1424] border border-rose-900/50 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="p-5 border-b border-rose-900/30 flex items-center justify-between bg-rose-950/20">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-rose-500/20 border border-rose-500/30 flex items-center justify-center text-rose-400">
                  <UserX className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">Drop Platform User</h3>
                  <p className="text-xs text-rose-300">Irreversible credential deletion</p>
                </div>
              </div>
              <button
                onClick={() => setDropUserTarget(null)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="p-5 space-y-4 text-xs">
              {dropUserError && (
                <div className="p-3 rounded-lg bg-rose-950/50 border border-rose-800/60 text-rose-300 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{dropUserError}</span>
                </div>
              )}

              <p className="text-slate-300 leading-relaxed">
                Are you sure you want to drop user{' '}
                <strong className="text-rose-300 font-mono">@{dropUserTarget.username}</strong> ({dropUserTarget.full_name || dropUserTarget.role})?
              </p>

              <div className="p-3 rounded-lg bg-rose-950/30 border border-rose-900/40 text-[11px] text-rose-200/90 leading-relaxed space-y-1">
                <div>• User credentials and role bindings will be permanently deleted.</div>
                <div>• All active session tokens will be immediately invalidated.</div>
                <div>• Forensic activity audit trail will remain preserved in metadata ledger.</div>
              </div>

              <div className="pt-4 border-t border-slate-800/80 flex items-center justify-end gap-3">
                <button
                  type="button"
                  onClick={() => setDropUserTarget(null)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  disabled={dropUserMutation.isPending}
                  onClick={() => dropUserMutation.mutate(dropUserTarget.user_id)}
                  className="flex items-center gap-2 px-4 py-2 bg-rose-600 hover:bg-rose-500 text-white rounded-lg font-semibold shadow-sm transition disabled:opacity-50"
                >
                  {dropUserMutation.isPending && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                  <span>Drop User Permanently</span>
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 5: CREATE ROLE                                                  */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {isCreateRoleOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-2xl bg-[#0e1424] border border-slate-800 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150 max-h-[90vh] flex flex-col">
            <div className="p-5 border-b border-slate-800/80 flex items-center justify-between flex-shrink-0">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
                  <ShieldCheck className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">Create Custom Role</h3>
                  <p className="text-xs text-slate-400">Define role identifier and manual access permissions</p>
                </div>
              </div>
              <button
                onClick={() => setIsCreateRoleOpen(false)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <form
              onSubmit={(e) => {
                e.preventDefault();
                setCreateRoleError(null);
                if (!newRoleName.trim()) {
                  setCreateRoleError('Role name is required.');
                  return;
                }
                createRoleMutation.mutate({
                  role_name: newRoleName.trim().toUpperCase(),
                  description: newRoleDesc.trim() || undefined,
                  permissions: Array.from(selectedPermissions),
                });
              }}
              className="p-5 space-y-4 text-xs overflow-y-auto flex-1"
            >
              {createRoleError && (
                <div className="p-3 rounded-lg bg-rose-950/50 border border-rose-800/60 text-rose-300 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{createRoleError}</span>
                </div>
              )}

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-medium mb-1">
                    Role Name <span className="text-rose-400">*</span>
                  </label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. DATA_STEWARD"
                    value={newRoleName}
                    onChange={(e) => setNewRoleName(e.target.value.toUpperCase())}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500 font-mono uppercase"
                  />
                </div>

                <div>
                  <label className="block text-slate-400 font-medium mb-1">Description</label>
                  <input
                    type="text"
                    placeholder="e.g. Can trigger pipelines and edit metadata"
                    value={newRoleDesc}
                    onChange={(e) => setNewRoleDesc(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>

              {/* Granular Permissions Picker */}
              <div className="space-y-3 pt-2">
                <div className="flex items-center justify-between pb-2 border-b border-slate-800/80">
                  <div className="text-xs font-semibold uppercase tracking-wider text-slate-300">
                    Granular Access Permissions ({selectedPermissions.size} of {permissions.length} selected)
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() => {
                        const allIds = new Set(permissions.map((p) => p.id));
                        setSelectedPermissions(allIds);
                      }}
                      className="text-xs text-indigo-400 hover:text-indigo-300 font-medium"
                    >
                      Select All
                    </button>
                    <span className="text-slate-600">•</span>
                    <button
                      type="button"
                      onClick={() => setSelectedPermissions(new Set())}
                      className="text-xs text-slate-400 hover:text-slate-300 font-medium"
                    >
                      Clear All
                    </button>
                  </div>
                </div>

                {isLoadingPermissions ? (
                  <div className="py-6 text-center text-slate-400">Loading system permissions...</div>
                ) : (
                  Object.entries(permissionsByCategory).map(([category, items]) => {
                    const allCatSelected = items.every((i) => selectedPermissions.has(i.id));

                    return (
                      <div key={category} className="rounded-lg bg-slate-900/60 border border-slate-800/70 p-3 space-y-2">
                        <div className="flex items-center justify-between pb-1.5 border-b border-slate-800/50">
                          <span className="font-bold text-slate-200 text-xs flex items-center gap-1.5">
                            <span className="w-2 h-2 rounded-full bg-indigo-500" />
                            {category}
                          </span>
                          <button
                            type="button"
                            onClick={() => {
                              const next = new Set(selectedPermissions);
                              if (allCatSelected) {
                                for (const it of items) next.delete(it.id);
                              } else {
                                for (const it of items) next.add(it.id);
                              }
                              setSelectedPermissions(next);
                            }}
                            className="text-[11px] text-slate-400 hover:text-indigo-300"
                          >
                            {allCatSelected ? 'Deselect Category' : 'Select Category'}
                          </button>
                        </div>

                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 pt-1">
                          {items.map((perm) => {
                            const isChecked = selectedPermissions.has(perm.id);
                            return (
                              <label
                                key={perm.id}
                                className={`flex items-start gap-2.5 p-2 rounded-md border transition cursor-pointer select-none ${
                                  isChecked
                                    ? 'bg-indigo-950/40 border-indigo-700/60 text-slate-100'
                                    : 'bg-slate-950/40 border-slate-800/60 text-slate-400 hover:border-slate-700'
                                }`}
                              >
                                <input
                                  type="checkbox"
                                  checked={isChecked}
                                  onChange={() => {
                                    const next = new Set(selectedPermissions);
                                    if (next.has(perm.id)) next.delete(perm.id);
                                    else next.add(perm.id);
                                    setSelectedPermissions(next);
                                  }}
                                  className="mt-0.5 rounded border-slate-700 text-indigo-600 focus:ring-0 focus:ring-offset-0"
                                />
                                <div className="min-w-0">
                                  <div className="font-mono font-semibold text-[11px]">{perm.id}</div>
                                  <div className="text-[10px] text-slate-500 leading-tight">{perm.description}</div>
                                </div>
                              </label>
                            );
                          })}
                        </div>
                      </div>
                    );
                  })
                )}
              </div>

              <div className="pt-4 border-t border-slate-800/80 flex items-center justify-end gap-3 flex-shrink-0">
                <button
                  type="button"
                  onClick={() => setIsCreateRoleOpen(false)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={createRoleMutation.isPending}
                  className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg font-semibold shadow-sm transition disabled:opacity-50"
                >
                  {createRoleMutation.isPending && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                  <span>Save Role Definition</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 6: EDIT ROLE                                                    */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {editingRole && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-2xl bg-[#0e1424] border border-slate-800 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150 max-h-[90vh] flex flex-col">
            <div className="p-5 border-b border-slate-800/80 flex items-center justify-between flex-shrink-0">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
                  <Sliders className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">Configure Role: {editingRole.role_name}</h3>
                  <p className="text-xs text-slate-400">
                    {editingRole.is_system_role ? 'System Protected Role' : 'Custom Platform Role'}
                  </p>
                </div>
              </div>
              <button
                onClick={() => setEditingRole(null)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <form
              onSubmit={(e) => {
                e.preventDefault();
                setEditRoleError(null);
                updateRoleMutation.mutate({
                  roleId: editingRole.role_id,
                  payload: {
                    role_name: editingRole.is_system_role ? undefined : editRoleName.trim().toUpperCase(),
                    description: editRoleDesc.trim(),
                    permissions: Array.from(editRolePermissions),
                  },
                });
              }}
              className="p-5 space-y-4 text-xs overflow-y-auto flex-1"
            >
              {editRoleError && (
                <div className="p-3 rounded-lg bg-rose-950/50 border border-rose-800/60 text-rose-300 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{editRoleError}</span>
                </div>
              )}

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 font-medium mb-1">Role Name</label>
                  <input
                    type="text"
                    disabled={editingRole.is_system_role}
                    value={editRoleName}
                    onChange={(e) => setEditRoleName(e.target.value.toUpperCase())}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500 font-mono uppercase disabled:opacity-60 disabled:cursor-not-allowed"
                  />
                </div>

                <div>
                  <label className="block text-slate-400 font-medium mb-1">Description</label>
                  <input
                    type="text"
                    value={editRoleDesc}
                    onChange={(e) => setEditRoleDesc(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-slate-200 focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>

              {/* Granular Permissions Picker */}
              <div className="space-y-3 pt-2">
                <div className="flex items-center justify-between pb-2 border-b border-slate-800/80">
                  <div className="text-xs font-semibold uppercase tracking-wider text-slate-300">
                    Assigned Permissions ({editRolePermissions.size} of {permissions.length} selected)
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() => {
                        const allIds = new Set(permissions.map((p) => p.id));
                        setEditRolePermissions(allIds);
                      }}
                      className="text-xs text-indigo-400 hover:text-indigo-300 font-medium"
                    >
                      Select All
                    </button>
                    <span className="text-slate-600">•</span>
                    <button
                      type="button"
                      onClick={() => setEditRolePermissions(new Set())}
                      className="text-xs text-slate-400 hover:text-slate-300 font-medium"
                    >
                      Clear All
                    </button>
                  </div>
                </div>

                {Object.entries(permissionsByCategory).map(([category, items]) => {
                  const allCatSelected = items.every((i) => editRolePermissions.has(i.id));

                  return (
                    <div key={category} className="rounded-lg bg-slate-900/60 border border-slate-800/70 p-3 space-y-2">
                      <div className="flex items-center justify-between pb-1.5 border-b border-slate-800/50">
                        <span className="font-bold text-slate-200 text-xs flex items-center gap-1.5">
                          <span className="w-2 h-2 rounded-full bg-indigo-500" />
                          {category}
                        </span>
                        <button
                          type="button"
                          onClick={() => {
                            const next = new Set(editRolePermissions);
                            if (allCatSelected) {
                              for (const it of items) next.delete(it.id);
                            } else {
                              for (const it of items) next.add(it.id);
                            }
                            setEditRolePermissions(next);
                          }}
                          className="text-[11px] text-slate-400 hover:text-indigo-300"
                        >
                          {allCatSelected ? 'Deselect Category' : 'Select Category'}
                        </button>
                      </div>

                      <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 pt-1">
                        {items.map((perm) => {
                          const isChecked = editRolePermissions.has(perm.id);
                          return (
                            <label
                              key={perm.id}
                              className={`flex items-start gap-2.5 p-2 rounded-md border transition cursor-pointer select-none ${
                                isChecked
                                  ? 'bg-indigo-950/40 border-indigo-700/60 text-slate-100'
                                  : 'bg-slate-950/40 border-slate-800/60 text-slate-400 hover:border-slate-700'
                              }`}
                            >
                              <input
                                type="checkbox"
                                checked={isChecked}
                                onChange={() => {
                                  const next = new Set(editRolePermissions);
                                  if (next.has(perm.id)) next.delete(perm.id);
                                  else next.add(perm.id);
                                  setEditRolePermissions(next);
                                }}
                                className="mt-0.5 rounded border-slate-700 text-indigo-600 focus:ring-0 focus:ring-offset-0"
                              />
                              <div className="min-w-0">
                                <div className="font-mono font-semibold text-[11px]">{perm.id}</div>
                                <div className="text-[10px] text-slate-500 leading-tight">{perm.description}</div>
                              </div>
                            </label>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>

              <div className="pt-4 border-t border-slate-800/80 flex items-center justify-end gap-3 flex-shrink-0">
                <button
                  type="button"
                  onClick={() => setEditingRole(null)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={updateRoleMutation.isPending}
                  className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg font-semibold shadow-sm transition disabled:opacity-50"
                >
                  {updateRoleMutation.isPending && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                  <span>Save Role Permissions</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 7: DROP ROLE CONFIRMATION                                       */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {dropRoleTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-md bg-[#0e1424] border border-rose-900/50 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="p-5 border-b border-rose-900/30 flex items-center justify-between bg-rose-950/20">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-rose-500/20 border border-rose-500/30 flex items-center justify-center text-rose-400">
                  <Trash2 className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">Drop Custom Role</h3>
                  <p className="text-xs text-rose-300">Irreversible role deletion</p>
                </div>
              </div>
              <button
                onClick={() => setDropRoleTarget(null)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="p-5 space-y-4 text-xs">
              {dropRoleError && (
                <div className="p-3 rounded-lg bg-rose-950/50 border border-rose-800/60 text-rose-300 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  <span>{dropRoleError}</span>
                </div>
              )}

              <p className="text-slate-300 leading-relaxed">
                Are you sure you want to drop role{' '}
                <strong className="text-rose-300 font-mono">{dropRoleTarget.role_name}</strong>?
              </p>

              <div className="p-3 rounded-lg bg-rose-950/30 border border-rose-900/40 text-[11px] text-rose-200/90 leading-relaxed">
                Role and its associated permission mappings will be permanently removed.
              </div>

              <div className="pt-4 border-t border-slate-800/80 flex items-center justify-end gap-3">
                <button
                  type="button"
                  onClick={() => setDropRoleTarget(null)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  disabled={dropRoleMutation.isPending}
                  onClick={() => dropRoleMutation.mutate(dropRoleTarget.role_id)}
                  className="flex items-center gap-2 px-4 py-2 bg-rose-600 hover:bg-rose-500 text-white rounded-lg font-semibold shadow-sm transition disabled:opacity-50"
                >
                  {dropRoleMutation.isPending && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                  <span>Drop Role Permanently</span>
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ═════════════════════════════════════════════════════════════════════ */}
      {/* MODAL 8: AUDIT EVENT DIFF INSPECTOR                                   */}
      {/* ═════════════════════════════════════════════════════════════════════ */}
      {inspectedAuditItem && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="w-full max-w-2xl bg-[#0e1424] border border-slate-800 rounded-2xl shadow-2xl overflow-hidden animate-in fade-in zoom-in-95 duration-150 max-h-[85vh] flex flex-col">
            <div className="p-5 border-b border-slate-800/80 flex items-center justify-between flex-shrink-0">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-cyan-600/20 border border-cyan-500/30 flex items-center justify-center text-cyan-400">
                  <Terminal className="w-4 h-4" />
                </div>
                <div>
                  <h3 className="font-bold text-slate-100 text-sm">
                    Forensic Operation Inspector #{inspectedAuditItem.audit_id}
                  </h3>
                  <p className="text-xs text-slate-400">
                    Operator @{inspectedAuditItem.username} • {inspectedAuditItem.action} • {inspectedAuditItem.resource_type}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() =>
                    handleCopyDiff(
                      JSON.stringify(
                        {
                          audit_id: inspectedAuditItem.audit_id,
                          event_ts: inspectedAuditItem.event_ts,
                          operator: inspectedAuditItem.username,
                          action: inspectedAuditItem.action,
                          resource_type: inspectedAuditItem.resource_type,
                          resource_id: inspectedAuditItem.resource_id,
                          status: inspectedAuditItem.status,
                          client_ip: inspectedAuditItem.client_ip,
                          old_value: inspectedAuditItem.old_value,
                          new_value: inspectedAuditItem.new_value,
                        },
                        null,
                        2
                      )
                    )
                  }
                  className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs transition"
                  title="Copy full JSON audit event"
                >
                  {isCopiedDiff ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
                  <span>{isCopiedDiff ? 'Copied' : 'Copy Event'}</span>
                </button>

                <button
                  onClick={() => setInspectedAuditItem(null)}
                  className="p-1 rounded-lg text-slate-400 hover:text-slate-200 hover:bg-slate-800 transition"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            </div>

            <div className="p-5 space-y-4 text-xs overflow-y-auto flex-1">
              {/* Event Metadata */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 p-3 rounded-xl bg-slate-900/60 border border-slate-800/80">
                <div>
                  <div className="text-[10px] uppercase font-semibold text-slate-500">Timestamp</div>
                  <div className="font-mono text-slate-300 text-[11px] truncate">
                    {inspectedAuditItem.event_ts || 'N/A'}
                  </div>
                </div>
                <div>
                  <div className="text-[10px] uppercase font-semibold text-slate-500">Operator</div>
                  <div className="font-mono text-indigo-300 text-[11px]">
                    @{inspectedAuditItem.username}
                  </div>
                </div>
                <div>
                  <div className="text-[10px] uppercase font-semibold text-slate-500">Client IP</div>
                  <div className="font-mono text-slate-300 text-[11px]">
                    {inspectedAuditItem.client_ip || 'UNKNOWN'}
                  </div>
                </div>
                <div>
                  <div className="text-[10px] uppercase font-semibold text-slate-500">Status</div>
                  <div className="font-mono font-bold text-emerald-400 text-[11px]">
                    {inspectedAuditItem.status}
                  </div>
                </div>
              </div>

              {/* State Diff / Payload */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {/* Old State */}
                <div className="space-y-1.5">
                  <div className="font-semibold text-slate-400 uppercase tracking-wider text-[10px] flex items-center justify-between">
                    <span>Pre-Operation State (old_value)</span>
                  </div>
                  <div className="p-3 rounded-lg bg-slate-950 border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto max-h-64 whitespace-pre-wrap">
                    {inspectedAuditItem.old_value ? (
                      JSON.stringify(inspectedAuditItem.old_value, null, 2)
                    ) : (
                      <span className="text-slate-600 italic">No prior state (Entity created)</span>
                    )}
                  </div>
                </div>

                {/* New State */}
                <div className="space-y-1.5">
                  <div className="font-semibold text-slate-400 uppercase tracking-wider text-[10px] flex items-center justify-between">
                    <span>Post-Operation State (new_value)</span>
                  </div>
                  <div className="p-3 rounded-lg bg-slate-950 border border-slate-800 font-mono text-[11px] text-emerald-300 overflow-x-auto max-h-64 whitespace-pre-wrap">
                    {inspectedAuditItem.new_value ? (
                      JSON.stringify(inspectedAuditItem.new_value, null, 2)
                    ) : (
                      <span className="text-slate-600 italic">No post state (Entity dropped)</span>
                    )}
                  </div>
                </div>
              </div>
            </div>

            <div className="p-4 border-t border-slate-800/80 flex items-center justify-end flex-shrink-0 bg-slate-900/30">
              <button
                type="button"
                onClick={() => setInspectedAuditItem(null)}
                className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg font-medium transition text-xs"
              >
                Close Inspector
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
