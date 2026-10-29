// frontend/src/views/IntegrationsView.tsx
import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { api } from '../api/client';
import type {
  IntegrationDetail,
  ConnectionCreateRequest,
  ConnectionTestResult,
} from '../api/types';
import {
  Network,
  Database,
  Search,
  RefreshCw,
  CheckCircle2,
  AlertTriangle,
  Server,
  ArrowRight,
  Shield,
  Zap,
  Plus,
  X,
  Loader2,
  Lock,
  Info,
} from 'lucide-react';

interface IntegrationsViewProps {
  onNavigateToPipelines?: (sourceTag?: string) => void;
}

export const IntegrationsView: React.FC<IntegrationsViewProps> = ({ onNavigateToPipelines }) => {
  const { projectScope } = useAuth();
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState<'all' | 'sources' | 'targets'>('all');

  // Modal & Form State
  const [isAddModalOpen, setIsAddModalOpen] = useState(false);
  const [formData, setFormData] = useState<ConnectionCreateRequest>({
    tag: '',
    supports_source: true,
    supports_target: false,
    environment: 'PROD',
    db_type: 'mysql',
    server: '',
    port: 3306,
    database_name: '',
    schema_name: '',
    username: '',
    credential_reference: '',
    service_name: '',
    purpose: '',
    owner: 'Admin',
    team: 'Data Engineering',
  });
  const [isTesting, setIsTesting] = useState(false);
  const [testResult, setTestResult] = useState<ConnectionTestResult | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saveSuccess, setSaveSuccess] = useState<string | null>(null);

  const {
    data: integrationsData,
    isLoading,
    refetch,
    isFetching,
  } = useQuery({
    queryKey: ['integrations', projectScope],
    queryFn: () => api.getIntegrations(projectScope),
    refetchInterval: 30000,
  });

  const integrations = integrationsData?.integrations || [];

  const filtered = integrations.filter((item: IntegrationDetail) => {
    // Type filter
    if (filterType === 'sources' && !item.supports_source) return false;
    if (filterType === 'targets' && !item.supports_target) return false;

    // Search filter
    if (!searchTerm) return true;
    const term = searchTerm.toLowerCase();
    return (
      item.tag.toLowerCase().includes(term) ||
      item.db_type.toLowerCase().includes(term) ||
      item.server.toLowerCase().includes(term) ||
      item.database_name.toLowerCase().includes(term) ||
      (item.purpose && item.purpose.toLowerCase().includes(term))
    );
  });

  const handleDbTypeChange = (newType: string) => {
    let defaultPort = 3306;
    if (newType === 'postgres') defaultPort = 5432;
    if (newType === 'oracle') defaultPort = 1521;
    if (newType === 'sqlserver') defaultPort = 1433;

    setFormData((prev) => ({
      ...prev,
      db_type: newType,
      port: defaultPort,
    }));
    setTestResult(null);
  };

  const handleTestConnection = async () => {
    if (!formData.tag || !formData.server || !formData.database_name || !formData.credential_reference) {
      setTestResult({
        status: 'FAILED',
        latency_ms: 0,
        error: 'Please fill in Tag, Server, Database, Username, and Credential Reference before testing.',
      });
      return;
    }

    setIsTesting(true);
    setTestResult(null);
    setSaveError(null);
    try {
      const res = await api.testConnection(formData, projectScope);
      setTestResult(res);
    } catch (err: any) {
      setTestResult({
        status: 'FAILED',
        latency_ms: 0,
        error: err.message || 'Endpoint connection probe failed.',
      });
    } finally {
      setIsTesting(false);
    }
  };

  const handleSaveConnection = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.tag || !formData.server || !formData.database_name || !formData.credential_reference) {
      setSaveError('All mandatory fields (Tag, Server, Database, Username, Credential Reference) are required.');
      return;
    }

    setIsSaving(true);
    setSaveError(null);
    setSaveSuccess(null);
    try {
      await api.saveConnection(formData, projectScope);
      setSaveSuccess(`Connection '${formData.tag}' successfully onboarded to metadata catalog!`);
      refetch();
      setTimeout(() => {
        setIsAddModalOpen(false);
        setSaveSuccess(null);
        setTestResult(null);
        setFormData({
          tag: '',
          supports_source: true,
          supports_target: false,
          environment: 'PROD',
          db_type: 'mysql',
          server: '',
          port: 3306,
          database_name: '',
          schema_name: '',
          username: '',
          credential_reference: '',
          service_name: '',
          purpose: '',
          owner: 'Admin',
          team: 'Data Engineering',
        });
      }, 1400);
    } catch (err: any) {
      setSaveError(err.message || 'Failed to save connection profile.');
    } finally {
      setIsSaving(false);
    }
  };

  const getDbBadge = (dbType: string) => {
    const type = dbType.toLowerCase();
    if (type.includes('mysql')) {
      return {
        label: 'MySQL',
        bg: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
      };
    }
    if (type.includes('postgres')) {
      return {
        label: 'PostgreSQL',
        bg: 'bg-blue-500/10 text-blue-400 border-blue-500/20',
      };
    }
    if (type.includes('oracle')) {
      return {
        label: 'Oracle DB',
        bg: 'bg-rose-500/10 text-rose-400 border-rose-500/20',
      };
    }
    if (type.includes('sqlserver') || type.includes('mssql')) {
      return {
        label: 'SQL Server',
        bg: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
      };
    }
    return {
      label: dbType.toUpperCase(),
      bg: 'bg-purple-500/10 text-purple-400 border-purple-500/20',
    };
  };

  return (
    <div className="p-8 space-y-8 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            <Network className="w-6 h-6 text-indigo-400" />
            Integrations & Connectors
            {projectScope && (
              <span className="text-xs font-mono px-2.5 py-0.5 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-indigo-300">
                {projectScope}
              </span>
            )}
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            Governed source database connections, target repository endpoints, and schema contracts.
          </p>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={() => {
              setIsAddModalOpen(true);
              setTestResult(null);
              setSaveError(null);
              setSaveSuccess(null);
            }}
            className="flex items-center gap-2 px-3.5 py-2 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-xs font-semibold text-white transition duration-150 shadow-lg shadow-indigo-600/20"
          >
            <Plus className="w-4 h-4" />
            <span>Onboard Connection</span>
          </button>

          <button
            onClick={() => refetch()}
            disabled={isFetching}
            className="flex items-center gap-2 px-3.5 py-2 rounded-xl bg-[#1e293b]/70 hover:bg-[#1e293b] border border-slate-700/80 text-xs font-semibold text-slate-200 transition duration-150 disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? 'animate-spin text-indigo-400' : 'text-slate-400'}`} />
            <span>Refresh</span>
          </button>
        </div>
      </div>

      {/* KPI Overview Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-5">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider">Total Integrations</span>
            <div className="w-8 h-8 rounded-xl bg-indigo-500/10 flex items-center justify-center text-indigo-400">
              <Database className="w-4 h-4" />
            </div>
          </div>
          <div className="text-3xl font-extrabold text-white tracking-tight">
            {isLoading ? '-' : integrationsData?.total_count || 0}
          </div>
          <p className="text-[11px] text-slate-500 mt-2">Active database endpoints</p>
        </div>

        <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-5">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider">Source Systems</span>
            <div className="w-8 h-8 rounded-xl bg-cyan-500/10 flex items-center justify-center text-cyan-400">
              <Zap className="w-4 h-4" />
            </div>
          </div>
          <div className="text-3xl font-extrabold text-cyan-400 tracking-tight">
            {isLoading ? '-' : integrationsData?.source_count || 0}
          </div>
          <p className="text-[11px] text-slate-500 mt-2">MySQL, Oracle & ERP sources</p>
        </div>

        <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-5">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider">Target Repositories</span>
            <div className="w-8 h-8 rounded-xl bg-emerald-500/10 flex items-center justify-center text-emerald-400">
              <Server className="w-4 h-4" />
            </div>
          </div>
          <div className="text-3xl font-extrabold text-emerald-400 tracking-tight">
            {isLoading ? '-' : integrationsData?.target_count || 0}
          </div>
          <p className="text-[11px] text-slate-500 mt-2">PostgreSQL Gold target schemas</p>
        </div>

        <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-5">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider">Security Invariant</span>
            <div className="w-8 h-8 rounded-xl bg-purple-500/10 flex items-center justify-center text-purple-400">
              <Shield className="w-4 h-4" />
            </div>
          </div>
          <div className="text-xs font-bold text-slate-200 mt-1 leading-snug">
            Single Authority
          </div>
          <p className="text-[11px] text-slate-500 mt-2">Zero hardcoded credentials in code</p>
        </div>
      </div>

      {/* Filter and Search Bar */}
      <div className="flex flex-col sm:flex-row gap-3 items-center justify-between">
        <div className="flex items-center gap-2 bg-[#0f172a] p-1 rounded-xl border border-slate-800">
          <button
            onClick={() => setFilterType('all')}
            className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition ${
              filterType === 'all'
                ? 'bg-indigo-600 text-white shadow'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            All Connectors ({integrations.length})
          </button>
          <button
            onClick={() => setFilterType('sources')}
            className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition ${
              filterType === 'sources'
                ? 'bg-indigo-600 text-white shadow'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            Source Systems ({integrations.filter((i: IntegrationDetail) => i.supports_source).length})
          </button>
          <button
            onClick={() => setFilterType('targets')}
            className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition ${
              filterType === 'targets'
                ? 'bg-indigo-600 text-white shadow'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            Target Databases ({integrations.filter((i: IntegrationDetail) => i.supports_target).length})
          </button>
        </div>

        <div className="relative w-full sm:w-72">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
          <input
            type="text"
            placeholder="Search integrations..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-9 pr-4 py-2 bg-[#0f172a] border border-slate-800 rounded-xl text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-indigo-500"
          />
        </div>
      </div>

      {/* Integrations Grid */}
      {isLoading ? (
        <div className="py-20 text-center text-slate-500 text-xs">
          Loading governed integrations ledger...
        </div>
      ) : filtered.length === 0 ? (
        <div className="bg-[#0f172a]/60 border border-slate-800 rounded-2xl p-12 text-center">
          <p className="text-slate-400 text-sm">No integrations match your search criteria.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {filtered.map((item: IntegrationDetail) => {
            const dbBadge = getDbBadge(item.db_type);
            const isTarget = item.supports_target;
            const isSource = item.supports_source;

            return (
              <div
                key={item.tag}
                className="bg-[#0f172a]/90 border border-slate-800 hover:border-slate-700 rounded-2xl p-5 shadow-lg transition duration-150 flex flex-col justify-between"
              >
                <div>
                  {/* Top Badges */}
                  <div className="flex items-center justify-between gap-2 mb-3">
                    <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold border ${dbBadge.bg}`}>
                      {dbBadge.label}
                    </span>

                    <div className="flex items-center gap-1.5">
                      {isSource && (
                        <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">
                          SOURCE
                        </span>
                      )}
                      {isTarget && (
                        <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                          TARGET
                        </span>
                      )}
                      <span className="px-2 py-0.5 rounded-full text-[10px] font-mono bg-slate-800 text-slate-300 border border-slate-700">
                        {item.environment}
                      </span>
                    </div>
                  </div>

                  {/* Title */}
                  <h3 className="text-base font-bold text-white tracking-tight flex items-center gap-2">
                    {item.tag}
                  </h3>

                  {item.purpose && (
                    <p className="text-xs text-slate-400 mt-1 line-clamp-1">{item.purpose}</p>
                  )}

                  {/* Connection Details Table */}
                  <div className="mt-4 space-y-2 text-xs bg-[#0b0f19] border border-slate-800/80 rounded-xl p-3">
                    <div className="flex justify-between items-center text-slate-400">
                      <span>Server / Host:</span>
                      <span className="font-mono text-slate-200 truncate max-w-[170px]" title={item.server}>
                        {item.server}:{item.port || '-'}
                      </span>
                    </div>
                    <div className="flex justify-between items-center text-slate-400">
                      <span>Database:</span>
                      <span className="font-mono text-slate-200 truncate max-w-[170px]" title={item.database_name}>
                        {item.database_name}
                      </span>
                    </div>
                    {item.schema_name && (
                      <div className="flex justify-between items-center text-slate-400">
                        <span>Schema:</span>
                        <span className="font-mono text-slate-200">{item.schema_name}</span>
                      </div>
                    )}
                    {item.username && (
                      <div className="flex justify-between items-center text-slate-400">
                        <span>Username:</span>
                        <span className="font-mono text-slate-200">{item.username}</span>
                      </div>
                    )}
                    {item.last_tested_at && (
                      <div className="flex justify-between items-center text-slate-400 pt-1 border-t border-slate-800">
                        <span>Last Latency:</span>
                        <span className="font-mono text-emerald-400">
                          {item.last_latency_ms ? `${item.last_latency_ms} ms` : 'Verified'}
                        </span>
                      </div>
                    )}
                  </div>
                </div>

                {/* Bottom Actions */}
                <div className="mt-5 pt-3 border-t border-slate-800/80 flex items-center justify-between">
                  <div className="flex items-center gap-1.5 text-[11px] text-slate-400">
                    <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
                    <span>Active Connection</span>
                  </div>

                  {isSource && onNavigateToPipelines && (
                    <button
                      onClick={() => onNavigateToPipelines(item.tag)}
                      className="text-xs font-semibold text-indigo-400 hover:text-indigo-300 flex items-center gap-1 transition group"
                    >
                      <span>Catalogue</span>
                      <ArrowRight className="w-3.5 h-3.5 group-hover:translate-x-0.5 transition" />
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Governed Architecture Callout */}
      <div className="bg-[#0f172a]/60 border border-slate-800/80 rounded-2xl p-6">
        <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 flex items-center gap-2">
          <Shield className="w-4 h-4 text-emerald-400" />
          Single Configuration Authority Standard (P0 Invariant)
        </h3>
        <p className="text-xs text-slate-400 leading-relaxed">
          Every database connection in HiveFlow is strictly resolved through <code className="text-indigo-300 font-mono">ConnectionManager</code> and backed by <code className="text-indigo-300 font-mono">metadata.connections</code>. Passwords and credentials are never stored in plain text and are injected strictly via container environment variables (<code className="text-indigo-300 font-mono">.env</code>) with zero fallback defaults.
        </p>
      </div>

      {/* ── Onboard Connection Modal ────────────────────────────────────────── */}
      {isAddModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-150">
          <div className="relative w-full max-w-2xl bg-[#0f172a] border border-slate-700/80 rounded-2xl shadow-2xl p-6 overflow-y-auto max-h-[92vh]">
            {/* Modal Header */}
            <div className="flex items-center justify-between pb-4 border-b border-slate-800">
              <div>
                <h2 className="text-lg font-bold text-white flex items-center gap-2">
                  <Database className="w-5 h-5 text-indigo-400" />
                  Onboard Database Connection
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">
                  Single Configuration Authority • Zero Hardcoded Passwords
                </p>
              </div>
              <button
                onClick={() => setIsAddModalOpen(false)}
                className="p-1 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Modal Form */}
            <form onSubmit={handleSaveConnection} className="mt-5 space-y-5">
              {/* Scope & Role */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Connection Tag <span className="text-rose-400">*</span>
                  </label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. crm_mysql or land_analytics"
                    value={formData.tag}
                    onChange={(e) =>
                      setFormData({ ...formData, tag: e.target.value.toLowerCase().replace(/[^a-z0-9_-]/g, '') })
                    }
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 font-mono"
                  />
                  <p className="text-[10px] text-slate-500 mt-1">
                    Unique alphanumeric identifier (e.g. source_tag in Airflow DAGs)
                  </p>
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Connection Role <span className="text-rose-400">*</span>
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    <button
                      type="button"
                      onClick={() =>
                        setFormData({
                          ...formData,
                          supports_source: true,
                          supports_target: false,
                        })
                      }
                      className={`px-3 py-2 rounded-xl text-xs font-semibold border transition ${
                        formData.supports_source
                          ? 'bg-cyan-500/10 border-cyan-500/30 text-cyan-400'
                          : 'bg-[#0b0f19] border-slate-800 text-slate-400 hover:text-slate-200'
                      }`}
                    >
                      Source System
                    </button>
                    <button
                      type="button"
                      onClick={() =>
                        setFormData({
                          ...formData,
                          supports_source: false,
                          supports_target: true,
                        })
                      }
                      className={`px-3 py-2 rounded-xl text-xs font-semibold border transition ${
                        formData.supports_target
                          ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400'
                          : 'bg-[#0b0f19] border-slate-800 text-slate-400 hover:text-slate-200'
                      }`}
                    >
                      Target Repository
                    </button>
                  </div>
                </div>
              </div>

              {/* Engine & Environment */}
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Database Engine <span className="text-rose-400">*</span>
                  </label>
                  <select
                    value={formData.db_type}
                    onChange={(e) => handleDbTypeChange(e.target.value)}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white focus:outline-none focus:border-indigo-500"
                  >
                    <option value="mysql">MySQL</option>
                    <option value="postgres">PostgreSQL</option>
                    <option value="oracle">Oracle Database</option>
                    <option value="sqlserver">Microsoft SQL Server</option>
                  </select>
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Environment
                  </label>
                  <select
                    value={formData.environment}
                    onChange={(e) => setFormData({ ...formData, environment: e.target.value })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white focus:outline-none focus:border-indigo-500"
                  >
                    <option value="PROD">PROD (Production)</option>
                    <option value="UAT">UAT (Staging)</option>
                    <option value="DEV">DEV (Development)</option>
                    <option value="TEST">TEST (Testing)</option>
                  </select>
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Port <span className="text-rose-400">*</span>
                  </label>
                  <input
                    type="number"
                    required
                    value={formData.port}
                    onChange={(e) => setFormData({ ...formData, port: parseInt(e.target.value) || 0 })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white font-mono focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>

              {/* Host & Database Name */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Server / Host <span className="text-rose-400">*</span>
                  </label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. 198.51.100.1 or ${MYSQL_HOST}"
                    value={formData.server}
                    onChange={(e) => setFormData({ ...formData, server: e.target.value })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 font-mono"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Database Name <span className="text-rose-400">*</span>
                  </label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. source_erp_db or postgres"
                    value={formData.database_name}
                    onChange={(e) => setFormData({ ...formData, database_name: e.target.value })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 font-mono"
                  />
                </div>
              </div>

              {/* Schema Name & Username */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Schema Name (Optional)
                  </label>
                  <input
                    type="text"
                    placeholder="e.g. public or dbo"
                    value={formData.schema_name || ''}
                    onChange={(e) => setFormData({ ...formData, schema_name: e.target.value })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 font-mono"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Username <span className="text-rose-400">*</span>
                  </label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. db_reader"
                    value={formData.username}
                    onChange={(e) => setFormData({ ...formData, username: e.target.value })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 font-mono"
                  />
                </div>
              </div>

              {/* Credential Reference & Secret Hygiene Notice */}
              <div>
                <label className="block text-xs font-medium text-slate-300 mb-1 flex items-center justify-between">
                  <span>
                    Credential Reference (Env Variable) <span className="text-rose-400">*</span>
                  </span>
                  <span className="text-[10px] text-amber-400 flex items-center gap-1">
                    <Lock className="w-3 h-3" />
                    Zero Plaintext Password Policy
                  </span>
                </label>
                <input
                  type="text"
                  required
                  placeholder="e.g. ${ERP_DB_PASSWORD} or ERP_DB_PASSWORD"
                  value={formData.credential_reference}
                  onChange={(e) => setFormData({ ...formData, credential_reference: e.target.value })}
                  className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 font-mono"
                />
                <div className="mt-2 bg-indigo-500/10 border border-indigo-500/20 rounded-xl p-3 flex items-start gap-2.5 text-xs text-indigo-300">
                  <Info className="w-4 h-4 shrink-0 mt-0.5 text-indigo-400" />
                  <p className="leading-relaxed">
                    Per project security standard, raw secrets are never persisted in the database. Enter the reference name here, and define the actual secret value in the application’s <code className="font-mono text-white bg-slate-800 px-1 py-0.5 rounded">.env</code> file.
                  </p>
                </div>
              </div>

              {/* Purpose & Team */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Purpose / Description
                  </label>
                  <input
                    type="text"
                    placeholder="e.g. Core transactional ERP database"
                    value={formData.purpose || ''}
                    onChange={(e) => setFormData({ ...formData, purpose: e.target.value })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500"
                  />
                </div>

                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">
                    Managing Team
                  </label>
                  <input
                    type="text"
                    placeholder="Data Engineering"
                    value={formData.team || ''}
                    onChange={(e) => setFormData({ ...formData, team: e.target.value })}
                    className="w-full px-3 py-2 bg-[#0b0f19] border border-slate-700 rounded-xl text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>

              {/* Test Connection Feedback */}
              {testResult && (
                <div
                  className={`p-3.5 rounded-xl border flex items-start gap-3 text-xs ${
                    testResult.status === 'SUCCESS'
                      ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
                      : 'bg-rose-500/10 border-rose-500/30 text-rose-300'
                  }`}
                >
                  {testResult.status === 'SUCCESS' ? (
                    <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0 mt-0.5" />
                  ) : (
                    <AlertTriangle className="w-4 h-4 text-rose-400 shrink-0 mt-0.5" />
                  )}
                  <div>
                    <div className="font-bold">
                      {testResult.status === 'SUCCESS'
                        ? `Connectivity Verified (${testResult.latency_ms} ms)`
                        : 'Connectivity Test Failed'}
                    </div>
                    {testResult.error && (
                      <p className="mt-1 text-[11px] font-mono opacity-90">{testResult.error}</p>
                    )}
                  </div>
                </div>
              )}

              {/* Save Feedback */}
              {saveSuccess && (
                <div className="p-3.5 rounded-xl bg-emerald-500/10 border border-emerald-500/30 text-emerald-300 flex items-center gap-2.5 text-xs font-semibold">
                  <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
                  <span>{saveSuccess}</span>
                </div>
              )}

              {saveError && (
                <div className="p-3.5 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 flex items-center gap-2.5 text-xs">
                  <AlertTriangle className="w-4 h-4 text-rose-400 shrink-0" />
                  <span>{saveError}</span>
                </div>
              )}

              {/* Modal Actions */}
              <div className="pt-4 border-t border-slate-800 flex items-center justify-between gap-3">
                <button
                  type="button"
                  onClick={handleTestConnection}
                  disabled={isTesting || isSaving}
                  className="flex items-center gap-2 px-4 py-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-xs font-semibold text-slate-200 border border-slate-700 transition disabled:opacity-50"
                >
                  {isTesting ? (
                    <>
                      <Loader2 className="w-3.5 h-3.5 animate-spin text-indigo-400" />
                      <span>Probing Endpoint...</span>
                    </>
                  ) : (
                    <>
                      <Zap className="w-3.5 h-3.5 text-amber-400" />
                      <span>Test Handshake</span>
                    </>
                  )}
                </button>

                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => setIsAddModalOpen(false)}
                    className="px-4 py-2 rounded-xl bg-transparent hover:bg-slate-800 text-xs font-semibold text-slate-400 hover:text-slate-200 transition"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={isSaving || isTesting}
                    className="flex items-center gap-2 px-5 py-2 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-xs font-semibold text-white shadow-lg shadow-indigo-600/30 transition disabled:opacity-50"
                  >
                    {isSaving ? (
                      <>
                        <Loader2 className="w-3.5 h-3.5 animate-spin" />
                        <span>Saving Profile...</span>
                      </>
                    ) : (
                      <span>Save Connection</span>
                    )}
                  </button>
                </div>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};
