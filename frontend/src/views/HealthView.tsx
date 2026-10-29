import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import {
  CheckCircle2,
  AlertTriangle,
  XCircle,
  RefreshCw,
  Server,
  ShieldCheck,
  Lock,
  Database,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';

const METADATA_GROUPS = [
  { group: 'Core catalog', purpose: 'Source/target definitions and table inventory.', tables: 'connections, db_tables, db_columns, db_tables_etl, db_columns_etl' },
  { group: 'Execution runs', purpose: 'Parent jobs, locks, events, attempts, and submission audit.', tables: 'gui_job_registry, job_scope_claim, job_event, job_submission_audit, batch_log' },
  { group: 'CDC state', purpose: 'Incremental cursor state and committed CDC windows.', tables: 'table_cdc_state, cdc_attempt_window, cdc_commit_event' },
  { group: 'Governance', purpose: 'Approved policies, recommendations, and promotion audit.', tables: 'table_policy_version, table_recommendations, policy_promotion_audit' },
  { group: 'Recovery evidence', purpose: 'Snapshot, reconciliation, and publication ledgers used after uncertain commits.', tables: 'source_snapshot_attempt, source_snapshot_manifest, attempt_persistence_manifest, cdc_attempt_reconciliation, gold_publication_manifest' },
  { group: 'Quality and audit', purpose: 'Operational audit, schema drift, lineage, notifications, and data-quality results.', tables: 'gui_audit_log, data_lineage, schema_drift_alerts, notification, data_quality_*' },
  { group: 'System', purpose: 'Migration ledger, capabilities, settings, and RBAC records.', tables: 'schema_migrations, system_capabilities, system_settings, gui_users, gui_roles, gui_role_permissions' },
];

export const HealthView: React.FC = () => {
  const [isClearingCache, setIsClearingCache] = useState(false);
  const [cacheMsg, setCacheMsg] = useState<string | null>(null);
  const [isSchemaMapOpen, setIsSchemaMapOpen] = useState(false);

  const {
    data: healthData,
    isLoading,
    refetch,
    isFetching,
  } = useQuery({
    queryKey: ['systemHealthDetailed'],
    queryFn: () => api.getHealth(),
  });

  const handleClearCache = async () => {
    setIsClearingCache(true);
    try {
      const res = await api.clearCache();
      setCacheMsg(res.message);
      refetch();
      setTimeout(() => setCacheMsg(null), 4000);
    } catch (err: any) {
      setCacheMsg('Cache clear failed: ' + err.message);
    } finally {
      setIsClearingCache(false);
    }
  };

  const isHealthy = healthData?.overall_status === 'HEALTHY';
  const isDegraded = healthData?.overall_status === 'DEGRADED';


  return (
    <div className="p-8 space-y-8 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            System & Infrastructure Health
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            Authoritative in-process probes: PostgreSQL metadata connectivity and SHA-256 schema migration ledger.
          </p>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={() => handleClearCache()}
            disabled={isClearingCache}
            className="self-start sm:self-auto flex items-center gap-2 px-3.5 py-2 rounded-xl bg-[#1e293b]/70 hover:bg-[#1e293b] border border-slate-700/80 text-xs font-semibold text-slate-200 transition duration-150 disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isClearingCache ? 'animate-spin text-indigo-400' : 'text-slate-400'}`} />
            <span>Clear Metadata Cache</span>
          </button>

          <button
            onClick={() => refetch()}
            disabled={isFetching}
            className="self-start sm:self-auto flex items-center gap-2 px-3.5 py-2 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-xs font-semibold text-white transition duration-150 shadow-md shadow-indigo-600/25 disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? 'animate-spin text-white' : 'text-white'}`} />
            <span>Execute Probes Now</span>
          </button>
        </div>
      </div>

      {cacheMsg && (
        <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-xl p-3 text-xs text-emerald-300 flex items-center gap-2">
          <CheckCircle2 className="w-4 h-4 text-emerald-400 flex-shrink-0" />
          <span>{cacheMsg}</span>
        </div>
      )}


      {/* Main Status Hero */}
      <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-6 shadow-xl flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div className="flex items-center gap-4">
          <div
            className={`w-14 h-14 rounded-2xl flex items-center justify-center ${
              isHealthy
                ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30'
                : isDegraded
                ? 'bg-amber-500/15 text-amber-400 border border-amber-500/30'
                : 'bg-rose-500/15 text-rose-400 border border-rose-500/30'
            }`}
          >
            {isHealthy ? (
              <CheckCircle2 className="w-7 h-7" />
            ) : isDegraded ? (
              <AlertTriangle className="w-7 h-7" />
            ) : (
              <XCircle className="w-7 h-7" />
            )}
          </div>

          <div>
            <div className="flex items-center gap-2">
              <span className="text-xs uppercase font-bold tracking-wider text-slate-400">
                Overall Cluster State
              </span>
              <span
                className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${
                  isHealthy
                    ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                    : 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                }`}
              >
                {healthData?.overall_status || 'CHECKING...'}
              </span>
            </div>
            <h2 className="text-xl font-bold text-white mt-1">
              {isHealthy
                ? 'All Core Metadata Subsystems Operational'
                : 'Subsystem Attention Required'}
            </h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Last probe evaluated at: {healthData?.checked_at || 'Pending'}
            </p>
          </div>
        </div>
      </div>

      {/* Probe Detail Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {isLoading ? (
          <div className="col-span-2 py-12 text-center text-slate-500 text-xs">
            Evaluating health probes...
          </div>
        ) : (
          healthData?.probes.map((probe, idx) => {
            const isOk = probe.status === 'HEALTHY';
            return (
              <div
                key={idx}
                className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-6 relative overflow-hidden group hover:border-slate-700 transition shadow-lg"
              >
                <div className="flex items-start justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div
                      className={`w-10 h-10 rounded-xl flex items-center justify-center ${
                        isOk
                          ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                          : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                      }`}
                    >
                      <Server className="w-5 h-5" />
                    </div>
                    <div>
                      <h3 className="text-sm font-bold text-white">{probe.name}</h3>
                      <span className="text-[11px] font-mono text-slate-500">
                        Response time: {probe.latency_ms} ms
                      </span>
                    </div>
                  </div>

                  <span
                    className={`px-2.5 py-0.5 rounded-full text-[10px] font-bold ${
                      isOk
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                        : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                    }`}
                  >
                    {probe.status}
                  </span>
                </div>

                <div className="bg-[#1e293b]/40 border border-slate-800/80 rounded-xl p-3 text-xs text-slate-300 font-mono">
                  {probe.message}
                </div>
              </div>
            );
          })
        )}
      </div>

      {/* Governance & Architectural Invariants Banner */}
      <div className="bg-[#0f172a]/60 border border-slate-800/80 rounded-2xl p-6">
        <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-4 flex items-center gap-2">
          <ShieldCheck className="w-4 h-4 text-emerald-400" />
          Enforced Milestone 1 Architectural Invariants
        </h3>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
          <div className="bg-[#1e293b]/30 border border-slate-800 rounded-xl p-4">
            <div className="flex items-center gap-2 text-indigo-300 font-semibold mb-1">
              <Lock className="w-4 h-4" />
              <span>Non-Destructive Boundary</span>
            </div>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              API exposes strictly read-only endpoints. Pipeline execution dispatching is deferred to Milestone 3 behind strict permission checks.
            </p>
          </div>

          <div className="bg-[#1e293b]/30 border border-slate-800 rounded-xl p-4">
            <div className="flex items-center gap-2 text-cyan-300 font-semibold mb-1">
              <Server className="w-4 h-4" />
              <span>Parallel Deployment</span>
            </div>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Legacy Streamlit operator console remains 100% active on port 8501 without interference or port collision.
            </p>
          </div>

          <div className="bg-[#1e293b]/30 border border-slate-800 rounded-xl p-4">
            <div className="flex items-center gap-2 text-emerald-300 font-semibold mb-1">
              <ShieldCheck className="w-4 h-4" />
              <span>SHA-256 Ledger Governance</span>
            </div>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Every database migration is verified against canonical hashes. Any drift triggers fail-closed protection.
            </p>
          </div>
        </div>
      </div>

      {/* Internal Metadata Architecture Map (Streamlit Step 7.5 Parity) */}
      <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
        <button
          onClick={() => setIsSchemaMapOpen(!isSchemaMapOpen)}
          className="w-full p-5 flex items-center justify-between text-left hover:bg-slate-800/20 transition"
        >
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-indigo-500/10 text-indigo-400 flex items-center justify-center border border-indigo-500/20">
              <Database className="w-5 h-5" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-white flex items-center gap-2">
                <span>Internal Metadata Control-Plane Schema Map</span>
                <span className="text-[10px] font-mono uppercase bg-indigo-500/10 text-indigo-400 px-2 py-0.5 rounded border border-indigo-500/20">
                  7 Core Groups
                </span>
              </h3>
              <p className="text-xs text-slate-400">
                Authoritative reference for governed metadata schemas, ledgers, and audit structures (Step 7.5).
              </p>
            </div>
          </div>

          <div className="text-slate-400">
            {isSchemaMapOpen ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
          </div>
        </button>

        {isSchemaMapOpen && (
          <div className="border-t border-slate-800 p-5 overflow-x-auto">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="bg-[#090d16]/70 border-b border-slate-800 text-[11px] uppercase tracking-wider text-slate-400 font-semibold">
                  <th className="py-2.5 px-4">Group</th>
                  <th className="py-2.5 px-4">Architectural Purpose</th>
                  <th className="py-2.5 px-4">Internal Control-Plane Tables</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {METADATA_GROUPS.map((g, idx) => (
                  <tr key={idx} className="hover:bg-[#1e293b]/40">
                    <td className="py-3 px-4 font-bold text-indigo-300 whitespace-nowrap">
                      {g.group}
                    </td>
                    <td className="py-3 px-4 text-slate-300">
                      {g.purpose}
                    </td>
                    <td className="py-3 px-4 font-mono text-[11px] text-cyan-300">
                      {g.tables}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>

  );
};
