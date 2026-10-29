// frontend/src/views/DashboardView.tsx
import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { api } from '../api/client';
import { LiveRunMetrics } from '../components/runs/LiveRunMetrics';
import { WorkflowCanvas } from '../components/runs/WorkflowCanvas';
import { ExecutionPanel } from '../components/runs/ExecutionPanel';
import {
  PlayCircle,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Clock,
  RefreshCw,
  Search,
  Server,
  Activity,
  ArrowRight,
  Workflow,
  Loader2,
} from 'lucide-react';

interface DashboardViewProps {
  focusedRunId?: string | null;
  isTriggerOpen?: boolean;
  onToggleTrigger?: (open: boolean) => void;
  onNavigateToRuns?: (runId?: string) => void;
  onNavigateToIntegrations?: () => void;
}

export const DashboardView: React.FC<DashboardViewProps> = ({
  focusedRunId,
  isTriggerOpen: isTriggerOpenProp,
  onToggleTrigger,
  onNavigateToRuns,
  onNavigateToIntegrations,
}) => {
  const { projectScope, setProjectScope } = useAuth();
  const [searchTerm, setSearchTerm] = useState('');
  const [internalTriggerOpen, setInternalTriggerOpen] = useState(false);
  const [locallyTriggeredRunId, setLocallyTriggeredRunId] = useState<string | null>(null);
  const workflowSectionRef = React.useRef<HTMLDivElement>(null);

  const isTriggerOpen = isTriggerOpenProp !== undefined ? isTriggerOpenProp : internalTriggerOpen;
  const handleToggleTrigger = (open: boolean) => {
    setInternalTriggerOpen(open);
    onToggleTrigger?.(open);
  };

  // 1. Dashboard KPIs
  const {
    data: kpis,
    isLoading: isLoadingKpis,
    refetch: refetchKpis,
    isFetching: isFetchingKpis,
  } = useQuery({
    queryKey: ['kpis', projectScope],
    queryFn: () => api.getKPIs(projectScope),
    refetchInterval: 30000,
  });

  // 2. Recent Activity Feed
  const {
    data: activityData,
    isLoading: isLoadingActivity,
    refetch: refetchActivity,
    isFetching: isFetchingActivity,
  } = useQuery({
    queryKey: ['activity', projectScope],
    queryFn: () => api.getActivity(20, projectScope),
    refetchInterval: 15000,
  });

  // 3. System Health Probes
  const { data: healthData, refetch: refetchHealth } = useQuery({
    queryKey: ['systemHealth'],
    queryFn: () => api.getHealth(),
    refetchInterval: 60000,
  });

  // 4. Query Runs list to detect active or latest run for Live Operational Workflow
  const {
    data: runs = [],
    refetch: refetchRuns,
  } = useQuery({
    queryKey: ['runs', projectScope],
    queryFn: () => api.getRuns(10, projectScope),
    refetchInterval: (query) => {
      const data = query.state.data;
      const hasActive = data?.some((r) => ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(r.status));
      return hasActive ? 2500 : 15000;
    },
  });

  // 5. Cluster-wide Active Run Detection (alerts operator if an active job runs in another scope)
  const { data: clusterRuns = [] } = useQuery({
    queryKey: ['clusterRuns'],
    queryFn: () => api.getRuns(10, null),
    refetchInterval: 3000,
  });

  const activeRun = runs.find((r) => ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(r.status));
  const clusterActiveRun = clusterRuns.find((r) => ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(r.status));
  const latestRun = runs[0];

  // Prioritize newly triggered run, focused run, active run in current scope, cluster-wide active run, latest run
  const featuredRunId =
    locallyTriggeredRunId ||
    focusedRunId ||
    activeRun?.run_id ||
    clusterActiveRun?.run_id ||
    (latestRun ? latestRun.run_id : null);

  // 6. Query Featured Run Detail for the Live Workflow Canvas & Metrics HUD
  const { data: featuredRunDetail, isLoading: isLoadingFeaturedRun } = useQuery({
    queryKey: ['runDetail', featuredRunId],
    queryFn: () => (featuredRunId ? api.getRunDetail(featuredRunId) : null),
    enabled: !!featuredRunId,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data && ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(data.status)) {
        return 2000;
      }
      return false;
    },
  });

  React.useEffect(() => {
    if ((focusedRunId || locallyTriggeredRunId) && workflowSectionRef.current) {
      workflowSectionRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, [focusedRunId, locallyTriggeredRunId, featuredRunDetail?.run_id]);

  const handleRefresh = () => {
    refetchKpis();
    refetchActivity();
    refetchHealth();
    refetchRuns();
  };

  const isRefreshing = isFetchingKpis || isFetchingActivity;

  // Filter activities
  const activities = activityData?.activities || [];
  const filteredActivities = activities.filter((act) => {
    if (!searchTerm) return true;
    const term = searchTerm.toLowerCase();
    return (
      act.table?.toLowerCase().includes(term) ||
      act.source?.toLowerCase().includes(term) ||
      act.status?.toLowerCase().includes(term) ||
      act.layer?.toLowerCase().includes(term)
    );
  });

  const isJobRunning = (kpis?.running || 0) > 0 || (activeRun !== undefined);

  return (
    <div className="p-8 space-y-8 max-w-7xl mx-auto">
      {/* Page Title & Refresh */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            Operations Dashboard
            {projectScope && (
              <span className="text-xs font-mono px-2.5 py-0.5 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-indigo-300">
                {projectScope}
              </span>
            )}
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            Real-time pipeline metrics, live operational runtime workflows, and persistence ledger.
          </p>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={() => handleToggleTrigger(!isTriggerOpen)}
            className="flex items-center gap-2 px-3.5 py-2 rounded-xl bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white text-xs font-semibold shadow-lg shadow-indigo-600/25 transition shrink-0 active:scale-95"
          >
            <PlayCircle className="w-3.5 h-3.5" />
            <span>{isTriggerOpen ? 'Close Trigger Panel' : 'Trigger Pipeline'}</span>
          </button>

          <button
            onClick={handleRefresh}
            disabled={isRefreshing}
            className="flex items-center gap-2 px-3.5 py-2 rounded-xl bg-[#1e293b]/70 hover:bg-[#1e293b] border border-slate-700/80 text-xs font-semibold text-slate-200 transition duration-150 disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin text-indigo-400' : 'text-slate-400'}`} />
            <span>Refresh Metrics</span>
          </button>
        </div>
      </div>

      {/* Embedded Pipeline Execution Panel directly in Operations Dashboard */}
      {isTriggerOpen && (
        <div className="animate-fadeIn">
          <ExecutionPanel
            onRunTriggered={(newRunId) => {
              setLocallyTriggeredRunId(newRunId);
              handleToggleTrigger(false);
              refetchRuns();
              refetchKpis();
              refetchActivity();
              setTimeout(() => {
                workflowSectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
              }, 120);
            }}
            onCancel={() => handleToggleTrigger(false)}
          />
        </div>
      )}

      {/* Cross-Scope In-Flight Execution Alert Banner */}
      {clusterActiveRun && projectScope && clusterActiveRun.source_tag && clusterActiveRun.source_tag !== projectScope && (
        <div className="bg-gradient-to-r from-indigo-950/90 via-slate-900 to-indigo-950/90 border border-cyan-500/50 rounded-2xl p-4 flex flex-col sm:flex-row sm:items-center justify-between gap-4 shadow-xl shadow-cyan-950/30 animate-fadeIn">
          <div className="flex items-center gap-3.5">
            <span className="relative flex h-3.5 w-3.5 shrink-0">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-3.5 w-3.5 bg-cyan-500"></span>
            </span>
            <div>
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-xs font-bold text-white uppercase tracking-wider">Active Pipeline In Flight</span>
                <span className="text-xs font-mono font-bold px-2.5 py-0.5 rounded-full bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">
                  {clusterActiveRun.source_tag}
                </span>
                <span className="text-[11px] font-mono text-slate-400">
                  ({clusterActiveRun.run_id})
                </span>
              </div>
              <p className="text-xs text-slate-300 mt-1">
                A pipeline job for <strong className="text-cyan-300 font-bold">{clusterActiveRun.source_tag}</strong> is actively running in the Spark cluster. Your view is currently scoped to <span className="text-indigo-300 font-semibold">{projectScope}</span>.
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 self-end sm:self-auto shrink-0">
            <button
              onClick={() => clusterActiveRun.source_tag && setProjectScope(clusterActiveRun.source_tag)}
              className="flex items-center gap-2 px-4 py-2 rounded-xl bg-gradient-to-r from-cyan-500 to-cyan-400 hover:from-cyan-400 hover:to-cyan-300 text-slate-950 font-bold text-xs shadow-lg shadow-cyan-500/20 active:scale-95 transition"
            >
              <span>Switch View to {clusterActiveRun.source_tag}</span>
              <ArrowRight className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      )}

      {/* KPI Cards Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        {/* Running Jobs (Clickable to jump to runs) */}
        <div
          onClick={() => onNavigateToRuns && onNavigateToRuns(activeRun?.run_id)}
          className={`bg-[#131d31]/80 border rounded-2xl p-5 relative overflow-hidden group transition ${
            isJobRunning
              ? 'border-indigo-500/60 shadow-lg shadow-indigo-500/10 cursor-pointer ring-1 ring-indigo-500/30'
              : 'border-slate-800/80 hover:border-slate-700/80'
          }`}
        >
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider font-sans">Running Jobs</span>
            <div className={`w-8 h-8 rounded-xl flex items-center justify-center ${
              isJobRunning ? 'bg-indigo-500/20 text-indigo-400' : 'bg-blue-500/10 text-blue-400'
            }`}>
              <PlayCircle className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-3xl font-extrabold text-white tracking-tight font-sans tabular-nums">
              {isLoadingKpis ? '-' : (kpis?.running ?? (activeRun ? 1 : 0))}
            </span>
            {isJobRunning && (
              <span className="flex items-center gap-1 text-[11px] font-semibold text-indigo-400 animate-pulse font-sans">
                <span className="w-1.5 h-1.5 rounded-full bg-indigo-400" />
                Active
              </span>
            )}
          </div>
          <p className="text-xs text-slate-400 mt-2 font-sans">
            {isJobRunning ? 'Click to inspect execution' : 'Active in Spark cluster'}
          </p>
        </div>

        {/* Success Today */}
        <div className="bg-[#131d31]/80 border border-slate-800/80 rounded-2xl p-5 relative overflow-hidden group hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider font-sans">Success Today</span>
            <div className="w-8 h-8 rounded-xl bg-emerald-500/10 flex items-center justify-center text-emerald-400">
              <CheckCircle2 className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-3xl font-extrabold text-emerald-400 tracking-tight font-sans tabular-nums">
              {isLoadingKpis ? '-' : kpis?.success_today || 0}
            </span>
            <span className="text-xs font-medium text-emerald-500/90 font-sans">Batches</span>
          </div>
          <p className="text-xs text-slate-400 mt-2 font-sans">100% converged writes</p>
        </div>

        {/* Failed Today */}
        <div className="bg-[#131d31]/80 border border-slate-800/80 rounded-2xl p-5 relative overflow-hidden group hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider font-sans">Failed Today</span>
            <div className="w-8 h-8 rounded-xl bg-rose-500/10 flex items-center justify-center text-rose-400">
              <XCircle className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className={`text-3xl font-extrabold tracking-tight font-sans tabular-nums ${kpis?.failed_today ? 'text-rose-400' : 'text-slate-300'}`}>
              {isLoadingKpis ? '-' : kpis?.failed_today || 0}
            </span>
            <span className="text-xs font-medium text-slate-400 font-sans">Errors</span>
          </div>
          <p className="text-xs text-slate-400 mt-2 font-sans">Isolated per source</p>
        </div>

        {/* Stalled Runs */}
        <div className="bg-[#131d31]/80 border border-slate-800/80 rounded-2xl p-5 relative overflow-hidden group hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider font-sans">Stalled Runs</span>
            <div className="w-8 h-8 rounded-xl bg-amber-500/10 flex items-center justify-center text-amber-400">
              <AlertTriangle className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className={`text-3xl font-extrabold tracking-tight font-sans tabular-nums ${kpis?.stalled_count ? 'text-amber-400' : 'text-slate-300'}`}>
              {isLoadingKpis ? '-' : kpis?.stalled_count || 0}
            </span>
            <span className="text-xs font-medium text-slate-400 font-sans">Heartbeat drop</span>
          </div>
          <p className="text-xs text-slate-400 mt-2 font-sans">Recovery candidate</p>
        </div>

        {/* Last Run Activity */}
        <div className="bg-[#131d31]/80 border border-slate-800/80 rounded-2xl p-5 relative overflow-hidden group hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider font-sans">Last Activity</span>
            <div className="w-8 h-8 rounded-xl bg-purple-500/10 flex items-center justify-center text-purple-400">
              <Clock className="w-4 h-4" />
            </div>
          </div>
          <div className="text-xs font-bold text-slate-200 truncate font-sans tabular-nums">
            {kpis?.last_run_end ? kpis.last_run_end.split('.')[0] : 'No activity yet'}
          </div>
          <p className="text-xs text-slate-400 mt-2 font-sans">Authoritative timestamp</p>
        </div>
      </div>

      {/* Featured Operational Runtime Workflow & Real-Time Telemetry */}
      {isLoadingFeaturedRun && !featuredRunDetail && focusedRunId && (
        <div ref={workflowSectionRef} className="p-8 rounded-2xl bg-[#131d31]/80 border border-indigo-500/30 flex items-center justify-center gap-3 text-indigo-300 animate-pulse scroll-mt-6">
          <Loader2 className="w-5 h-5 animate-spin text-indigo-400" />
          <span className="text-sm font-semibold">Connecting to active run {focusedRunId}...</span>
        </div>
      )}
      {featuredRunDetail && (
        <div ref={workflowSectionRef} className="space-y-4 animate-fadeIn scroll-mt-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2.5">
              {['RUNNING', 'DISPATCHED', 'QUEUED'].includes(featuredRunDetail.status) ? (
                <span className="relative flex h-3 w-3">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-3 w-3 bg-cyan-500"></span>
                </span>
              ) : (
                <Workflow className="w-4 h-4 text-indigo-400" />
              )}
              <h2 className="text-xs font-bold uppercase tracking-wider text-white">
                {['RUNNING', 'DISPATCHED', 'QUEUED'].includes(featuredRunDetail.status)
                  ? 'Active Pipeline Execution Workflow'
                  : 'Operational Pipeline Runtime Workflow'}
              </h2>
              <span className="text-xs font-mono text-slate-400">
                ({featuredRunDetail.run_id})
              </span>
            </div>

            {onNavigateToRuns && (
              <button
                onClick={() => onNavigateToRuns(featuredRunDetail.run_id)}
                className="text-xs font-semibold text-indigo-400 hover:text-indigo-300 flex items-center gap-1 transition"
              >
                <span>Full Execution History & Terminal Logs</span>
                <ArrowRight className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          {/* Real-time stats HUD (Rows/Min, Chunks, Heartbeat) */}
          <LiveRunMetrics run={featuredRunDetail} />

          {/* Visual Medallion DAG Workflow Canvas */}
          <WorkflowCanvas run={featuredRunDetail} />
        </div>
      )}

      {/* System Health Probes Bar */}
      {healthData && (
        <div className="bg-[#131d31]/60 border border-slate-800/80 rounded-2xl p-5">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Server className="w-4 h-4 text-indigo-400" />
              <h2 className="text-xs font-bold uppercase tracking-wider text-slate-300 font-sans">
                System Infrastructure Probes
              </h2>
            </div>
            <div className="flex items-center gap-3">
              {onNavigateToIntegrations && (
                <button
                  onClick={onNavigateToIntegrations}
                  className="text-xs font-semibold text-indigo-400 hover:text-indigo-300 flex items-center gap-1 transition font-sans"
                >
                  <span>View All Integrations</span>
                  <ArrowRight className="w-3.5 h-3.5" />
                </button>
              )}
              <span className="text-xs text-slate-400 font-sans">Checked: {healthData.checked_at}</span>
              <span
                className={`text-[10px] font-bold px-2.5 py-0.5 rounded-full font-sans tracking-wide ${
                  healthData.overall_status === 'HEALTHY'
                    ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                    : 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                }`}
              >
                {healthData.overall_status}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {healthData.probes.map((probe, idx) => (
              <div
                key={idx}
                className="bg-[#151f33]/70 border border-slate-800/80 rounded-xl p-3 flex items-start justify-between gap-3"
              >
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <span
                      className={`w-2 h-2 rounded-full ${
                        probe.status === 'HEALTHY' ? 'bg-emerald-400' : 'bg-rose-400'
                      }`}
                    />
                    <span className="text-xs font-semibold text-slate-200 truncate font-sans">{probe.name}</span>
                  </div>
                  <p className="text-xs text-slate-400 mt-1 truncate font-sans">{probe.message}</p>
                </div>
                <span className="text-xs font-sans tabular-nums text-slate-400 flex-shrink-0 font-medium">
                  {probe.latency_ms} ms
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Activity Table Card */}
      <div className="bg-[#111827]/90 border border-slate-800/80 rounded-2xl overflow-hidden shadow-xl">
        <div className="p-5 border-b border-slate-800/80 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <h2 className="text-base font-bold text-white flex items-center gap-2 font-sans">
              <Activity className="w-4 h-4 text-indigo-400" />
              Recent Pipeline Activity
            </h2>
            <p className="text-xs text-slate-400 mt-0.5 font-sans">
              Live read-only projection of persisted metadata execution telemetry.
            </p>
          </div>

          {/* Search Table */}
          <div className="relative w-full sm:w-64">
            <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Filter by table or status..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full bg-[#151f33] border border-slate-700/80 focus:border-indigo-500 rounded-xl pl-9 pr-3 py-2 text-xs text-white placeholder-slate-400 outline-none transition font-sans"
            />
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-[#0e1626]/90 border-b border-slate-800 text-xs uppercase tracking-wider text-slate-300 font-semibold select-none font-sans">
                <th className="py-3.5 px-4">Status</th>
                <th className="py-3.5 px-4">Source & Table</th>
                <th className="py-3.5 px-4">Target Table</th>
                <th className="py-3.5 px-4">Layer</th>
                <th className="py-3.5 px-4 text-right">Extracted / Persisted</th>
                <th className="py-3.5 px-4 text-right">Elapsed</th>
                <th className="py-3.5 px-4 text-right">Timestamp</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 text-xs">
              {isLoadingActivity ? (
                <tr>
                  <td colSpan={7} className="py-12 text-center text-slate-400 font-sans">
                    Loading activity telemetry...
                  </td>
                </tr>
              ) : filteredActivities.length === 0 ? (
                <tr>
                  <td colSpan={7} className="py-12 text-center text-slate-400 font-sans">
                    No matching activity recorded.
                  </td>
                </tr>
              ) : (
                filteredActivities.map((act, idx) => (
                  <tr
                    key={idx}
                    onClick={() => onNavigateToRuns && onNavigateToRuns(act.run_id || undefined)}
                    className="hover:bg-[#151f33]/60 transition duration-150 cursor-pointer"
                  >
                    <td className="py-3.5 px-4 whitespace-nowrap">
                      <span
                        className={`inline-flex items-center px-2.5 py-0.5 rounded-md text-[11px] font-semibold uppercase tracking-wide border font-sans ${
                          act.status === 'SUCCESS'
                            ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                            : act.status === 'NOT_REQUESTED'
                            ? 'bg-slate-800 text-slate-400 border-slate-700'
                            : act.status.includes('FAIL')
                            ? 'bg-rose-500/10 text-rose-400 border-rose-500/20'
                            : 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20'
                        }`}
                      >
                        {act.status}
                      </span>
                    </td>
                    <td className="py-3.5 px-4 whitespace-nowrap">
                      <div className="font-semibold text-slate-100 font-sans">{act.table || 'all_datasets'}</div>
                      <div className="text-xs text-slate-400 font-sans mt-0.5">{act.source}</div>
                    </td>
                    <td className="py-3.5 px-4 whitespace-nowrap font-sans text-slate-300">
                      {act.target_table || '-'}
                    </td>
                    <td className="py-3.5 px-4 whitespace-nowrap">
                      <span className="font-sans text-[11px] font-semibold text-slate-400 uppercase tracking-wider">
                        {act.layer || 'MEDALLION'}
                      </span>
                    </td>
                    <td className="py-3.5 px-4 whitespace-nowrap text-right font-sans tabular-nums">
                      <span className="text-slate-400">{(act.source_count || 0).toLocaleString()}</span>
                      <span className="text-slate-600 mx-1.5">→</span>
                      <span className="text-emerald-400 font-semibold">{(act.target_count || 0).toLocaleString()}</span>
                    </td>
                    <td className="py-3.5 px-4 whitespace-nowrap text-right font-sans tabular-nums text-slate-400">
                      {act.elapsed_sec ? `${act.elapsed_sec.toFixed(1)}s` : '0.0s'}
                    </td>
                    <td className="py-3.5 px-4 whitespace-nowrap text-right text-slate-400 text-xs font-sans tabular-nums">
                      {act.end_time || '-'}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
