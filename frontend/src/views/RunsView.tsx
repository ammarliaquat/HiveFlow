import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { api } from '../api/client';
import { LiveRunMetrics } from '../components/runs/LiveRunMetrics';
import { WorkflowCanvas } from '../components/runs/WorkflowCanvas';
import {
  History,
  Search,
  X,
  Play,
  StopCircle,
  Terminal,
  RefreshCw,
  Loader2,
  AlertOctagon,
} from 'lucide-react';

interface RunsViewProps {
  initialSelectedRunId?: string | null;
  onOpenTriggerModal?: () => void;
}

export const RunsView: React.FC<RunsViewProps> = ({ initialSelectedRunId, onOpenTriggerModal }) => {
  const { projectScope } = useAuth();
  const queryClient = useQueryClient();
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedRunId, setSelectedRunId] = useState<string | null>(initialSelectedRunId || null);
  const [showLogs, setShowLogs] = useState<boolean>(false);
  const [cancelError, setCancelError] = useState<string | null>(null);

  React.useEffect(() => {
    if (initialSelectedRunId) {
      setSelectedRunId(initialSelectedRunId);
    }
  }, [initialSelectedRunId]);

  // 1. Query Runs List with dynamic polling (2.5s if active runs exist, else 15s)
  const {
    data: runs = [],
    isLoading,
  } = useQuery({
    queryKey: ['runs', projectScope],
    queryFn: () => api.getRuns(30, projectScope),
    refetchInterval: (query) => {
      const data = query.state.data;
      const hasActive = data?.some((r) => ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(r.status));
      return hasActive ? 2500 : 15000;
    },
  });

  // Identify first active run (if any) to feature in live operational banner
  const activeRunSummary = runs.find((r) => ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(r.status));
  const activeRunId = activeRunSummary?.run_id || null;

  // 2. Query Active Run Detail for top live operational monitor
  const { data: activeRunDetail } = useQuery({
    queryKey: ['runDetail', activeRunId],
    queryFn: () => (activeRunId ? api.getRunDetail(activeRunId) : null),
    enabled: !!activeRunId,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data && ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(data.status)) {
        return 2500;
      }
      return false;
    },
  });

  // 3. Query Selected Run Detail for drawer
  const {
    data: runDetail,
    isLoading: isLoadingDetail,
  } = useQuery({
    queryKey: ['runDetail', selectedRunId],
    queryFn: () => (selectedRunId ? api.getRunDetail(selectedRunId) : null),
    enabled: !!selectedRunId,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data && ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(data.status)) {
        return 2500;
      }
      return false;
    },
  });

  // 4. Query Run Logs for drawer terminal viewer
  const {
    data: logData,
    isLoading: isLoadingLogs,
    isFetching: isFetchingLogs,
    refetch: refetchLogs,
  } = useQuery({
    queryKey: ['runLogs', selectedRunId],
    queryFn: () => (selectedRunId ? api.getRunLogs(selectedRunId, 200, projectScope) : null),
    enabled: !!selectedRunId && showLogs,
    refetchInterval: showLogs ? 4000 : false,
  });

  const cancelMutation = useMutation({
    mutationFn: (runId: string) => api.cancelRun(runId, projectScope),
    onSuccess: () => {
      setCancelError(null);
      queryClient.invalidateQueries({ queryKey: ['runs'] });
      queryClient.invalidateQueries({ queryKey: ['runDetail', selectedRunId] });
      queryClient.invalidateQueries({ queryKey: ['runDetail', activeRunId] });
      queryClient.invalidateQueries({ queryKey: ['kpis'] });
    },
    onError: (err: any) => {
      setCancelError(err.message || 'Failed to cancel run.');
    },
  });

  const filteredRuns = runs.filter((run) => {
    if (!searchTerm) return true;
    const term = searchTerm.toLowerCase();
    return (
      run.run_id?.toLowerCase().includes(term) ||
      run.source_table?.toLowerCase().includes(term) ||
      run.source_tag?.toLowerCase().includes(term) ||
      run.status?.toLowerCase().includes(term)
    );
  });

  const getStatusBadge = (status: string) => {
    const s = status.toUpperCase();
    if (s === 'SUCCESS') {
      return 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';
    }
    if (s === 'RUNNING' || s === 'DISPATCHED') {
      return 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20 animate-pulse';
    }
    if (s === 'QUEUED') {
      return 'bg-amber-500/10 text-amber-400 border-amber-500/20';
    }
    if (s.includes('FAIL') || s === 'ERROR') {
      return 'bg-rose-500/10 text-rose-400 border-rose-500/20';
    }
    if (s === 'CANCELLED') {
      return 'bg-slate-700/50 text-slate-400 border-slate-600/30';
    }
    return 'bg-slate-800 text-slate-300 border-slate-700';
  };

  return (
    <div className="p-8 space-y-8 max-w-7xl mx-auto relative">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            Execution Runs & Workflows
            {projectScope && (
              <span className="text-xs font-mono px-2.5 py-0.5 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-indigo-300">
                {projectScope}
              </span>
            )}
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            Authoritative run state machines, real-time throughput metrics (rows/min), and storage convergence telemetry.
          </p>
        </div>

        {/* Actions & Search */}
        <div className="flex items-center gap-3 w-full sm:w-auto">
          {onOpenTriggerModal && (
            <button
              onClick={onOpenTriggerModal}
              className="flex items-center gap-2 px-3.5 py-2 bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white text-xs font-semibold rounded-xl shadow-lg shadow-indigo-600/25 transition shrink-0 active:scale-95"
            >
              <Play className="w-3.5 h-3.5 fill-current text-indigo-100" />
              <span>Trigger New Run</span>
            </button>
          )}

          <div className="relative w-full sm:w-72">
            <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search run ID, table, or status..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full bg-[#1e293b]/60 border border-slate-700/80 focus:border-indigo-500 rounded-xl pl-9 pr-3 py-2 text-xs text-white placeholder-slate-500 outline-none transition"
            />
          </div>
        </div>
      </div>

      {/* Active Pipeline Hero Panel (Shown when a run is actively executing) */}
      {activeRunDetail && (
        <div className="space-y-4 animate-fadeIn">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className="relative flex h-3 w-3">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-3 w-3 bg-indigo-500"></span>
              </span>
              <h2 className="text-sm font-bold text-white uppercase tracking-wider">
                Live Active Pipeline Execution
              </h2>
              <span className="text-xs font-mono text-slate-400">
                ({activeRunDetail.run_id})
              </span>
            </div>

            <div className="flex items-center gap-2">
              <button
                onClick={() => setSelectedRunId(activeRunDetail.run_id)}
                className="px-3 py-1 rounded-lg text-xs font-semibold bg-indigo-600 hover:bg-indigo-500 text-white transition shadow-sm"
              >
                Inspect in Drawer
              </button>
              {['RUNNING', 'QUEUED', 'DISPATCHED'].includes(activeRunDetail.status) && (
                <button
                  onClick={() => {
                    if (confirm(`Cancel active run ${activeRunDetail.run_id}?`)) {
                      cancelMutation.mutate(activeRunDetail.run_id);
                    }
                  }}
                  disabled={cancelMutation.isPending}
                  className="px-2.5 py-1 rounded-lg text-xs font-semibold bg-rose-500/10 border border-rose-500/30 text-rose-300 hover:bg-rose-500/20 transition flex items-center gap-1"
                >
                  <StopCircle className="w-3.5 h-3.5" />
                  <span>Cancel</span>
                </button>
              )}
            </div>
          </div>

          <LiveRunMetrics run={activeRunDetail} />
          <WorkflowCanvas run={activeRunDetail} />
        </div>
      )}

      {/* Main Runs Table */}
      <div className="bg-[#111827]/90 border border-slate-800/80 rounded-2xl overflow-hidden shadow-xl">
        <div className="p-4 border-b border-slate-800/80 flex items-center justify-between">
          <div className="flex items-center gap-2 text-xs text-slate-400 font-sans">
            <History className="w-4 h-4 text-indigo-400" />
            <span>
              Showing <strong className="text-white">{filteredRuns.length}</strong> execution runs
            </span>
          </div>
          <span className="text-xs text-slate-400 font-sans">Click any run to view workflow visualization & telemetry</span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-[#0e1626]/90 border-b border-slate-800 text-xs uppercase tracking-wider text-slate-300 font-semibold select-none font-sans">
                <th className="py-3.5 px-4">Status</th>
                <th className="py-3.5 px-4">Run ID</th>
                <th className="py-3.5 px-4">Source & Table</th>
                <th className="py-3.5 px-4">Target Table</th>
                <th className="py-3.5 px-4 text-right">Extracted / Written</th>
                <th className="py-3.5 px-4 text-right">Throughput</th>
                <th className="py-3.5 px-4 text-right">Elapsed</th>
                <th className="py-3.5 px-4 text-right">Completed At</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 text-xs">
              {isLoading ? (
                <tr>
                  <td colSpan={8} className="py-12 text-center text-slate-400 font-sans">
                    Loading execution history...
                  </td>
                </tr>
              ) : filteredRuns.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-12 text-center text-slate-400 font-sans">
                    No execution runs found.
                  </td>
                </tr>
              ) : (
                filteredRuns.map((run) => {
                  const isSelected = selectedRunId === run.run_id;
                  const throughputRPM = run.rows_per_minute || 0;

                  return (
                    <tr
                      key={run.run_id}
                      onClick={() => setSelectedRunId(run.run_id)}
                      className={`cursor-pointer transition duration-150 ${
                        isSelected
                          ? 'bg-indigo-600/15'
                          : 'hover:bg-[#151f33]/60'
                      }`}
                    >
                      <td className="py-3.5 px-4 whitespace-nowrap">
                        <span
                          className={`inline-flex items-center px-2.5 py-0.5 rounded-md text-[11px] font-semibold uppercase tracking-wide border font-sans ${getStatusBadge(
                            run.status
                          )}`}
                        >
                          {run.status}
                        </span>
                      </td>

                      <td className="py-3.5 px-4 whitespace-nowrap font-mono text-xs text-indigo-300">
                        {run.run_id.length > 18 ? `${run.run_id.slice(0, 18)}...` : run.run_id}
                      </td>

                      <td className="py-3.5 px-4 whitespace-nowrap">
                        <div className="font-semibold text-slate-100 font-sans">
                          {run.source_table || 'all_datasets'}
                        </div>
                        <div className="text-xs text-slate-400 font-sans mt-0.5">{run.source_tag}</div>
                      </td>

                      <td className="py-3.5 px-4 whitespace-nowrap font-sans text-xs text-slate-300">
                        {run.target_table || '-'}
                      </td>

                      <td className="py-3.5 px-4 whitespace-nowrap text-right font-sans tabular-nums">
                        <span className="text-slate-400">{run.source_count.toLocaleString()}</span>
                        <span className="text-slate-600 mx-1.5">→</span>
                        <span className="text-emerald-400 font-semibold">{run.target_count.toLocaleString()}</span>
                      </td>

                      {/* Throughput Rows/Min */}
                      <td className="py-3.5 px-4 whitespace-nowrap text-right font-sans tabular-nums text-cyan-400 font-semibold">
                        {throughputRPM > 0 ? `${Math.round(throughputRPM).toLocaleString()} r/m` : '-'}
                      </td>

                      <td className="py-3.5 px-4 whitespace-nowrap text-right font-sans tabular-nums text-slate-400">
                        {run.elapsed_seconds ? `${run.elapsed_seconds.toFixed(1)}s` : '0.0s'}
                      </td>

                      <td className="py-3.5 px-4 whitespace-nowrap text-right text-slate-400 text-xs font-sans tabular-nums">
                        {run.completed_at || '-'}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Slide-out Run Detail Drawer with Full Workflow Canvas */}
      {selectedRunId && (
        <div className="fixed inset-y-0 right-0 w-full max-w-3xl bg-[#111827] border-l border-slate-800/80 shadow-2xl z-50 flex flex-col animate-slideLeft font-sans">
          {/* Drawer Header */}
          <div className="p-6 border-b border-slate-800/80 flex items-start justify-between bg-[#0c1322]/95 backdrop-blur-md">
            <div>
              <span className="text-xs font-semibold px-2.5 py-0.5 rounded-md bg-indigo-500/15 text-indigo-300 border border-indigo-500/25 font-sans">
                Operational Run Workflow
              </span>
              <h2 className="text-lg font-bold text-white font-mono mt-1.5">
                {selectedRunId}
              </h2>
            </div>

            <button
              onClick={() => setSelectedRunId(null)}
              className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          <div className="p-6 flex-1 overflow-y-auto space-y-6">
            {isLoadingDetail ? (
              <div className="py-12 text-center text-slate-500 text-xs flex flex-col items-center justify-center gap-3">
                <Loader2 className="w-6 h-6 animate-spin text-indigo-500" />
                <span>Loading runtime telemetry and state machine...</span>
              </div>
            ) : !runDetail ? (
              <div className="p-4 rounded-xl bg-slate-800/40 text-xs text-slate-400 text-center">
                Run details not available.
              </div>
            ) : (
              <>
                {/* Actions Bar inside Drawer */}
                <div className="flex items-center justify-between gap-3 p-3 rounded-xl bg-[#1e293b]/60 border border-slate-800">
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setShowLogs(!showLogs)}
                      className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition ${
                        showLogs
                          ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30'
                          : 'bg-slate-800 text-slate-300 hover:text-white hover:bg-slate-700'
                      }`}
                    >
                      <Terminal className="w-3.5 h-3.5" />
                      <span>{showLogs ? 'Hide Output Logs' : 'View Terminal Logs'}</span>
                    </button>

                    {showLogs && (
                      <button
                        onClick={() => refetchLogs()}
                        disabled={isFetchingLogs}
                        title="Refresh Logs"
                        className="p-1.5 text-slate-400 hover:text-white rounded-lg hover:bg-slate-800 transition"
                      >
                        <RefreshCw className={`w-3.5 h-3.5 ${isFetchingLogs ? 'animate-spin text-indigo-400' : ''}`} />
                      </button>
                    )}
                  </div>

                  {['RUNNING', 'QUEUED', 'DISPATCHED'].includes(runDetail.status) && (
                    <button
                      onClick={() => {
                        if (confirm(`Are you sure you want to cancel run ${selectedRunId}?`)) {
                          cancelMutation.mutate(selectedRunId!);
                        }
                      }}
                      disabled={cancelMutation.isPending}
                      className="flex items-center gap-1.5 px-3 py-1.5 bg-rose-500/10 border border-rose-500/30 hover:bg-rose-500/20 text-rose-300 rounded-lg text-xs font-semibold transition"
                    >
                      {cancelMutation.isPending ? (
                        <Loader2 className="w-3.5 h-3.5 animate-spin" />
                      ) : (
                        <StopCircle className="w-3.5 h-3.5 text-rose-400" />
                      )}
                      <span>Cancel Run</span>
                    </button>
                  )}
                </div>

                {cancelError && (
                  <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 text-xs flex items-center gap-2 animate-fadeIn">
                    <AlertOctagon className="w-4 h-4 text-rose-400 flex-shrink-0" />
                    <span>{cancelError}</span>
                  </div>
                )}

                {/* Live Logs Terminal Viewer */}
                {showLogs && (
                  <div className="bg-[#050811] border border-slate-800 rounded-xl overflow-hidden shadow-inner animate-fadeIn">
                    <div className="px-4 py-2 border-b border-slate-800/80 bg-[#090d16] flex items-center justify-between text-[11px] text-slate-400">
                      <div className="flex items-center gap-2">
                        <Terminal className="w-3.5 h-3.5 text-indigo-400" />
                        <span className="font-mono">Engine Output (Last {logData?.lines_returned || 0} lines)</span>
                      </div>
                      <span className="text-[10px] text-slate-500">Auto-refresh active</span>
                    </div>
                    <div className="p-4 max-h-64 overflow-y-auto font-mono text-[11px] leading-relaxed text-emerald-400/90 whitespace-pre-wrap select-text">
                      {isLoadingLogs ? (
                        <div className="text-slate-500 flex items-center gap-2">
                          <Loader2 className="w-3.5 h-3.5 animate-spin text-indigo-400" />
                          <span>Streaming execution logs from JobManager...</span>
                        </div>
                      ) : logData?.logs ? (
                        logData.logs
                      ) : (
                        <span className="text-slate-500">No logs emitted yet for this run.</span>
                      )}
                    </div>
                  </div>
                )}

                {/* 1. Live Operational Metrics HUD */}
                <LiveRunMetrics run={runDetail} />

                {/* 2. Visual Medallion Workflow Canvas DAG */}
                <WorkflowCanvas run={runDetail} />

                {/* Error diagnostics if any */}
                {runDetail.error_message && (
                  <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 text-xs">
                    <span className="font-bold block text-rose-200 mb-1">Execution Diagnostics</span>
                    <pre className="font-mono text-[11px] whitespace-pre-wrap">{runDetail.error_message}</pre>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
