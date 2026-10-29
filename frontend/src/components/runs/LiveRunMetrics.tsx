import React from 'react';
import type { RunDetailResponse } from '../../api/types';
import {
  Gauge,
  Activity,
  Layers,
  Timer,
  AlertTriangle,
  Flame,
  ArrowRight,
} from 'lucide-react';

interface LiveRunMetricsProps {
  run: RunDetailResponse;
}

export const LiveRunMetrics: React.FC<LiveRunMetricsProps> = ({ run }) => {
  const isRunning = ['RUNNING', 'DISPATCHED', 'QUEUED'].includes(run.status);
  const isCompleted = run.status === 'SUCCESS';
  const isFailed = ['FAILED', 'RECONCILIATION_FAILED'].includes(run.status);

  // Format estimated time remaining
  const formatETA = (seconds: number | null) => {
    if (seconds === null || seconds <= 0) return null;
    if (seconds < 60) return `~${Math.round(seconds)}s remaining`;
    const mins = Math.floor(seconds / 60);
    const secs = Math.round(seconds % 60);
    return `~${mins}m ${secs}s remaining`;
  };

  const etaFormatted = formatETA(run.estimated_remaining_seconds);

  // Convergence percent
  const totalTarget = run.extracted_count || run.rows_read || 0;
  const progressPercent = totalTarget > 0
    ? Math.min(100, Math.round((run.rows_written / totalTarget) * 100))
    : (isCompleted ? 100 : 0);

  return (
    <div className="space-y-4 font-sans">
      {/* Worker Liveness & Stall Alert Banner */}
      {run.is_stalled && isRunning && (
        <div className="p-3.5 rounded-xl bg-amber-500/10 border border-amber-500/30 flex items-center justify-between text-xs text-amber-300 animate-pulse font-sans">
          <div className="flex items-center gap-2">
            <AlertTriangle className="w-4 h-4 text-amber-400 flex-shrink-0" />
            <span className="font-semibold">
              Execution Heartbeat Delay: Worker has not reported in {run.last_heartbeat_age_seconds ? `${run.last_heartbeat_age_seconds}s` : '>45s'}.
            </span>
          </div>
          <span className="text-[10px] font-sans px-2 py-0.5 rounded bg-amber-500/20 text-amber-200 border border-amber-500/30 font-bold uppercase">
            POTENTIAL STALL
          </span>
        </div>
      )}

      {/* Grid of Key Operational Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {/* Metric 1: Processing Speed / Rows Per Minute */}
        <div className="p-4 rounded-xl bg-[#131d31]/80 border border-slate-800/80 flex flex-col justify-between hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between">
            <span className="text-xs uppercase tracking-wider font-semibold text-slate-400 flex items-center gap-1.5 font-sans">
              <Gauge className="w-3.5 h-3.5 text-cyan-400" />
              Throughput
            </span>
            {isRunning && run.rows_per_minute > 0 && (
              <span className="flex items-center gap-1 text-[11px] font-sans text-cyan-400 animate-pulse font-semibold">
                <Flame className="w-3 h-3 text-cyan-400 fill-current" /> LIVE
              </span>
            )}
          </div>

          <div className="mt-2">
            <div className="text-xl font-extrabold text-white font-sans tabular-nums flex items-baseline gap-1.5">
              <span>{Math.round(run.rows_per_minute || 0).toLocaleString()}</span>
              <span className="text-xs font-semibold text-cyan-400">r/min</span>
            </div>
            <div className="text-xs text-slate-400 font-sans tabular-nums mt-0.5">
              {run.throughput_rows_sec ? `${run.throughput_rows_sec.toFixed(1)} rows/s` : '0.0 rows/s'}
            </div>
          </div>
        </div>

        {/* Metric 2: Rows Processed & Written */}
        <div className="p-4 rounded-xl bg-[#131d31]/80 border border-slate-800/80 flex flex-col justify-between hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between">
            <span className="text-xs uppercase tracking-wider font-semibold text-slate-400 flex items-center gap-1.5 font-sans">
              <Layers className="w-3.5 h-3.5 text-indigo-400" />
              Data Ingestion
            </span>
            <span className="text-xs font-sans font-bold text-slate-300 tabular-nums">
              {progressPercent}%
            </span>
          </div>

          <div className="mt-2">
            <div className="text-sm font-bold text-white font-sans tabular-nums flex items-center gap-1.5">
              <span className="text-slate-300">{run.extracted_count.toLocaleString()}</span>
              <ArrowRight className="w-3.5 h-3.5 text-slate-500" />
              <span className="text-emerald-400 font-extrabold">{run.rows_written.toLocaleString()}</span>
            </div>
            {/* Progress bar */}
            <div className="w-full bg-slate-800 h-1.5 rounded-full mt-2.5 overflow-hidden">
              <div
                className={`h-full transition-all duration-500 rounded-full ${
                  isFailed ? 'bg-rose-500' : 'bg-gradient-to-r from-indigo-500 to-emerald-400'
                }`}
                style={{ width: `${progressPercent}%` }}
              />
            </div>
          </div>
        </div>

        {/* Metric 3: Chunks & Partitions */}
        <div className="p-4 rounded-xl bg-[#131d31]/80 border border-slate-800/80 flex flex-col justify-between hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between">
            <span className="text-xs uppercase tracking-wider font-semibold text-slate-400 flex items-center gap-1.5 font-sans">
              <Activity className="w-3.5 h-3.5 text-emerald-400" />
              Batch Chunks
            </span>
            {run.chunk_progress_pct !== null && (
              <span className="text-xs font-sans font-bold text-emerald-400 tabular-nums">
                {run.chunk_progress_pct}%
              </span>
            )}
          </div>

          <div className="mt-2">
            <div className="text-lg font-bold text-white font-sans tabular-nums">
              {run.total_chunks ? (
                <>
                  <span className="text-white">{run.chunks_completed ?? run.current_chunk ?? 0}</span>
                  <span className="text-slate-400 text-sm"> / {run.total_chunks} chunks</span>
                </>
              ) : (
                <span className="text-xs text-slate-300 font-sans">Single Execution Batch</span>
              )}
            </div>
            <div className="text-xs text-slate-400 mt-0.5 font-sans">
              {run.total_chunks ? 'Partitioned streaming load' : 'Direct transactional load'}
            </div>
          </div>
        </div>

        {/* Metric 4: Elapsed Time & ETA / Liveness */}
        <div className="p-4 rounded-xl bg-[#131d31]/80 border border-slate-800/80 flex flex-col justify-between hover:border-slate-700/80 transition">
          <div className="flex items-center justify-between">
            <span className="text-xs uppercase tracking-wider font-semibold text-slate-400 flex items-center gap-1.5 font-sans">
              <Timer className="w-3.5 h-3.5 text-purple-400" />
              Runtime Clock
            </span>
            <div className="flex items-center gap-1.5">
              <div
                className={`w-2 h-2 rounded-full ${
                  isRunning
                    ? run.is_stalled
                      ? 'bg-amber-400 animate-ping'
                      : 'bg-emerald-400 animate-ping'
                    : isCompleted
                    ? 'bg-emerald-500'
                    : 'bg-slate-600'
                }`}
              />
              <span className="text-xs font-sans font-semibold text-slate-300 uppercase">
                {isRunning ? (run.is_stalled ? 'STALLED' : 'ACTIVE') : run.status}
              </span>
            </div>
          </div>

          <div className="mt-2">
            <div className="text-lg font-bold text-white font-sans tabular-nums">
              {run.elapsed_seconds ? `${run.elapsed_seconds.toFixed(1)}s` : '0.0s'}
            </div>
            <div className="text-xs font-sans text-purple-300 mt-0.5">
              {isRunning
                ? (etaFormatted || (run.last_heartbeat_age_seconds ? `hb: ${run.last_heartbeat_age_seconds}s ago` : 'Calculating ETA...'))
                : (isCompleted ? 'Finished Converged' : 'Run Terminated')}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
