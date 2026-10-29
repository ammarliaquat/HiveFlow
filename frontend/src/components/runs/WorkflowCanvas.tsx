import React from 'react';
import type { RunDetailResponse, RunStageItem } from '../../api/types';
import {
  ShieldCheck,
  Database,
  Filter,
  Send,
  CheckCircle2,
  AlertTriangle,
  Loader2,
  Clock,
  ArrowRight,
  Minus,
} from 'lucide-react';

interface WorkflowCanvasProps {
  run: RunDetailResponse;
}

interface CanvasStageDef {
  key: string;
  label: string;
  layerPill: string;
  subtitle: string;
  icon: React.ReactNode;
  status: 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'NOT_REQUESTED';
  rowsProcessed?: number;
  rowsPerMin?: number;
}

export const WorkflowCanvas: React.FC<WorkflowCanvasProps> = ({ run }) => {
  // Helper to find stage from runDetail.stages
  const findStage = (keywords: string[]): RunStageItem | undefined => {
    return run.stages?.find((s) =>
      keywords.some((kw) => s.stage_name.toLowerCase().includes(kw))
    );
  };

  const bronzeStage = findStage(['bronze', 'extract', 'landing']);
  const silverStage = findStage(['silver', 'history', 'cleanse']);
  const goldStage = findStage(['gold', 'target', 'publish']);

  // Normalize stage status
  const normalizeStatus = (
    stage: RunStageItem | undefined,
    fallbackIfRunDone: 'PENDING' | 'SUCCESS' = 'PENDING'
  ): 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'NOT_REQUESTED' => {
    if (!stage) {
      if (run.status === 'SUCCESS') return fallbackIfRunDone;
      return 'PENDING';
    }
    const st = stage.status?.toUpperCase() || 'PENDING';
    if (st.includes('FAIL')) return 'FAILED';
    if (st === 'RUNNING' || st === 'RUNNING_STALLED') return 'RUNNING';
    if (st === 'NOT_REQUESTED') return 'NOT_REQUESTED';
    if (st === 'SUCCESS' || st === 'SKIPPED') return 'SUCCESS';
    return 'PENDING';
  };

  // Determine Preflight status
  const preflightStatus: 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED' = (() => {
    if (run.status === 'FAILED' && (!run.stages || run.stages.length === 0)) return 'FAILED';
    if (['QUEUED', 'DISPATCHED'].includes(run.status)) return 'RUNNING';
    return 'SUCCESS';
  })();

  // Determine Reconciliation status
  const reconcileStatus: 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'NOT_REQUESTED' = (() => {
    if (run.reconciliation_status === 'PASS') return 'SUCCESS';
    if (run.reconciliation_status === 'FAIL') return 'FAILED';
    if (run.status === 'SUCCESS') return 'SUCCESS';
    // If the overall run has terminated (FAILED, CANCELLED, TERMINATED), it is no longer running
    if (['FAILED', 'CANCELLED', 'TERMINATED'].includes(run.status)) {
      return run.reconciliation_status === 'FAIL' ? 'FAILED' : 'NOT_REQUESTED';
    }
    // Only if the run is actively running and Gold has completed
    if (['RUNNING', 'DISPATCHED'].includes(run.status) && (goldStage?.status === 'SUCCESS' || goldStage?.status === 'COMPLETED')) {
      return 'RUNNING';
    }
    return 'PENDING';
  })();

  const canvasStages: CanvasStageDef[] = [
    {
      key: 'preflight',
      label: 'Preflight & Scope',
      layerPill: 'GATE 0',
      subtitle: 'Schema Certification & Scope Lease',
      icon: <ShieldCheck className="w-4 h-4" />,
      status: preflightStatus,
      rowsProcessed: undefined,
      rowsPerMin: undefined,
    },
    {
      key: 'bronze',
      label: 'Bronze Landing',
      layerPill: 'BRONZE',
      subtitle: 'Immutable Parquet Raw Snapshot',
      icon: <Database className="w-4 h-4" />,
      status: normalizeStatus(bronzeStage, 'SUCCESS'),
      rowsProcessed: bronzeStage?.rows_processed || run.extracted_count,
      rowsPerMin: bronzeStage?.throughput_rows_min,
    },
    {
      key: 'silver',
      label: 'Silver Cleanse',
      layerPill: 'SILVER',
      subtitle: 'Deduplication & Entity Keys',
      icon: <Filter className="w-4 h-4" />,
      status: normalizeStatus(silverStage, 'SUCCESS'),
      rowsProcessed: silverStage?.rows_processed || (run.status === 'SUCCESS' ? run.extracted_count : 0),
      rowsPerMin: silverStage?.throughput_rows_min,
    },
    {
      key: 'gold',
      label: 'Gold Publish',
      layerPill: 'GOLD',
      subtitle: 'Transactional Target UPSERT',
      icon: <Send className="w-4 h-4" />,
      status: normalizeStatus(goldStage, 'SUCCESS'),
      rowsProcessed: goldStage?.rows_processed || run.rows_written,
      rowsPerMin: goldStage?.throughput_rows_min || run.rows_per_minute,
    },
    {
      key: 'reconcile',
      label: 'Convergence Gate',
      layerPill: 'AUDIT',
      subtitle: 'Key-Set Convergence Audit',
      icon: <CheckCircle2 className="w-4 h-4" />,
      status: reconcileStatus,
      rowsProcessed: run.rows_written,
      rowsPerMin: undefined,
    },
  ];

  // Helper styles by status
  const getStageStyles = (status: CanvasStageDef['status']) => {
    switch (status) {
      case 'RUNNING':
        return {
          card: 'bg-indigo-950/40 border-indigo-500 shadow-lg shadow-indigo-500/20 ring-1 ring-indigo-500/50',
          iconBg: 'bg-indigo-600 text-white animate-pulse',
          badge: 'bg-indigo-500/20 text-indigo-300 border-indigo-500/40',
          pill: 'bg-indigo-600 text-white',
          pulseLine: true,
        };
      case 'SUCCESS':
        return {
          card: 'bg-emerald-950/20 border-emerald-500/40 hover:border-emerald-500/60 shadow-sm',
          iconBg: 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30',
          badge: 'bg-emerald-500/10 text-emerald-300 border-emerald-500/20',
          pill: 'bg-emerald-900/60 text-emerald-300 border border-emerald-700/50',
          pulseLine: false,
        };
      case 'FAILED':
        return {
          card: 'bg-rose-950/30 border-rose-500/50 shadow-md shadow-rose-950/50',
          iconBg: 'bg-rose-500/20 text-rose-400 border border-rose-500/30',
          badge: 'bg-rose-500/10 text-rose-300 border-rose-500/20',
          pill: 'bg-rose-900/60 text-rose-300 border border-rose-700/50',
          pulseLine: false,
        };
      case 'NOT_REQUESTED':
        return {
          card: 'bg-slate-900/30 border-slate-800/60 border-dashed opacity-50',
          iconBg: 'bg-slate-800/50 text-slate-500',
          badge: 'bg-slate-800 text-slate-500 border-slate-700',
          pill: 'bg-slate-800 text-slate-500',
          pulseLine: false,
        };
      default:
        return {
          card: 'bg-[#1e293b]/30 border-slate-800 hover:border-slate-700',
          iconBg: 'bg-slate-800 text-slate-400',
          badge: 'bg-slate-800/80 text-slate-400 border-slate-700',
          pill: 'bg-slate-800 text-slate-400',
          pulseLine: false,
        };
    }
  };

  return (
    <div className="bg-[#111827]/95 border border-slate-800/80 rounded-2xl p-5 shadow-2xl relative overflow-hidden font-sans">
      {/* Background visual accents */}
      <div className="absolute top-0 right-0 w-80 h-32 bg-indigo-600/5 blur-3xl pointer-events-none rounded-full" />
      <div className="absolute bottom-0 left-0 w-80 h-32 bg-cyan-600/5 blur-3xl pointer-events-none rounded-full" />

      {/* Workflow Canvas Top Bar */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 mb-5 border-b border-slate-800/80 gap-3">
        <div>
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-cyan-400 shadow-sm shadow-cyan-400/50" />
            <h3 className="text-xs font-bold uppercase tracking-wider text-white font-sans">
              Governed Medallion Pipeline Workflow
            </h3>
            <span className="text-xs font-semibold px-2.5 py-0.5 rounded-md bg-slate-800 text-slate-300 border border-slate-700/80 font-sans">
              HiveFlow Engine
            </span>
          </div>
          <p className="text-xs text-slate-400 mt-1 font-sans">
            Real-time state machine projection for dataset:{' '}
            <strong className="text-white font-sans">{run.dataset_id || 'all_datasets'}</strong> in{' '}
            <strong className="text-indigo-300 font-sans">{run.project_id}</strong>
          </p>
        </div>

        {/* Status Pill */}
        <div className="flex items-center gap-2">
          <span
            className={`px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider font-sans border flex items-center gap-1.5 ${
              run.status === 'RUNNING'
                ? 'bg-indigo-500/15 border-indigo-500/40 text-indigo-300 animate-pulse'
                : run.status === 'SUCCESS'
                ? 'bg-emerald-500/15 border-emerald-500/30 text-emerald-300'
                : run.status.includes('FAIL')
                ? 'bg-rose-500/15 border-rose-500/30 text-rose-300'
                : 'bg-slate-800 border-slate-700 text-slate-400'
            }`}
          >
            {run.status === 'RUNNING' && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
            {run.status === 'SUCCESS' && <CheckCircle2 className="w-3.5 h-3.5" />}
            {run.status.includes('FAIL') && <AlertTriangle className="w-3.5 h-3.5" />}
            <span>{run.status}</span>
          </span>
        </div>
      </div>

      {/* Visual DAG Nodes Container */}
      <div className="grid grid-cols-1 md:grid-cols-5 gap-3 relative">
        {canvasStages.map((stage, idx) => {
          const styles = getStageStyles(stage.status);
          const isLast = idx === canvasStages.length - 1;

          return (
            <div key={stage.key} className="flex flex-col relative group">
              {/* Stage Node Card */}
              <div
                className={`p-4 rounded-xl border transition-all duration-300 flex flex-col justify-between h-full min-h-[148px] ${styles.card}`}
              >
                {/* Card Header: Icon & Layer Pill */}
                <div className="flex items-center justify-between mb-3">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${styles.iconBg}`}>
                    {stage.status === 'RUNNING' ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      stage.icon
                    )}
                  </div>

                  <span className={`text-[10px] font-sans font-bold px-2 py-0.5 rounded uppercase tracking-wider ${styles.pill}`}>
                    {stage.layerPill}
                  </span>
                </div>

                {/* Stage Info */}
                <div>
                  <h4 className="text-xs font-bold text-white tracking-tight flex items-center justify-between font-sans">
                    <span>{stage.label}</span>
                  </h4>
                  <p className="text-xs text-slate-400 mt-0.5 line-clamp-1 font-sans">{stage.subtitle}</p>
                </div>

                {/* Metric Footer */}
                <div className="mt-3 pt-2 border-t border-slate-800/60 flex items-center justify-between text-xs font-sans">
                  {stage.status === 'NOT_REQUESTED' ? (
                    <span className="text-slate-500 italic flex items-center gap-1">
                      <Minus className="w-3 h-3" /> Deferred
                    </span>
                  ) : stage.rowsProcessed !== undefined ? (
                    <div className="tabular-nums">
                      <span className="text-slate-200 font-semibold">
                        {stage.rowsProcessed.toLocaleString()}
                      </span>{' '}
                      <span className="text-slate-400 text-[11px]">rows</span>
                    </div>
                  ) : (
                    <span className="text-slate-400 flex items-center gap-1 text-[11px]">
                      <Clock className="w-3 h-3" /> Fencing
                    </span>
                  )}

                  {/* Throughput per stage */}
                  {stage.rowsPerMin ? (
                    <span className="text-cyan-400 font-semibold tabular-nums">
                      {Math.round(stage.rowsPerMin).toLocaleString()} r/m
                    </span>
                  ) : (
                    <span
                      className={`px-2 py-0.5 rounded text-[10px] font-semibold border ${styles.badge}`}
                    >
                      {stage.status}
                    </span>
                  )}
                </div>
              </div>

              {/* Connecting arrow for larger screens (hidden on mobile and last element) */}
              {!isLast && (
                <div className="hidden md:flex absolute -right-3 top-1/2 -translate-y-1/2 z-10 pointer-events-none">
                  <div
                    className={`w-6 h-6 rounded-full border flex items-center justify-center text-[10px] shadow-sm transition ${
                      stage.status === 'SUCCESS'
                        ? 'bg-emerald-950 border-emerald-500/50 text-emerald-400'
                        : stage.status === 'RUNNING'
                        ? 'bg-indigo-950 border-indigo-500 text-indigo-300 animate-pulse'
                        : 'bg-[#111827] border-slate-800 text-slate-500'
                    }`}
                  >
                    <ArrowRight className="w-3 h-3" />
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
};
