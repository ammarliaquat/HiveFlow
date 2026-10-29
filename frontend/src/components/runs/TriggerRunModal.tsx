import React, { useState, useEffect, useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../../context/AuthContext';
import { api } from '../../api/client';
import type { TriggerRunRequest, PipelineItem } from '../../api/types';
import {
  Play,
  X,
  AlertTriangle,
  Layers,
  ShieldCheck,
  Loader2,
  CheckCircle2,
  Database,
  Table,
  Search,
  CheckSquare,
  Square,
  Check,
  AlertCircle,
  RefreshCw,
} from 'lucide-react';

interface TriggerRunModalProps {
  isOpen: boolean;
  onClose: () => void;
  onRunTriggered?: (runId: string) => void;
}

export const TriggerRunModal: React.FC<TriggerRunModalProps> = ({
  isOpen,
  onClose,
  onRunTriggered,
}) => {
  const { user, projectScope, setProjectScope } = useAuth();
  const queryClient = useQueryClient();

  const allowedSources = user?.allowed_projects && user.allowed_projects.length > 0
    ? user.allowed_projects
    : ['source_erp_db', 'opf_oracle_housing'];

  const [sourceTag, setSourceTag] = useState<string>(
    projectScope && allowedSources.includes(projectScope)
      ? projectScope
      : allowedSources[0] || 'source_erp_db'
  );
  const [targetTag, setTargetTag] = useState<string>('');
  const [datasetMode, setDatasetMode] = useState<'ALL' | 'SELECTED'>('ALL');
  const [selectedTableNames, setSelectedTableNames] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [executionScope, setExecutionScope] = useState<'FULL' | 'BRONZE' | 'SILVER' | 'GOLD'>('FULL');
  const [priority, setPriority] = useState<number>(5);
  const [confirmed, setConfirmed] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [rejectionData, setRejectionData] = useState<{
    rejectionCode?: string;
    existingActiveRunId?: string;
  } | null>(null);

  // Fetch registered datasets for the selected source project system
  const {
    data: pipelineData,
    isLoading: isLoadingDatasets,
    isError: isErrorDatasets,
    error: datasetError,
    refetch: refetchDatasets,
  } = useQuery({
    queryKey: ['pipelines', sourceTag],
    queryFn: () => api.getPipelines(sourceTag),
    enabled: isOpen && !!sourceTag,
  });

  // Support both direct array and object with .pipelines property
  const availableTables: PipelineItem[] = useMemo(() => {
    if (Array.isArray(pipelineData)) return pipelineData;
    return (pipelineData as any)?.pipelines || [];
  }, [pipelineData]);

  // Derive distinct target destination systems routed from this source
  const availableTargetTags = useMemo(() => {
    const tags = Array.from(
      new Set(
        availableTables
          .map((t) => t.target_tag)
          .filter((tag): tag is string => Boolean(tag && tag.trim()))
      )
    );
    return tags.sort();
  }, [availableTables]);

  // Auto-sync active target destination whenever source or targets change
  useEffect(() => {
    if (availableTargetTags.length > 0) {
      setTargetTag((prev) => {
        if (prev && availableTargetTags.includes(prev)) return prev;
        const prod = availableTargetTags.find((t) => t.toLowerCase().includes('prod'));
        return prod || availableTargetTags[0];
      });
    } else {
      setTargetTag('');
    }
  }, [availableTargetTags]);

  // Datasets scoped to the selected target destination system
  const tablesForTarget = useMemo(() => {
    if (!targetTag) return availableTables;
    return availableTables.filter(
      (t) => !t.target_tag || t.target_tag.toLowerCase() === targetTag.toLowerCase()
    );
  }, [availableTables, targetTag]);

  // Keep selectedTableNames in sync when target-scoped datasets change
  useEffect(() => {
    if (tablesForTarget.length > 0) {
      const getTableName = (t: PipelineItem) => t.source_table || t.table_name || '';
      const validNames = new Set(tablesForTarget.map(getTableName).filter(Boolean));
      setSelectedTableNames((prev) => {
        // Retain previously selected tables that are still valid for this target
        const retained = prev.filter((name) => validNames.has(name));
        if (retained.length > 0) {
          return retained;
        }
        // Default to the first active dataset for this target
        const firstActive = tablesForTarget.find((t) => t.is_active) || tablesForTarget[0];
        const defaultName = getTableName(firstActive);
        return defaultName ? [defaultName] : [];
      });
    } else {
      setSelectedTableNames([]);
    }
  }, [tablesForTarget]);

  // Reset search and selection when changing source system
  const handleSourceChange = (newSource: string) => {
    setSourceTag(newSource);
    setTargetTag('');
    setSearchQuery('');
    setErrorMsg(null);
    setRejectionData(null);
  };

  // Filtered dataset list according to search query within target-scoped datasets
  const filteredTables = useMemo(() => {
    if (!searchQuery.trim()) return tablesForTarget;
    const q = searchQuery.toLowerCase().trim();
    return tablesForTarget.filter((tbl) => {
      const source = (tbl.source_table || tbl.table_name || '').toLowerCase();
      const target = (tbl.target_table || tbl.target_table_name || '').toLowerCase();
      const tag = (tbl.target_tag || '').toLowerCase();
      return source.includes(q) || target.includes(q) || tag.includes(q);
    });
  }, [tablesForTarget, searchQuery]);

  // Selection toggle handlers
  const toggleTable = (tableName: string) => {
    setSelectedTableNames((prev) =>
      prev.includes(tableName)
        ? prev.filter((name) => name !== tableName)
        : [...prev, tableName]
    );
  };

  const selectAllVisible = () => {
    const visibleNames = filteredTables.map((t) => t.source_table || t.table_name || '').filter(Boolean);
    setSelectedTableNames((prev) => Array.from(new Set([...prev, ...visibleNames])));
  };

  const clearSelection = () => {
    setSelectedTableNames([]);
  };

  // Resolve selected table objects for metadata inspection (e.g. target_tag safety)
  const selectedTableObjects = useMemo(() => {
    const selectedSet = new Set(selectedTableNames);
    return tablesForTarget.filter((t) => selectedSet.has(t.source_table || t.table_name || ''));
  }, [tablesForTarget, selectedTableNames]);

  // Target tag consistency check across selected tables (ADR-006 & run_governed V1 invariant)
  const selectedTargetTags = useMemo(() => {
    return Array.from(
      new Set(
        selectedTableObjects
          .map((t) => t.target_tag)
          .filter((tag): tag is string => Boolean(tag))
      )
    );
  }, [selectedTableObjects]);

  const hasConflictingTargetTags = datasetMode === 'SELECTED' && selectedTargetTags.length > 1;

  const triggerMutation = useMutation({
    mutationFn: (payload: TriggerRunRequest) => api.triggerRun(payload, payload.source_tag),
    onSuccess: (data) => {
      // Auto-align global project scope with the triggered pipeline source
      if (sourceTag && projectScope !== sourceTag) {
        setProjectScope(sourceTag);
      }
      queryClient.invalidateQueries({ queryKey: ['runs'] });
      queryClient.invalidateQueries({ queryKey: ['clusterRuns'] });
      queryClient.invalidateQueries({ queryKey: ['kpis'] });
      queryClient.invalidateQueries({ queryKey: ['activity'] });
      if (data.run_id) {
        queryClient.invalidateQueries({ queryKey: ['runDetail', data.run_id] });
      }
      queryClient.refetchQueries({ queryKey: ['runs'] });
      queryClient.refetchQueries({ queryKey: ['clusterRuns'] });
      queryClient.refetchQueries({ queryKey: ['kpis'] });
      if (data.run_id && onRunTriggered) {
        onRunTriggered(data.run_id);
      }
      onClose();
    },
    onError: (err: any) => {
      setErrorMsg(err.message || 'Pipeline trigger was rejected by server.');
      if (err.data) {
        setRejectionData({
          rejectionCode: err.data.rejection_code,
          existingActiveRunId: err.data.existing_active_run_id,
        });
      }
    },
  });

  if (!isOpen) return null;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!confirmed || !sourceTag) return;

    if (datasetMode === 'SELECTED') {
      if (selectedTableNames.length === 0) {
        setErrorMsg('Please select at least one dataset to execute.');
        return;
      }
      if (hasConflictingTargetTags) {
        setErrorMsg(
          `Target tag conflict: selected datasets span multiple destinations (${selectedTargetTags.join(
            ', '
          )}). In run_governed V1, all tables must share one target_tag.`
        );
        return;
      }
    }

    setErrorMsg(null);
    setRejectionData(null);

    const selectedIds: number[] = selectedTableObjects
      .map((t) => t.table_id)
      .filter((id): id is number => typeof id === 'number');

    triggerMutation.mutate({
      source_tag: sourceTag,
      target_tag: targetTag || undefined,
      source_table:
        datasetMode === 'SELECTED'
          ? selectedTableNames.length === 1
            ? selectedTableNames[0]
            : `${selectedTableNames.length} datasets`
          : undefined,
      source_table_ids: datasetMode === 'SELECTED' ? selectedIds : undefined,
      execution_scope: executionScope,
      priority,
    });
  };

  const scopeCards: {
    key: 'FULL' | 'BRONZE' | 'SILVER' | 'GOLD';
    title: string;
    subtitle: string;
    badge: string;
  }[] = [
    {
      key: 'FULL',
      title: 'Full Medallion Pipeline',
      subtitle: 'Source → Bronze landing → Silver cleanse → Gold target publication → Reconcile',
      badge: 'RECOMMENDED',
    },
    {
      key: 'BRONZE',
      title: 'Bronze Extract Only',
      subtitle: 'Raw source ingestion into immutable Parquet snapshot. Silver & Gold deferred.',
      badge: 'INGEST',
    },
    {
      key: 'SILVER',
      title: 'Silver Cleansing Only',
      subtitle: 'Reads verified Bronze artifact and updates Silver history with schema governance.',
      badge: 'CLEANSE',
    },
    {
      key: 'GOLD',
      title: 'Gold Publication Only',
      subtitle: 'Reads verified Silver data and writes to target database with deterministic UPSERT.',
      badge: 'PUBLISH',
    },
  ];

  const isFormValid =
    confirmed &&
    !triggerMutation.isPending &&
    (datasetMode === 'ALL' || (selectedTableNames.length > 0 && !hasConflictingTargetTags));

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 dark:bg-black/75 backdrop-blur-sm animate-fadeIn">
      <div className="w-full max-w-2xl bg-white dark:bg-[#0f172a] border border-slate-200 dark:border-slate-700/70 rounded-2xl shadow-2xl shadow-slate-900/20 dark:shadow-black/80 overflow-hidden flex flex-col max-h-[88vh]">
        {/* Modal Header */}
        <div className="p-5 border-b border-slate-200 dark:border-slate-800/80 flex items-center justify-between bg-slate-50/90 dark:bg-[#0c1322]/95 shrink-0">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-indigo-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-indigo-500/25">
              <Play className="w-4 h-4 text-white fill-current ml-0.5" />
            </div>
            <div>
              <h2 className="text-base font-bold text-slate-900 dark:text-white tracking-tight font-sans">
                Trigger Pipeline Execution
              </h2>
              <p className="text-xs text-slate-500 dark:text-slate-400 font-sans mt-0.5">
                Governed Medallion Processing · HiveFlow Engine
              </p>
            </div>
          </div>

          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 dark:hover:text-white hover:bg-slate-100 dark:hover:bg-slate-800 transition"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Modal Body Form */}
        <form onSubmit={handleSubmit} className="flex flex-col flex-1 overflow-hidden min-h-0">
          <div className="p-6 space-y-5 overflow-y-auto flex-1 custom-scrollbar">
            {/* Error / Rejection Banner */}
            {errorMsg && (
              <div className="p-4 rounded-xl bg-rose-50 dark:bg-rose-500/10 border border-rose-200 dark:border-rose-500/20 flex items-start gap-3 text-rose-800 dark:text-rose-300 text-xs animate-fadeIn font-sans">
                <AlertTriangle className="w-5 h-5 flex-shrink-0 mt-0.5 text-rose-600 dark:text-rose-400" />
                <div>
                  <p className="font-bold text-rose-900 dark:text-rose-200">Execution Request Rejected</p>
                  <p className="mt-0.5 leading-relaxed">{errorMsg}</p>
                  {rejectionData?.existingActiveRunId && (
                    <p className="mt-1 text-xs text-amber-700 dark:text-amber-300 font-mono">
                      Conflicting active run holding scope lock: {rejectionData.existingActiveRunId}
                    </p>
                  )}
                </div>
              </div>
            )}

            {/* Source & Target System Selection (Multi-Target Routing) */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300 mb-2 flex items-center gap-1.5 font-sans">
                  <Database className="w-3.5 h-3.5 text-indigo-600 dark:text-indigo-400" />
                  Source Project System
                </label>
                <select
                  value={sourceTag}
                  onChange={(e) => handleSourceChange(e.target.value)}
                  className="w-full bg-white dark:bg-[#151f33] border border-slate-300 dark:border-slate-700/80 hover:border-slate-400 dark:hover:border-slate-600 rounded-xl px-4 py-2.5 text-sm text-slate-800 dark:text-slate-100 font-medium outline-none focus:ring-2 focus:ring-indigo-500/20 focus:border-indigo-500 transition cursor-pointer font-sans shadow-sm"
                >
                  {allowedSources.map((tag) => (
                    <option key={tag} value={tag}>
                      {tag}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300 mb-2 flex items-center justify-between font-sans">
                  <span className="flex items-center gap-1.5">
                    <Layers className="w-3.5 h-3.5 text-cyan-600 dark:text-cyan-400" />
                    Target Destination System
                  </span>
                  {availableTargetTags.length > 1 && (
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-cyan-100 dark:bg-cyan-500/10 text-cyan-800 dark:text-cyan-300 border border-cyan-200 dark:border-cyan-500/20 font-sans font-medium">
                      {availableTargetTags.length} destinations
                    </span>
                  )}
                </label>
                <select
                  value={targetTag}
                  onChange={(e) => {
                    setTargetTag(e.target.value);
                    setErrorMsg(null);
                  }}
                  disabled={availableTargetTags.length === 0}
                  className="w-full bg-white dark:bg-[#151f33] border border-slate-300 dark:border-slate-700/80 hover:border-slate-400 dark:hover:border-slate-600 rounded-xl px-4 py-2.5 text-sm text-slate-800 dark:text-slate-100 font-medium outline-none focus:ring-2 focus:ring-cyan-500/20 focus:border-cyan-500 transition cursor-pointer font-sans disabled:opacity-50 shadow-sm"
                >
                  {availableTargetTags.length > 0 ? (
                    availableTargetTags.map((tag) => (
                      <option key={tag} value={tag}>
                        {tag}
                      </option>
                    ))
                  ) : (
                    <option value="">{isLoadingDatasets ? 'Loading destinations...' : '(No target systems found)'}</option>
                  )}
                </select>
              </div>
            </div>
            <span className="text-xs text-slate-500 dark:text-slate-400 -mt-2 block font-sans">
              {targetTag ? (
                <>
                  Routing <strong className="text-indigo-600 dark:text-indigo-300">{sourceTag}</strong> → <strong className="text-cyan-600 dark:text-cyan-300">{targetTag}</strong> ({tablesForTarget.length} active dataset{tablesForTarget.length === 1 ? '' : 's'} registered).
                </>
              ) : (
                'Execution authority mode and schema contracts will be verified prior to landing.'
              )}
            </span>

            {/* Dataset Selection Mode */}
            <div>
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300 mb-2 flex items-center justify-between font-sans">
                <span className="flex items-center gap-1.5">
                  <Table className="w-3.5 h-3.5 text-cyan-600 dark:text-cyan-400" />
                  Dataset Execution Target
                </span>
                {tablesForTarget.length > 0 && (
                  <span className="text-xs text-slate-500 dark:text-slate-400 font-normal font-sans">
                    {tablesForTarget.length} datasets available {targetTag ? `for ${targetTag}` : ''}
                  </span>
                )}
              </label>

              <div className="grid grid-cols-2 gap-3 mb-3">
                <button
                  type="button"
                  onClick={() => setDatasetMode('ALL')}
                  className={`p-3.5 rounded-xl border text-left transition flex flex-col justify-between ${
                    datasetMode === 'ALL'
                      ? 'bg-cyan-50 dark:bg-cyan-500/10 border-cyan-400 dark:border-cyan-500/50 shadow-sm ring-1 ring-cyan-400/30 dark:ring-cyan-500/20'
                      : 'bg-white dark:bg-[#151f33]/60 border-slate-200 dark:border-slate-800/90 hover:bg-slate-50 dark:hover:bg-[#19253d] hover:border-slate-300 dark:hover:border-slate-700 shadow-sm'
                  }`}
                >
                  <div className="flex items-center justify-between w-full">
                    <span className={`text-sm font-bold font-sans ${
                      datasetMode === 'ALL' ? 'text-cyan-950 dark:text-white' : 'text-slate-800 dark:text-white'
                    }`}>
                      All Datasets
                    </span>
                    <span
                      className={`text-[11px] px-2 py-0.5 rounded font-sans font-semibold uppercase ${
                        datasetMode === 'ALL'
                          ? 'bg-cyan-100 dark:bg-cyan-500/20 text-cyan-800 dark:text-cyan-300 border border-cyan-300 dark:border-cyan-500/30'
                          : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 border border-slate-200 dark:border-slate-700'
                      }`}
                    >
                      Full Sync
                    </span>
                  </div>
                  <span className={`text-xs mt-1.5 leading-relaxed font-sans ${
                    datasetMode === 'ALL' ? 'text-cyan-900/80 dark:text-slate-400' : 'text-slate-500 dark:text-slate-400'
                  }`}>
                    Executes all active tables registered for {targetTag ? targetTag : 'this target'}.
                  </span>
                </button>

                <button
                  type="button"
                  onClick={() => setDatasetMode('SELECTED')}
                  className={`p-3.5 rounded-xl border text-left transition flex flex-col justify-between ${
                    datasetMode === 'SELECTED'
                      ? 'bg-indigo-50 dark:bg-indigo-600/15 border-indigo-400 dark:border-indigo-500/60 shadow-sm ring-1 ring-indigo-400/30 dark:ring-indigo-500/20'
                      : 'bg-white dark:bg-[#151f33]/60 border-slate-200 dark:border-slate-800/90 hover:bg-slate-50 dark:hover:bg-[#19253d] hover:border-slate-300 dark:hover:border-slate-700 shadow-sm'
                  }`}
                >
                  <div className="flex items-center justify-between w-full">
                    <span className={`text-sm font-bold font-sans ${
                      datasetMode === 'SELECTED' ? 'text-indigo-950 dark:text-white' : 'text-slate-800 dark:text-white'
                    }`}>
                      Selected Datasets
                    </span>
                    <span
                      className={`text-[11px] px-2 py-0.5 rounded font-sans font-semibold uppercase ${
                        datasetMode === 'SELECTED'
                          ? 'bg-indigo-100 dark:bg-indigo-500/20 text-indigo-800 dark:text-indigo-300 border border-indigo-300 dark:border-indigo-500/30'
                          : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 border border-slate-200 dark:border-slate-700'
                      }`}
                    >
                      Multi-Select
                    </span>
                  </div>
                  <span className={`text-xs mt-1.5 leading-relaxed font-sans ${
                    datasetMode === 'SELECTED' ? 'text-indigo-900/80 dark:text-slate-400' : 'text-slate-500 dark:text-slate-400'
                  }`}>
                    Target one or multiple datasets for {targetTag ? targetTag : 'this target'} with granular checkboxes.
                  </span>
                </button>
              </div>

              {/* When Selected Dataset is chosen, render modern Interactive Dataset Selector */}
              {datasetMode === 'SELECTED' && (
                <div className="p-4 rounded-xl bg-slate-50/80 dark:bg-[#0c1322]/90 border border-indigo-200 dark:border-indigo-500/30 space-y-3.5 animate-fadeIn">
                  {/* Header & Quick Action Bar */}
                  <div className="flex items-center justify-between pb-1">
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-bold text-slate-800 dark:text-slate-200 flex items-center gap-1.5 font-sans">
                        <Table className="w-3.5 h-3.5 text-indigo-600 dark:text-indigo-400" />
                        Target Datasets
                      </span>
                      <span className="text-xs font-semibold px-2.5 py-0.5 rounded-full bg-indigo-100 dark:bg-indigo-500/15 text-indigo-800 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-500/25 font-sans">
                        {selectedTableNames.length} of {tablesForTarget.length} selected
                      </span>
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => refetchDatasets()}
                        disabled={isLoadingDatasets}
                        title="Reload datasets from metadata catalog"
                        className="text-xs font-medium text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white bg-white dark:bg-slate-800/70 hover:bg-slate-100 dark:hover:bg-slate-800 border border-slate-200 dark:border-slate-700/70 p-1.5 rounded-lg transition disabled:opacity-40 shadow-sm"
                      >
                        <RefreshCw className={`w-3.5 h-3.5 ${isLoadingDatasets ? 'animate-spin text-indigo-600 dark:text-indigo-400' : ''}`} />
                      </button>
                      <button
                        type="button"
                        onClick={selectAllVisible}
                        disabled={isLoadingDatasets || tablesForTarget.length === 0}
                        className="text-xs font-semibold text-indigo-700 dark:text-indigo-200 hover:text-indigo-900 dark:hover:text-white bg-indigo-100/70 dark:bg-indigo-500/15 hover:bg-indigo-100 dark:hover:bg-indigo-500/25 border border-indigo-200 dark:border-indigo-500/30 px-2.5 py-1 rounded-lg transition flex items-center gap-1.5 disabled:opacity-50 font-sans shadow-sm"
                      >
                        <CheckSquare className="w-3.5 h-3.5" /> Select All
                      </button>
                      <button
                        type="button"
                        onClick={clearSelection}
                        disabled={selectedTableNames.length === 0}
                        className="text-xs font-medium text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white bg-white dark:bg-slate-800/60 hover:bg-slate-100 dark:hover:bg-slate-800 border border-slate-200 dark:border-slate-700/60 px-2.5 py-1 rounded-lg transition flex items-center gap-1.5 disabled:opacity-40 font-sans shadow-sm"
                      >
                        <Square className="w-3.5 h-3.5" /> Clear
                      </button>
                    </div>
                  </div>

                  {/* Real-time Search / Filter Input */}
                  <div className="relative">
                    <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      placeholder="Filter datasets by name or target (e.g. task, bank, user)..."
                      className="w-full bg-white dark:bg-[#131d31] border border-slate-300 dark:border-slate-700/80 rounded-xl pl-9 pr-8 py-2 text-xs text-slate-900 dark:text-slate-100 placeholder-slate-400 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/20 font-sans transition shadow-sm"
                    />
                    {searchQuery && (
                      <button
                        type="button"
                        onClick={() => setSearchQuery('')}
                        className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600 dark:hover:text-white p-0.5 rounded"
                      >
                        <X className="w-3.5 h-3.5" />
                      </button>
                    )}
                  </div>

                  {/* Scrollable Checkbox Dataset List */}
                  <div className="bg-white dark:bg-[#090e1a] border border-slate-200 dark:border-slate-800/90 rounded-xl p-2 max-h-56 overflow-y-auto space-y-1.5 custom-scrollbar shadow-inner">
                    {isLoadingDatasets ? (
                      <div className="p-6 flex flex-col items-center justify-center text-xs text-slate-500 dark:text-slate-400 gap-2 font-sans">
                        <Loader2 className="w-4 h-4 animate-spin text-indigo-600 dark:text-indigo-400" />
                        <span>Loading registered datasets for '{sourceTag}'...</span>
                      </div>
                    ) : isErrorDatasets ? (
                      <div className="p-5 text-center text-xs text-rose-800 dark:text-rose-300 flex flex-col items-center gap-2 bg-rose-50 dark:bg-rose-500/5 rounded-lg border border-rose-200 dark:border-rose-500/20 font-sans">
                        <AlertCircle className="w-4 h-4 text-rose-600 dark:text-rose-400" />
                        <span>Failed to retrieve datasets: {(datasetError as any)?.message || 'Catalog query error'}</span>
                        <button
                          type="button"
                          onClick={() => refetchDatasets()}
                          className="px-2.5 py-1 bg-rose-100 dark:bg-rose-500/15 hover:bg-rose-200 dark:hover:bg-rose-500/25 border border-rose-200 dark:border-rose-500/30 text-rose-800 dark:text-rose-200 rounded text-xs transition font-sans"
                        >
                          Retry Query
                        </button>
                      </div>
                    ) : filteredTables.length === 0 ? (
                      <div className="p-5 text-center text-xs text-slate-500 dark:text-slate-400 font-sans">
                        {searchQuery ? (
                          <>No datasets match <code className="text-indigo-600 dark:text-indigo-300 font-mono font-semibold">"{searchQuery}"</code></>
                        ) : (
                          `No active registered datasets found for project '${sourceTag}' targeting '${targetTag || 'destination'}'.`
                        )}
                      </div>
                    ) : (
                      filteredTables.map((tbl) => {
                        const name = tbl.source_table || tbl.table_name || '';
                        const target = tbl.target_table || tbl.target_table_name || '';
                        const isSelected = selectedTableNames.includes(name);

                        return (
                          <div
                            key={name || tbl.table_id}
                            onClick={() => toggleTable(name)}
                            className={`p-3 rounded-xl border flex items-center justify-between cursor-pointer transition select-none ${
                              isSelected
                                ? 'bg-indigo-50 dark:bg-indigo-950/35 border-indigo-300 dark:border-indigo-500/60 shadow-sm'
                                : 'bg-white dark:bg-[#121929]/70 border-slate-200 dark:border-slate-800/80 hover:bg-slate-50 dark:hover:bg-[#162136] hover:border-slate-300 dark:hover:border-slate-700'
                            }`}
                          >
                            <div className="flex items-center gap-3.5 min-w-0">
                              {/* Custom Checkbox */}
                              <div
                                className={`w-4.5 h-4.5 rounded-md border flex items-center justify-center flex-shrink-0 transition ${
                                  isSelected
                                    ? 'bg-indigo-600 border-indigo-600 text-white'
                                    : 'border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900/70'
                                }`}
                              >
                                {isSelected && <Check className="w-3 h-3 stroke-[3]" />}
                              </div>

                              <div className="min-w-0">
                                <span className={`text-sm font-semibold block truncate font-sans tracking-tight ${
                                  isSelected ? 'text-indigo-950 dark:text-indigo-200' : 'text-slate-900 dark:text-slate-100'
                                }`}>
                                  {name}
                                </span>
                                <span className="text-xs text-slate-500 dark:text-slate-400 block truncate font-sans mt-0.5">
                                  → {tbl.target_tag ? `${tbl.target_tag}.` : ''}{target || name}
                                </span>
                              </div>
                            </div>

                            <div className="flex items-center gap-2 flex-shrink-0 font-sans">
                              {tbl.is_cdc && (
                                <span className="text-[10px] font-semibold px-2 py-0.5 rounded-md bg-amber-100 dark:bg-amber-500/15 text-amber-800 dark:text-amber-300 border border-amber-200 dark:border-amber-500/30 uppercase">
                                  CDC
                                </span>
                              )}
                              {tbl.target_tag && (
                                <span className="text-[10px] font-semibold px-2 py-0.5 rounded-md bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700/80">
                                  {tbl.target_tag}
                                </span>
                              )}
                              <span className="text-[10px] font-medium px-2 py-0.5 rounded-md bg-slate-100 dark:bg-slate-800/60 text-slate-600 dark:text-slate-400 border border-slate-200/60 dark:border-transparent">
                                P{tbl.priority || 1}
                              </span>
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>

                  {/* Selected Dataset Summary Chips */}
                  {selectedTableNames.length > 0 && (
                    <div className="space-y-1.5 pt-1">
                      <div className="flex items-center justify-between text-xs text-slate-600 dark:text-slate-400 font-sans font-medium">
                        <span>Selected for Execution ({selectedTableNames.length}):</span>
                      </div>

                      <div className="flex flex-wrap gap-1.5 max-h-20 overflow-y-auto custom-scrollbar">
                        {selectedTableNames.slice(0, 8).map((tblName) => (
                          <span
                            key={tblName}
                            className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-lg bg-indigo-100 dark:bg-indigo-500/15 text-indigo-800 dark:text-indigo-200 border border-indigo-200 dark:border-indigo-500/30 text-xs font-sans font-medium animate-fadeIn"
                          >
                            <span>{tblName}</span>
                            <button
                              type="button"
                              onClick={(e) => {
                                e.stopPropagation();
                                toggleTable(tblName);
                              }}
                              className="text-indigo-500 dark:text-indigo-400 hover:text-indigo-900 dark:hover:text-white rounded ml-0.5 text-sm"
                            >
                              ×
                            </button>
                          </span>
                        ))}
                        {selectedTableNames.length > 8 && (
                          <span className="text-xs text-slate-500 dark:text-slate-400 self-center font-sans font-medium">
                            +{selectedTableNames.length - 8} more
                          </span>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Target Tag Conflict Alert */}
                  {hasConflictingTargetTags && (
                    <div className="p-3 rounded-lg bg-amber-50 dark:bg-amber-500/10 border border-amber-200 dark:border-amber-500/25 flex items-start gap-2.5 text-xs text-amber-800 dark:text-amber-300 animate-fadeIn font-sans">
                      <AlertCircle className="w-4 h-4 flex-shrink-0 mt-0.5 text-amber-600 dark:text-amber-400" />
                      <div>
                        <p className="font-semibold text-amber-900 dark:text-amber-200">Mixed Target Tags Detected</p>
                        <p className="text-xs text-amber-800/90 dark:text-amber-300/90 mt-0.5 leading-relaxed">
                          Selected datasets target multiple destinations: <code className="font-mono bg-amber-100 dark:bg-amber-950/50 px-1 py-0.5 rounded text-amber-900 dark:text-amber-200">{selectedTargetTags.join(', ')}</code>.
                          All tables in a governed run must share one target destination. Please adjust your selection.
                        </p>
                      </div>
                    </div>
                  )}

                  {/* Zero Selection Notice */}
                  {selectedTableNames.length === 0 && !isLoadingDatasets && !isErrorDatasets && (
                    <div className="p-2.5 rounded-lg bg-slate-100 dark:bg-slate-800/40 border border-slate-200 dark:border-slate-700/50 text-xs text-slate-600 dark:text-slate-400 flex items-center gap-2 font-sans">
                      <AlertCircle className="w-4 h-4 text-slate-500 dark:text-slate-400 flex-shrink-0" />
                      <span>Please check at least one dataset to enable run submission.</span>
                    </div>
                  )}

                  {/* Scope Lock Notice */}
                  {selectedTableNames.length > 0 && !hasConflictingTargetTags && (
                    <p className="text-xs text-slate-600 dark:text-slate-400 flex items-center gap-1.5 pt-0.5 font-sans">
                      <ShieldCheck className="w-4 h-4 text-emerald-600 dark:text-emerald-400 flex-shrink-0" />
                      <span>
                        Exclusive scope claims and execution locks will be acquired for the{' '}
                        <strong className="text-indigo-700 dark:text-indigo-300 font-semibold">{selectedTableNames.length}</strong> selected dataset(s).
                      </span>
                    </p>
                  )}
                </div>
              )}
          </div>

          {/* Execution Scope Selection */}
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300 mb-2 flex items-center gap-1.5 font-sans">
              <Layers className="w-3.5 h-3.5 text-indigo-600 dark:text-indigo-400" />
              Execution Scope & Medallion Layer
            </label>
            <div className="grid grid-cols-1 gap-2.5">
              {scopeCards.map((sc) => (
                <div
                  key={sc.key}
                  onClick={() => setExecutionScope(sc.key)}
                  className={`p-3.5 rounded-xl border cursor-pointer transition flex items-start justify-between ${
                    executionScope === sc.key
                      ? 'bg-indigo-50 dark:bg-indigo-600/15 border-indigo-400 dark:border-indigo-500/60 shadow-sm'
                      : 'bg-white dark:bg-[#151f33]/60 border-slate-200 dark:border-slate-800/90 hover:bg-slate-50 dark:hover:bg-[#19253d] hover:border-slate-300 dark:hover:border-slate-700 shadow-sm'
                  }`}
                >
                  <div className="flex items-start gap-3">
                    <div
                      className={`w-4 h-4 rounded-full border mt-0.5 flex items-center justify-center ${
                        executionScope === sc.key
                          ? 'border-indigo-600 bg-indigo-600 dark:border-indigo-400 dark:bg-indigo-500'
                          : 'border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-800'
                      }`}
                    >
                      {executionScope === sc.key && <div className="w-1.5 h-1.5 rounded-full bg-white" />}
                    </div>
                    <div>
                      <span className={`text-sm font-semibold block font-sans ${
                        executionScope === sc.key ? 'text-indigo-950 dark:text-white' : 'text-slate-900 dark:text-white'
                      }`}>
                        {sc.title}
                      </span>
                      <span className={`text-xs mt-0.5 block leading-relaxed font-sans ${
                        executionScope === sc.key ? 'text-indigo-900/80 dark:text-slate-400' : 'text-slate-500 dark:text-slate-400'
                      }`}>
                        {sc.subtitle}
                      </span>
                    </div>
                  </div>
                  <span className={`text-xs font-semibold px-2.5 py-0.5 rounded-md font-sans ${
                    executionScope === sc.key
                      ? 'bg-indigo-100 dark:bg-indigo-500/20 text-indigo-800 dark:text-indigo-300 border border-indigo-300 dark:border-indigo-500/30'
                      : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 border border-slate-200 dark:border-slate-700/80'
                  }`}>
                    {sc.badge}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Priority Selection */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300 mb-2 font-sans">
                Execution Priority
              </label>
              <select
                value={priority}
                onChange={(e) => setPriority(Number(e.target.value))}
                className="w-full bg-white dark:bg-[#151f33] border border-slate-300 dark:border-slate-700/80 rounded-xl px-3 py-2 text-xs text-slate-800 dark:text-slate-100 outline-none focus:ring-2 focus:ring-indigo-500/20 focus:border-indigo-500 cursor-pointer font-sans shadow-sm"
              >
                <option value={1}>1 - Highest (Urgent Sync)</option>
                <option value={5}>5 - Normal (Standard Pipeline)</option>
                <option value={10}>10 - Low (Background Batch)</option>
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300 mb-2 font-sans">
                Concurrency Guard
              </label>
              <div className="h-9 px-3 bg-emerald-50 dark:bg-[#151f33]/60 border border-emerald-200 dark:border-slate-800 rounded-xl flex items-center gap-2 text-xs text-emerald-800 dark:text-emerald-400 font-sans">
                <ShieldCheck className="w-4 h-4 text-emerald-600 dark:text-emerald-400" />
                <span className="font-medium">Scope-Claim Fencing Active</span>
              </div>
            </div>
          </div>

          {/* Safety Confirmation */}
          <div className="pt-3 border-t border-slate-200 dark:border-slate-800">
            <label className="flex items-start gap-3 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={confirmed}
                onChange={(e) => setConfirmed(e.target.checked)}
                className="mt-0.5 rounded border-slate-300 dark:border-slate-700 text-indigo-600 focus:ring-indigo-500 bg-white dark:bg-[#1e293b] w-4 h-4 cursor-pointer"
              />
              <span className="text-xs text-slate-600 dark:text-slate-300 leading-relaxed font-sans">
                I understand this will trigger a governed execution run in the engine for{' '}
                <strong className="text-slate-900 dark:text-white font-bold">{sourceTag}</strong>
                {targetTag && (
                  <> → <strong className="text-cyan-700 dark:text-cyan-300 font-bold">{targetTag}</strong></>
                )}
                {datasetMode === 'SELECTED' ? (
                  <>
                    {' '}(
                    <strong className="text-indigo-700 dark:text-indigo-300 font-semibold">
                      {selectedTableNames.length} selected dataset{selectedTableNames.length === 1 ? '' : 's'}
                    </strong>
                    , {executionScope} scope)
                  </>
                ) : (
                  <> (all active datasets for {targetTag || 'destination'}, {executionScope} scope)</>
                )}.
              </span>
            </label>
          </div>
        </div>

        {/* Docked Modal Footer */}
        <div className="p-4 px-6 border-t border-slate-200 dark:border-slate-800/80 bg-slate-50/90 dark:bg-[#0c1322]/95 flex items-center justify-end gap-3 shrink-0">
          <button
            type="button"
            onClick={onClose}
            disabled={triggerMutation.isPending}
            className="px-4 py-2 rounded-xl text-xs font-semibold text-slate-600 hover:text-slate-900 hover:bg-slate-200/60 dark:text-slate-300 dark:hover:text-white dark:hover:bg-slate-800 transition font-sans"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={!isFormValid}
            className="flex items-center gap-2 px-5 py-2.5 bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white text-xs font-semibold rounded-xl shadow-lg shadow-indigo-600/25 transition duration-150 disabled:opacity-40 disabled:cursor-not-allowed font-sans"
          >
            {triggerMutation.isPending ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                <span>Submitting Run to Engine...</span>
              </>
            ) : (
              <>
                <CheckCircle2 className="w-4 h-4" />
                <span>
                  {datasetMode === 'SELECTED'
                    ? selectedTableNames.length === 0
                      ? 'Select Datasets to Dispatch'
                      : selectedTableNames.length === 1
                      ? `Dispatch Run (${selectedTableNames[0]})`
                      : `Dispatch Run (${selectedTableNames.length} Datasets)`
                    : 'Dispatch Governed Run'}
                </span>
              </>
            )}
          </button>
        </div>
      </form>
      </div>
    </div>
  );
};
