// frontend/src/components/runs/ExecutionPanel.tsx
import React, { useState, useEffect, useMemo } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../../context/AuthContext';
import { api } from '../../api/client';
import type { TriggerRunRequest, PipelineItem } from '../../api/types';
import {
  Play,
  Layers,
  Database,
  Search,
  CheckSquare,
  Square,
  AlertTriangle,
  Loader2,
  ShieldCheck,
  Zap,
  X,
} from 'lucide-react';

interface ExecutionPanelProps {
  initialTables?: string[];
  onRunTriggered: (runId: string) => void;
  onCancel?: () => void;
}

export const ExecutionPanel: React.FC<ExecutionPanelProps> = ({
  initialTables,
  onRunTriggered,
  onCancel,
}) => {
  const { user, projectScope, setProjectScope } = useAuth();
  const queryClient = useQueryClient();

  const allowedSources =
    user?.allowed_projects && user.allowed_projects.length > 0
      ? user.allowed_projects
      : ['school_system', 'source_erp_db', 'opf_oracle_housing'];

  // Step 1: Source & Target states
  const [sourceTag, setSourceTag] = useState<string>(
    projectScope && allowedSources.includes(projectScope)
      ? projectScope
      : allowedSources[0] || 'school_system'
  );
  const [targetTag, setTargetTag] = useState<string>('');

  // Step 2: Dataset selection states
  const [datasetMode, setDatasetMode] = useState<'ALL' | 'SELECTED'>('SELECTED');
  const [selectedTableNames, setSelectedTableNames] = useState<string[]>(initialTables || []);
  const [searchQuery, setSearchQuery] = useState<string>('');

  // Step 3: Execution Scope & Options
  const [executionScope, setExecutionScope] = useState<'FULL' | 'BRONZE' | 'SILVER' | 'GOLD'>('FULL');
  const [priority, setPriority] = useState<number>(2);
  const [confirmed, setConfirmed] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [rejectionData, setRejectionData] = useState<{
    rejectionCode?: string;
    existingActiveRunId?: string;
  } | null>(null);

  // Fetch datasets for the selected source system
  const {
    data: pipelineData,
    isLoading: isLoadingDatasets,
  } = useQuery({
    queryKey: ['pipelines', sourceTag],
    queryFn: () => api.getPipelines(sourceTag),
    enabled: !!sourceTag,
  });

  // Extract available tables
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

  // Auto-sync active target destination
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

  // Initialize selectedTableNames if empty
  useEffect(() => {
    if (tablesForTarget.length > 0 && selectedTableNames.length === 0) {
      if (initialTables && initialTables.length > 0) {
        setSelectedTableNames(initialTables);
      } else {
        const getTableName = (t: PipelineItem) => t.source_table || t.table_name || '';
        const firstActive = tablesForTarget.find((t) => t.is_active) || tablesForTarget[0];
        const defaultName = getTableName(firstActive);
        if (defaultName) setSelectedTableNames([defaultName]);
      }
    }
  }, [tablesForTarget]);

  // Reset when changing source
  const handleSourceChange = (newSource: string) => {
    setSourceTag(newSource);
    setTargetTag('');
    setSearchQuery('');
    setErrorMsg(null);
    setRejectionData(null);
  };

  // Filtered dataset list
  const filteredTables = useMemo(() => {
    if (!searchQuery.trim()) return tablesForTarget;
    const q = searchQuery.toLowerCase().trim();
    return tablesForTarget.filter((tbl) => {
      const source = (tbl.source_table || tbl.table_name || '').toLowerCase();
      const target = (tbl.target_table || tbl.target_table_name || '').toLowerCase();
      const tag = (tbl.target_tag || '').toLowerCase();
      const id = String(tbl.table_id || '');
      return source.includes(q) || target.includes(q) || tag.includes(q) || id.includes(q);
    });
  }, [tablesForTarget, searchQuery]);

  // Toggle selection for a single table
  const toggleTable = (tableName: string) => {
    setSelectedTableNames((prev) =>
      prev.includes(tableName)
        ? prev.filter((name) => name !== tableName)
        : [...prev, tableName]
    );
  };

  // Select all currently visible tables
  const selectAllVisible = () => {
    const visibleNames = filteredTables.map((t) => t.source_table || t.table_name || '').filter(Boolean);
    setSelectedTableNames((prev) => Array.from(new Set([...prev, ...visibleNames])));
  };

  // Clear all selections
  const clearSelection = () => {
    setSelectedTableNames([]);
  };

  // Resolve selected table objects
  const selectedTableObjects = useMemo(() => {
    const selectedSet = new Set(selectedTableNames);
    return tablesForTarget.filter((t) => selectedSet.has(t.source_table || t.table_name || ''));
  }, [tablesForTarget, selectedTableNames]);

  // Check conflicting target tags
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

  // Trigger Mutation
  const triggerMutation = useMutation({
    mutationFn: (payload: TriggerRunRequest) => api.triggerRun(payload, payload.source_tag),
    onSuccess: (data) => {
      if (sourceTag && projectScope !== sourceTag) {
        setProjectScope(sourceTag);
      }
      queryClient.invalidateQueries({ queryKey: ['runs'] });
      queryClient.invalidateQueries({ queryKey: ['clusterRuns'] });
      queryClient.invalidateQueries({ queryKey: ['kpis'] });
      queryClient.invalidateQueries({ queryKey: ['activity'] });

      const newRunId = data.run_id || data.request_id || 'RUN-ACCEPTED';
      setErrorMsg(null);
      setRejectionData(null);
      // Immediately direct to Canvas!
      onRunTriggered(newRunId);
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

    const effectiveTargetTag = targetTag || (selectedTargetTags[0] ?? null);

    const payload: TriggerRunRequest = {
      source_tag: sourceTag,
      target_tag: effectiveTargetTag,
      execution_scope: executionScope,
      priority,
      source_table_ids: datasetMode === 'SELECTED' && selectedIds.length > 0 ? selectedIds : null,
      source_table:
        datasetMode === 'SELECTED' && selectedTableNames.length === 1
          ? selectedTableNames[0]
          : null,
    };

    triggerMutation.mutate(payload);
  };

  const isAllVisibleSelected =
    filteredTables.length > 0 &&
    filteredTables.every((t) => selectedTableNames.includes(t.source_table || t.table_name || ''));

  const canSubmit =
    confirmed &&
    !triggerMutation.isPending &&
    (datasetMode === 'ALL' || (selectedTableNames.length > 0 && !hasConflictingTargetTags));

  return (
    <div className="p-6 md:p-8 rounded-3xl bg-[#0d1424]/95 border-2 border-indigo-500/40 shadow-2xl shadow-indigo-950/50 backdrop-blur-xl space-y-7 animate-fadeIn font-sans text-slate-100 relative">
      {/* Panel Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-5 border-b border-slate-800/80">
        <div>
          <div className="flex items-center gap-2 text-xs text-indigo-400 font-semibold uppercase tracking-wider mb-1">
            <Zap className="w-4 h-4 text-amber-400" />
            <span>Daily Operations · Interactive Pipeline Trigger</span>
          </div>
          <h2 className="text-xl font-bold tracking-tight text-white flex items-center gap-2.5">
            Trigger Pipeline Execution
          </h2>
          <p className="text-xs text-slate-400 mt-0.5">
            Select source, target, datasets, and execution scope. When dispatched, live progress connects directly to the Workflow Canvas below.
          </p>
        </div>

        <div className="flex items-center gap-2.5">
          <div className="flex items-center gap-1.5 px-3 py-1 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-semibold">
            <ShieldCheck className="w-3.5 h-3.5" />
            Fencing Active
          </div>
          {onCancel && (
            <button
              onClick={onCancel}
              className="p-1.5 text-slate-400 hover:text-white hover:bg-slate-800 rounded-xl transition"
              title="Close Trigger Panel"
            >
              <X className="w-5 h-5" />
            </button>
          )}
        </div>
      </div>

      {/* Error / Rejection Banner */}
      {errorMsg && (
        <div className="p-4 rounded-2xl bg-rose-500/10 border border-rose-500/30 flex items-start gap-3.5 text-rose-300 text-sm animate-fadeIn">
          <AlertTriangle className="w-5 h-5 flex-shrink-0 mt-0.5 text-rose-400" />
          <div className="space-y-1">
            <p className="font-bold text-rose-200">Execution Request Rejected by Engine</p>
            <p className="text-xs text-rose-300 leading-relaxed">{errorMsg}</p>
            {rejectionData?.existingActiveRunId && (
              <p className="text-xs text-amber-300 font-mono pt-1">
                Conflicting active run holding scope lock: {rejectionData.existingActiveRunId}
              </p>
            )}
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-6">
        {/* STEP 1: SELECT SOURCE & TARGET DESTINATION */}
        <div className="p-5 rounded-2xl bg-[#11192e]/80 border border-slate-800 space-y-4">
          <div className="flex items-center justify-between pb-3 border-b border-slate-800/80">
            <div className="flex items-center gap-2.5">
              <div className="w-6 h-6 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-xs font-bold text-indigo-400">
                1
              </div>
              <h3 className="text-sm font-bold text-white">
                Step 1 — Select Source & Target Destination
              </h3>
            </div>
            <div className="flex items-center gap-1.5 text-[11px] text-slate-400">
              <Database className="w-3 h-3 text-indigo-400" />
              <span>metadata.connections</span>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
            {/* Source Selection */}
            <div className="space-y-1.5">
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-300">
                Source System
              </label>
              <select
                value={sourceTag}
                onChange={(e) => handleSourceChange(e.target.value)}
                className="w-full bg-[#152037] border border-slate-700 hover:border-slate-600 rounded-xl px-3.5 py-2.5 text-xs text-white font-medium outline-none focus:ring-2 focus:ring-indigo-500/30 transition cursor-pointer"
              >
                {allowedSources.map((tag) => (
                  <option key={tag} value={tag}>
                    {tag}
                  </option>
                ))}
              </select>
            </div>

            {/* Target Selection */}
            <div className="space-y-1.5">
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-300">
                Target Destination
              </label>
              <select
                value={targetTag}
                onChange={(e) => {
                  setTargetTag(e.target.value);
                  setErrorMsg(null);
                }}
                disabled={availableTargetTags.length === 0}
                className="w-full bg-[#152037] border border-slate-700 hover:border-slate-600 rounded-xl px-3.5 py-2.5 text-xs text-white font-medium outline-none focus:ring-2 focus:ring-cyan-500/30 transition cursor-pointer disabled:opacity-50"
              >
                {availableTargetTags.length > 0 ? (
                  availableTargetTags.map((tag) => (
                    <option key={tag} value={tag}>
                      {tag}
                    </option>
                  ))
                ) : (
                  <option value="">
                    {isLoadingDatasets ? 'Loading destinations...' : '(No target systems routed)'}
                  </option>
                )}
              </select>
            </div>
          </div>
        </div>

        {/* STEP 2: SELECT DATASET (FULL-WIDTH EXPANSIVE TABLE) */}
        <div className="p-5 rounded-2xl bg-[#11192e]/80 border border-slate-800 space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-slate-800/80">
            <div className="flex items-center gap-2.5">
              <div className="w-6 h-6 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-xs font-bold text-indigo-400">
                2
              </div>
              <h3 className="text-sm font-bold text-white">
                Step 2 — Select Dataset
              </h3>
            </div>

            {/* Scope Toggle: Full vs Selected */}
            <div className="flex items-center gap-2 bg-[#152037] p-1 rounded-xl border border-slate-700">
              <button
                type="button"
                onClick={() => setDatasetMode('ALL')}
                className={`px-3 py-1 rounded-lg text-xs font-semibold transition ${
                  datasetMode === 'ALL'
                    ? 'bg-indigo-600 text-white shadow-sm'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                Full Dataset ({tablesForTarget.length})
              </button>
              <button
                type="button"
                onClick={() => setDatasetMode('SELECTED')}
                className={`px-3 py-1 rounded-lg text-xs font-semibold transition ${
                  datasetMode === 'SELECTED'
                    ? 'bg-indigo-600 text-white shadow-sm'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                Selected Dataset ({selectedTableNames.length})
              </button>
            </div>
          </div>

          {datasetMode === 'SELECTED' ? (
            <div className="space-y-3">
              {/* Search & Actions Bar */}
              <div className="flex flex-col sm:flex-row items-center justify-between gap-3">
                <div className="relative w-full sm:w-72">
                  <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder="Search datasets, table name..."
                    className="w-full bg-[#152037] border border-slate-700 hover:border-slate-600 rounded-xl pl-9 pr-4 py-1.5 text-xs text-white placeholder-slate-500 outline-none focus:ring-2 focus:ring-indigo-500/30 transition"
                  />
                </div>

                <div className="flex items-center gap-2 w-full sm:w-auto justify-end">
                  <button
                    type="button"
                    onClick={selectAllVisible}
                    className="px-2.5 py-1 rounded-lg text-xs font-medium text-slate-300 hover:text-white bg-slate-800/80 border border-slate-700 transition"
                  >
                    Select All Visible
                  </button>
                  <button
                    type="button"
                    onClick={clearSelection}
                    className="px-2.5 py-1 rounded-lg text-xs font-medium text-slate-400 hover:text-rose-400 bg-slate-800/40 border border-slate-700/40 transition"
                  >
                    Clear All
                  </button>
                  <div className="px-2.5 py-1 rounded-lg bg-indigo-500/10 border border-indigo-500/20 text-indigo-300 text-xs font-semibold">
                    {selectedTableNames.length} Selected
                  </div>
                </div>
              </div>

              {/* Full-width Responsive Dataset Table */}
              <div className="border border-slate-800 rounded-xl overflow-hidden bg-[#0c1220]/80">
                <div className="overflow-x-auto max-h-[300px] custom-scrollbar">
                  <table className="w-full text-left text-xs text-slate-300 border-collapse">
                    <thead className="bg-[#131c31] text-[11px] font-semibold text-slate-400 uppercase tracking-wider sticky top-0 z-10 border-b border-slate-800">
                      <tr>
                        <th className="py-2.5 px-3 w-10 text-center">
                          <button
                            type="button"
                            onClick={() => (isAllVisibleSelected ? clearSelection() : selectAllVisible())}
                            className="p-0.5 hover:text-white transition"
                          >
                            {isAllVisibleSelected ? (
                              <CheckSquare className="w-3.5 h-3.5 text-indigo-400" />
                            ) : (
                              <Square className="w-3.5 h-3.5 text-slate-500" />
                            )}
                          </button>
                        </th>
                        <th className="py-2.5 px-3">table_id</th>
                        <th className="py-2.5 px-3">source_table</th>
                        <th className="py-2.5 px-3">target_tag</th>
                        <th className="py-2.5 px-3">target_table</th>
                        <th className="py-2.5 px-2 text-center">priority</th>
                        <th className="py-2.5 px-3">landing_mode</th>
                        <th className="py-2.5 px-2 text-center">is_cdc</th>
                        <th className="py-2.5 px-3">last_success_timestamp</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60 font-sans">
                      {isLoadingDatasets ? (
                        <tr>
                          <td colSpan={9} className="py-8 text-center text-slate-500">
                            <div className="flex items-center justify-center gap-2">
                              <Loader2 className="w-4 h-4 animate-spin text-indigo-400" />
                              <span>Loading registered datasets for {sourceTag}...</span>
                            </div>
                          </td>
                        </tr>
                      ) : filteredTables.length === 0 ? (
                        <tr>
                          <td colSpan={9} className="py-8 text-center text-slate-500">
                            {searchQuery
                              ? `No datasets matched query "${searchQuery}"`
                              : 'No registered datasets found for this source & target.'}
                          </td>
                        </tr>
                      ) : (
                        filteredTables.map((tbl) => {
                          const name = tbl.source_table || tbl.table_name || '';
                          const isSelected = selectedTableNames.includes(name);
                          const targetTbl = tbl.target_table || tbl.target_table_name || name;
                          return (
                            <tr
                              key={name}
                              onClick={() => toggleTable(name)}
                              className={`transition cursor-pointer ${
                                isSelected
                                  ? 'bg-indigo-600/15 hover:bg-indigo-600/20 text-white font-medium'
                                  : 'hover:bg-slate-800/40 text-slate-300'
                              }`}
                            >
                              <td className="py-2 px-3 text-center" onClick={(e) => e.stopPropagation()}>
                                <input
                                  type="checkbox"
                                  checked={isSelected}
                                  onChange={() => toggleTable(name)}
                                  className="w-3.5 h-3.5 rounded text-indigo-600 bg-slate-900 border-slate-700 focus:ring-indigo-500 cursor-pointer"
                                />
                              </td>
                              <td className="py-2 px-3 font-mono text-slate-400 text-[11px]">
                                {tbl.table_id ?? '—'}
                              </td>
                              <td className="py-2 px-3 font-semibold text-slate-100 flex items-center gap-1.5">
                                <Database className="w-3 h-3 text-slate-500" />
                                <span>{name}</span>
                              </td>
                              <td className="py-2 px-3 font-mono text-[11px]">
                                <span className="px-1.5 py-0.5 rounded bg-cyan-500/10 text-cyan-300 border border-cyan-500/20">
                                  {tbl.target_tag || targetTag || '—'}
                                </span>
                              </td>
                              <td className="py-2 px-3 font-mono text-slate-300 text-[11px]">
                                {targetTbl}
                              </td>
                              <td className="py-2 px-2 text-center">
                                <span
                                  className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-bold ${
                                    tbl.priority === 1
                                      ? 'bg-rose-500/15 text-rose-300 border border-rose-500/30'
                                      : tbl.priority === 2
                                      ? 'bg-amber-500/15 text-amber-300 border border-amber-500/30'
                                      : 'bg-slate-800 text-slate-300'
                                  }`}
                                >
                                  {tbl.priority ?? 5}
                                </span>
                              </td>
                              <td className="py-2 px-3 font-mono text-[11px]">
                                <span
                                  className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                                    String(tbl.landing_mode).toUpperCase() === 'DIRECT'
                                      ? 'bg-purple-500/15 text-purple-300 border border-purple-500/30'
                                      : 'bg-slate-800 text-slate-400'
                                  }`}
                                >
                                  {tbl.landing_mode || 'METADATA'}
                                </span>
                              </td>
                              <td className="py-2 px-2 text-center font-mono">
                                <span
                                  className={`px-1.5 py-0.5 rounded text-[10px] ${
                                    tbl.is_cdc ? 'bg-emerald-500/20 text-emerald-300 font-bold' : 'text-slate-500'
                                  }`}
                                >
                                  {tbl.is_cdc ? 'y' : 'n'}
                                </span>
                              </td>
                              <td className="py-2 px-3 text-slate-400 text-[11px] font-mono">
                                {tbl.last_success_timestamp
                                  ? tbl.last_success_timestamp.replace('T', ' ').substring(0, 19)
                                  : 'None'}
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
          ) : (
            <div className="p-5 rounded-xl bg-[#131c31]/60 border border-slate-800 flex items-center justify-center gap-3 text-center">
              <Layers className="w-5 h-5 text-indigo-400" />
              <p className="text-xs text-slate-300">
                Execution will encompass all <strong>{tablesForTarget.length} datasets</strong> for{' '}
                <span className="text-indigo-300 font-mono">{sourceTag}</span> →{' '}
                <span className="text-cyan-300 font-mono">{targetTag}</span> in priority order.
              </p>
            </div>
          )}
        </div>

        {/* STEP 3: EXECUTION SCOPE & MEDALLION LAYER */}
        <div className="p-5 rounded-2xl bg-[#11192e]/80 border border-slate-800 space-y-4">
          <div className="flex items-center gap-2.5 pb-3 border-b border-slate-800/80">
            <div className="w-6 h-6 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-xs font-bold text-indigo-400">
              3
            </div>
            <h3 className="text-sm font-bold text-white">
              Step 3 — Execution Scope & Medallion Layer
            </h3>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
            {/* Full Medallion */}
            <div
              onClick={() => setExecutionScope('FULL')}
              className={`p-3.5 rounded-xl border transition cursor-pointer flex flex-col justify-between ${
                executionScope === 'FULL'
                  ? 'bg-indigo-600/15 border-indigo-500/60 shadow-md shadow-indigo-900/20'
                  : 'bg-[#152037]/60 border-slate-800 hover:border-slate-700 text-slate-400'
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-white">Full Pipeline</span>
                <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">
                  RECOMMENDED
                </span>
              </div>
              <p className="text-[11px] text-slate-400 mt-1.5 leading-relaxed">
                Bronze extract → Silver deduplicate → Gold UPSERT.
              </p>
            </div>

            {/* Bronze Extract Only */}
            <div
              onClick={() => setExecutionScope('BRONZE')}
              className={`p-3.5 rounded-xl border transition cursor-pointer flex flex-col justify-between ${
                executionScope === 'BRONZE'
                  ? 'bg-amber-600/15 border-amber-500/60 shadow-md shadow-amber-900/20'
                  : 'bg-[#152037]/60 border-slate-800 hover:border-slate-700 text-slate-400'
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-white">Bronze Only</span>
                <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-300 border border-amber-500/30">
                  INGEST
                </span>
              </div>
              <p className="text-[11px] text-slate-400 mt-1.5 leading-relaxed">
                Raw source ingestion into immutable Parquet snapshot.
              </p>
            </div>

            {/* Silver Cleansing Only */}
            <div
              onClick={() => setExecutionScope('SILVER')}
              className={`p-3.5 rounded-xl border transition cursor-pointer flex flex-col justify-between ${
                executionScope === 'SILVER'
                  ? 'bg-cyan-600/15 border-cyan-500/60 shadow-md shadow-cyan-900/20'
                  : 'bg-[#152037]/60 border-slate-800 hover:border-slate-700 text-slate-400'
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-white">Silver Cleanse</span>
                <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">
                  CLEANSE
                </span>
              </div>
              <p className="text-[11px] text-slate-400 mt-1.5 leading-relaxed">
                Reads Bronze artifacts and applies schema governance.
              </p>
            </div>

            {/* Gold Publication Only */}
            <div
              onClick={() => setExecutionScope('GOLD')}
              className={`p-3.5 rounded-xl border transition cursor-pointer flex flex-col justify-between ${
                executionScope === 'GOLD'
                  ? 'bg-emerald-600/15 border-emerald-500/60 shadow-md shadow-emerald-900/20'
                  : 'bg-[#152037]/60 border-slate-800 hover:border-slate-700 text-slate-400'
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-white">Gold Publish</span>
                <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
                  PUBLISH
                </span>
              </div>
              <p className="text-[11px] text-slate-400 mt-1.5 leading-relaxed">
                Reads Silver data and writes to target with UPSERT.
              </p>
            </div>
          </div>
        </div>

        {/* STEP 4: PRIORITY & SAFETY DISPATCH */}
        <div className="p-5 rounded-2xl bg-[#11192e]/80 border border-slate-800 space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 items-center">
            {/* Priority Selector */}
            <div className="space-y-1.5">
              <label className="block text-xs font-semibold uppercase tracking-wider text-slate-300">
                Execution Priority
              </label>
              <select
                value={priority}
                onChange={(e) => setPriority(Number(e.target.value))}
                className="w-full bg-[#152037] border border-slate-700 hover:border-slate-600 rounded-xl px-3 py-2 text-xs text-white font-medium outline-none focus:ring-2 focus:ring-indigo-500/30 transition cursor-pointer"
              >
                <option value={1}>1 - Urgent / Critical Priority</option>
                <option value={2}>2 - High Operational Priority (Default)</option>
                <option value={3}>3 - Medium Priority</option>
                <option value={4}>4 - Below Normal Priority</option>
                <option value={5}>5 - Standard / Batch Priority</option>
              </select>
            </div>

            {/* Fencing Info */}
            <div className="p-3 rounded-xl bg-slate-900/60 border border-slate-800 flex items-center gap-2.5 text-xs text-slate-300">
              <ShieldCheck className="w-4 h-4 text-emerald-400 shrink-0" />
              <span>Advisory locks fence scope against concurrent runs automatically.</span>
            </div>
          </div>

          {/* Safety Confirmation Checkbox */}
          <div className="p-3 rounded-xl bg-[#152037]/50 border border-slate-800 flex items-start gap-2.5">
            <input
              type="checkbox"
              id="confirmExecution"
              checked={confirmed}
              onChange={(e) => setConfirmed(e.target.checked)}
              className="mt-0.5 w-3.5 h-3.5 rounded text-indigo-600 bg-slate-900 border-slate-700 focus:ring-indigo-500 cursor-pointer"
            />
            <label htmlFor="confirmExecution" className="text-xs text-slate-300 cursor-pointer select-none leading-relaxed">
              Confirm governed dispatch for <strong className="text-indigo-400 font-mono">{sourceTag}</strong> →{' '}
              <strong className="text-cyan-400 font-mono">{targetTag}</strong> (
              {datasetMode === 'ALL' ? 'all datasets' : `${selectedTableNames.length} datasets`},{' '}
              {executionScope} scope).
            </label>
          </div>

          {/* Action Buttons */}
          <div className="flex flex-col sm:flex-row items-center justify-between gap-3 pt-3 border-t border-slate-800/80">
            <div className="text-xs text-slate-400">
              Ready to process <strong className="text-white">{datasetMode === 'ALL' ? tablesForTarget.length : selectedTableNames.length}</strong> dataset(s).
            </div>

            <div className="flex items-center gap-3 w-full sm:w-auto">
              {onCancel && (
                <button
                  type="button"
                  onClick={onCancel}
                  className="px-4 py-2.5 rounded-xl border border-slate-700 text-xs font-semibold text-slate-400 hover:text-white hover:bg-slate-800 transition"
                >
                  Cancel
                </button>
              )}

              <button
                type="submit"
                disabled={!canSubmit}
                className="flex-1 sm:flex-initial px-6 py-2.5 bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 disabled:from-slate-800 disabled:to-slate-800 disabled:text-slate-500 text-white font-bold text-xs rounded-xl shadow-lg shadow-indigo-950/40 transition flex items-center justify-center gap-2 cursor-pointer disabled:cursor-not-allowed active:scale-95"
              >
                {triggerMutation.isPending ? (
                  <>
                    <Loader2 className="w-3.5 h-3.5 animate-spin text-white" />
                    <span>Dispatching...</span>
                  </>
                ) : (
                  <>
                    <Play className="w-3.5 h-3.5 fill-current text-white" />
                    <span>
                      Dispatch Run (
                      {datasetMode === 'ALL' ? tablesForTarget.length : selectedTableNames.length} Datasets)
                    </span>
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      </form>
    </div>
  );
};
