import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { api } from '../api/client';
import type { PipelineItem } from '../api/types';
import {
  Search,
  Key,
  ArrowRight,
  X,
  FileSpreadsheet,
  Database,
} from 'lucide-react';

export const PipelinesView: React.FC = () => {
  const { projectScope } = useAuth();
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedPipeline, setSelectedPipeline] = useState<PipelineItem | null>(null);

  const {
    data: pipelinesData,
    isLoading,
  } = useQuery({
    queryKey: ['pipelines', projectScope],
    queryFn: () => api.getPipelines(projectScope),
  });

  const {
    data: detailData,
    isLoading: isLoadingDetail,
  } = useQuery({
    queryKey: ['pipelineDetail', selectedPipeline?.source_tag, selectedPipeline?.table_name],
    queryFn: () =>
      selectedPipeline
        ? api.getPipelineDetail(selectedPipeline.source_tag, selectedPipeline.table_name)
        : null,
    enabled: !!selectedPipeline,
  });

  const pipelines: PipelineItem[] = Array.isArray(pipelinesData)
    ? pipelinesData
    : (pipelinesData as any)?.pipelines || [];

  const filteredPipelines = pipelines.filter((pipe) => {
    if (!searchTerm) return true;
    const term = searchTerm.toLowerCase();
    const tableName = (pipe.source_table || pipe.table_name || '').toLowerCase();
    const sourceTag = (pipe.source_tag || '').toLowerCase();
    const targetTable = (pipe.target_table || pipe.target_table_name || '').toLowerCase();
    return (
      tableName.includes(term) ||
      sourceTag.includes(term) ||
      targetTable.includes(term) ||
      (pipe.cdc_strategy && pipe.cdc_strategy.toLowerCase().includes(term))
    );
  });

  return (
    <div className="p-8 space-y-8 max-w-7xl mx-auto relative">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            Pipeline Catalogue
            {projectScope && (
              <span className="text-xs font-mono px-2.5 py-0.5 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-indigo-300">
                {projectScope}
              </span>
            )}
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            Authoritative metadata repository mappings, CDC watermarks, and business keys.
          </p>
        </div>

        {/* Search */}
        <div className="relative w-full sm:w-72">
          <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            placeholder="Search pipelines by name or tag..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full bg-[#1e293b]/60 border border-slate-700/80 focus:border-indigo-500 rounded-xl pl-9 pr-3 py-2 text-xs text-white placeholder-slate-500 outline-none transition"
          />
        </div>
      </div>

      {/* Main Table Card */}
      <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
        <div className="p-4 border-b border-slate-800 flex items-center justify-between">
          <div className="flex items-center gap-2 text-xs text-slate-400">
            <Database className="w-4 h-4 text-indigo-400" />
            <span>
              Total Configured Pipelines:{' '}
              <strong className="text-white">{filteredPipelines.length}</strong>
            </span>
          </div>
          <span className="text-[11px] text-slate-500">Click row to inspect column mappings</span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-[#090d16]/70 border-b border-slate-800 text-[11px] uppercase tracking-wider text-slate-400 font-semibold select-none">
                <th className="py-3 px-4">Status</th>
                <th className="py-3 px-4">Priority</th>
                <th className="py-3 px-4">Source & Table</th>
                <th className="py-3 px-4">Target Table</th>
                <th className="py-3 px-4">Sync Strategy</th>
                <th className="py-3 px-4">Business Keys</th>
                <th className="py-3 px-4 text-right">Last Row Count</th>
                <th className="py-3 px-4 text-right">Last Success</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 text-xs">
              {isLoading ? (
                <tr>
                  <td colSpan={8} className="py-12 text-center text-slate-500">
                    Loading pipeline catalogue...
                  </td>
                </tr>
              ) : filteredPipelines.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-12 text-center text-slate-500">
                    No pipelines found matching your filter criteria.
                  </td>
                </tr>
              ) : (
                filteredPipelines.map((pipe) => {
                  const isSelected =
                    selectedPipeline?.table_name === pipe.table_name &&
                    selectedPipeline?.source_tag === pipe.source_tag &&
                    selectedPipeline?.target_tag === pipe.target_tag;

                  return (
                    <tr
                      key={pipe.table_id || `${pipe.source_tag}_${pipe.target_tag || 'def'}_${pipe.table_name}`}
                      onClick={() => setSelectedPipeline(pipe)}
                      className={`cursor-pointer transition duration-150 ${
                        isSelected
                          ? 'bg-indigo-600/10 border-l-2 border-indigo-500'
                          : 'hover:bg-[#1e293b]/40'
                      }`}
                    >
                      {/* Active Status */}
                      <td className="py-3 px-4 whitespace-nowrap">
                        <span
                          className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-bold ${
                            pipe.is_active
                              ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                              : 'bg-slate-500/10 text-slate-400 border border-slate-500/20'
                          }`}
                        >
                          <span
                            className={`w-1.5 h-1.5 rounded-full ${
                              pipe.is_active ? 'bg-emerald-400' : 'bg-slate-400'
                            }`}
                          />
                          {pipe.is_active ? 'ACTIVE' : 'INACTIVE'}
                        </span>
                      </td>

                      {/* Priority */}
                      <td className="py-3 px-4 whitespace-nowrap font-mono text-slate-300">
                        <span className="px-2 py-0.5 rounded bg-slate-800 text-[11px] font-semibold text-slate-300">
                          P{pipe.priority}
                        </span>
                      </td>

                      {/* Source & Table */}
                      <td className="py-3 px-4 whitespace-nowrap">
                        <div className="font-semibold text-slate-200">{pipe.table_name}</div>
                        <div className="text-[10px] font-mono text-slate-500">{pipe.source_tag}</div>
                      </td>

                      {/* Target Table */}
                      <td className="py-3 px-4 whitespace-nowrap text-slate-300">
                        <span className="font-mono text-slate-300">{pipe.target_table_name}</span>
                        <span className="text-[10px] text-slate-500 block">({pipe.target_tag})</span>
                      </td>

                      {/* Strategy */}
                      <td className="py-3 px-4 whitespace-nowrap">
                        {pipe.is_cdc ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-cyan-500/10 border border-cyan-500/20 text-cyan-300 text-[10px] font-medium font-mono">
                            CDC: {pipe.cdc_strategy || 'MODIFIED'}
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px] font-medium font-mono">
                            FULL SNAPSHOT
                          </span>
                        )}
                        {pipe.watermark_column && (
                          <span className="text-[10px] text-slate-500 block mt-0.5 truncate max-w-[120px]">
                            wm: {pipe.watermark_column}
                          </span>
                        )}
                      </td>

                      {/* Business Keys */}
                      <td className="py-3 px-4 whitespace-nowrap">
                        {pipe.business_keys && pipe.business_keys.length > 0 ? (
                          <div className="flex flex-wrap gap-1 max-w-[180px]">
                            {pipe.business_keys.map((k) => (
                              <span
                                key={k}
                                className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded bg-indigo-500/10 text-indigo-300 text-[10px] font-mono border border-indigo-500/20"
                              >
                                <Key className="w-2.5 h-2.5 text-indigo-400" />
                                {k}
                              </span>
                            ))}
                          </div>
                        ) : (
                          <span className="text-slate-500 text-[11px] italic">None configured</span>
                        )}
                      </td>

                      {/* Last Row Count */}
                      <td className="py-3 px-4 whitespace-nowrap text-right font-mono text-slate-300">
                        {pipe.last_row_count != null
                          ? pipe.last_row_count.toLocaleString()
                          : '-'}
                      </td>

                      {/* Last Success */}
                      <td className="py-3 px-4 whitespace-nowrap text-right text-slate-500 text-[11px]">
                        {pipe.last_success_timestamp || '-'}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Slide-out Column Mapping Drawer */}
      {selectedPipeline && (
        <div className="fixed inset-y-0 right-0 w-full max-w-xl bg-[#0f172a] border-l border-slate-800 shadow-2xl z-50 flex flex-col animate-slideLeft">
          {/* Drawer Header */}
          <div className="p-6 border-b border-slate-800 flex items-start justify-between bg-[#090d16]/80">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-indigo-500/10 text-indigo-300 border border-indigo-500/20">
                  {selectedPipeline.source_tag}
                </span>
                <span className="text-slate-600">→</span>
                <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-300 border border-emerald-500/20">
                  {selectedPipeline.target_tag}
                </span>
              </div>
              <h2 className="text-lg font-bold text-white tracking-tight">
                {selectedPipeline.table_name}
              </h2>
              <p className="text-xs text-slate-400 mt-0.5">
                Target Table: <span className="text-slate-200 font-mono">{selectedPipeline.target_table_name}</span>
              </p>
            </div>

            <button
              onClick={() => setSelectedPipeline(null)}
              className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Drawer Body: Column Mappings */}
          <div className="p-6 flex-1 overflow-y-auto space-y-6">
            <div>
              <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 flex items-center gap-2">
                <FileSpreadsheet className="w-4 h-4 text-indigo-400" />
                Authoritative Column Mappings ({detailData?.columns.length || 0})
              </h3>

              {isLoadingDetail ? (
                <div className="py-8 text-center text-slate-500 text-xs">
                  Loading column metadata...
                </div>
              ) : !detailData?.columns || detailData.columns.length === 0 ? (
                <div className="p-4 rounded-xl bg-slate-800/40 border border-slate-800 text-xs text-slate-400 text-center">
                  No explicit column overrides registered. Default 1:1 metadata projection applies.
                </div>
              ) : (
                <div className="space-y-2">
                  {detailData.columns.map((col, idx) => (
                    <div
                      key={idx}
                      className="bg-[#1e293b]/50 border border-slate-800 rounded-xl p-3 flex items-center justify-between text-xs"
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-semibold text-slate-200 font-mono">
                            {col.source_column}
                          </span>
                          <ArrowRight className="w-3 h-3 text-slate-600 flex-shrink-0" />
                          <span className="font-semibold text-emerald-300 font-mono">
                            {col.target_column}
                          </span>
                        </div>
                        <div className="flex items-center gap-2 mt-1 text-[11px] text-slate-500 font-mono">
                          <span>{col.canonical_type || 'STRING'}</span>
                          {col.transformation && (
                            <span className="text-indigo-400 font-sans">
                              (expr: {col.transformation})
                            </span>
                          )}
                        </div>
                      </div>

                      <div className="flex items-center gap-1.5 flex-shrink-0">
                        {col.is_business_key && (
                          <span className="px-1.5 py-0.5 rounded bg-indigo-500/10 border border-indigo-500/20 text-indigo-300 text-[10px] font-bold flex items-center gap-0.5">
                            <Key className="w-2.5 h-2.5" />
                            KEY
                          </span>
                        )}
                        {col.is_surrogate_key && (
                          <span className="px-1.5 py-0.5 rounded bg-purple-500/10 border border-purple-500/20 text-purple-300 text-[10px] font-bold">
                            SURROGATE
                          </span>
                        )}
                        {!col.is_nullable && (
                          <span className="px-1.5 py-0.5 rounded bg-amber-500/10 border border-amber-500/20 text-amber-300 text-[10px] font-semibold">
                            NOT NULL
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
