// frontend/src/views/DiscoveryView.tsx
import React, { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { api } from '../api/client';
import type {
  CatalogTableItem,
  CatalogColumnItem,
  Tier2SubscriptionItem,
  PromoteResponse,
  DriftCheckResponse,
  IntegrationDetail,
  UnpromotedTablesResponse,
} from '../api/types';
import {
  Compass,
  Search,
  RefreshCw,
  Play,
  CheckCircle2,
  AlertTriangle,
  Database,
  Layers,
  Terminal,
  X,
  CheckSquare,
  Square,
  AlertOctagon,
  Sparkles,
  Server,
  Sliders,
} from 'lucide-react';


export const DiscoveryView: React.FC = () => {
  const { user, projectScope } = useAuth();
  const queryClient = useQueryClient();
  const isAdmin = user?.role?.toUpperCase() === 'ADMIN';

  // Active Tab
  const [activeTab, setActiveTab] = useState<'crawler' | 'catalog' | 'promote'>('crawler');

  // Step 1: Crawler State
  const [crawlSource, setCrawlSource] = useState<string>('');
  const [activeCrawlRunId, setActiveCrawlRunId] = useState<string | null>(null);
  const [isCrawlerRunning, setIsCrawlerRunning] = useState<boolean>(false);
  const [crawlerLogs, setCrawlerLogs] = useState<string>('');

  // Step 2: Catalog Review State
  const [catalogSourceFilter, setCatalogSourceFilter] = useState<string>('');
  const [catalogSearch, setCatalogSearch] = useState<string>('');
  const [selectedTableForCols, setSelectedTableForCols] = useState<CatalogTableItem | null>(null);

  // Step 3: Promotion & Mapping State
  const [promoSource, setPromoSource] = useState<string>('');
  const [promoTarget, setPromoTarget] = useState<string>('');
  const [promoScope, setPromoScope] = useState<'selected' | 'full'>('selected');
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [promoPriority, setPromoPriority] = useState<number>(1);
  const [stewardDeclared, setStewardDeclared] = useState<boolean>(false);
  const [promoResult, setPromoResult] = useState<PromoteResponse | null>(null);

  // Drift Check Modal State
  const [isDriftModalOpen, setIsDriftModalOpen] = useState<boolean>(false);
  const [driftResult, setDriftResult] = useState<DriftCheckResponse | null>(null);
  const [isCheckingDrift, setIsCheckingDrift] = useState<boolean>(false);

  // ── Queries ─────────────────────────────────────────────────────────────────

  // 1. Integrations (to get sources)
  const { data: integrationsData } = useQuery({
    queryKey: ['integrations', projectScope],
    queryFn: () => api.getIntegrations(projectScope),
  });

  const sources: string[] = (integrationsData?.integrations || [])
    .filter((i: IntegrationDetail) => i.supports_source)
    .map((i: IntegrationDetail) => i.tag);

  // Synchronize sources with projectScope and selection
  useEffect(() => {
    if (sources.length > 0) {
      if (projectScope && sources.includes(projectScope)) {
        if (crawlSource !== projectScope) setCrawlSource(projectScope);
        if (promoSource !== projectScope) setPromoSource(projectScope);
      } else {
        if (!crawlSource || !sources.includes(crawlSource)) {
          setCrawlSource(sources[0]);
        }
        if (!promoSource || !sources.includes(promoSource)) {
          setPromoSource(sources[0]);
        }
      }
    }
  }, [sources, projectScope]);

  // 2. Targets list
  const { data: targetsData } = useQuery({
    queryKey: ['discoveryTargets'],
    queryFn: () => api.getTargets(),
  });

  const targets: string[] = targetsData || [];

  useEffect(() => {
    if (targets.length > 0) {
      if (!promoTarget || !targets.includes(promoTarget)) {
        setPromoTarget(targets[0]);
      }
    }
  }, [targets, promoTarget]);

  // Clear selections, error states, and previous results on source/target change
  useEffect(() => {
    setSelectedTables([]);
    setStewardDeclared(false);
    setPromoResult(null);
    promoteMutation.reset();
  }, [promoSource, promoTarget]);

  // 3. Tier-1 Master Catalog
  const {
    data: catalogData,
    isLoading: isLoadingCatalog,
  } = useQuery({
    queryKey: ['discoveryCatalog', catalogSourceFilter || null, projectScope],
    queryFn: () => api.getCatalog(catalogSourceFilter || null, projectScope),
  });

  // 4. Column Schema for Inspector
  const {
    data: columnSchemaData,
    isLoading: isLoadingColumns,
  } = useQuery({
    queryKey: ['discoveryColumns', selectedTableForCols?.table_master_key],
    queryFn: () =>
      selectedTableForCols
        ? api.getCatalogColumns(selectedTableForCols.table_master_key)
        : Promise.resolve([]),
    enabled: !!selectedTableForCols,
  });

  // 5. Unpromoted Tables
  const {
    data: unpromotedData,
    isLoading: isLoadingUnpromoted,
    refetch: refetchUnpromoted,
  } = useQuery<UnpromotedTablesResponse>({
    queryKey: ['discoveryUnpromoted', promoSource, promoTarget, projectScope],
    queryFn: () =>
      promoSource && promoTarget
        ? api.getUnpromotedTables(promoSource, promoTarget, projectScope)
        : Promise.resolve({
            source_tag: promoSource || '',
            target_tag: promoTarget || '',
            unpromoted_tables: [],
            catalog_tables: [],
            promoted_tables: [],
          }),
    enabled: !!promoSource && !!promoTarget,
  });


  // 6. Current Tier-2 Subscriptions
  const {
    data: subscriptionsData,
    isLoading: isLoadingSubscriptions,
    refetch: refetchSubscriptions,
  } = useQuery({
    queryKey: ['discoverySubscriptions', promoSource, promoTarget, projectScope],
    queryFn: () =>
      promoSource
        ? api.getSubscriptions(promoSource, promoTarget || null, projectScope)
        : Promise.resolve([]),
    enabled: !!promoSource,
  });

  // ── Crawler Polling Effect ──────────────────────────────────────────────────
  useEffect(() => {
    let interval: any = null;
    if (activeCrawlRunId && isCrawlerRunning) {
      interval = setInterval(async () => {
        try {
          const statusRes = await api.getCrawlStatus(activeCrawlRunId);
          const logsRes = await api.getCrawlLogs(activeCrawlRunId);
          setCrawlerLogs(logsRes.logs || '');

          const state = (statusRes.status || '').toUpperCase();
          if (['SUCCESS', 'FAILED', 'CANCELLED', 'RECONCILIATION_FAILED'].includes(state)) {
            setIsCrawlerRunning(false);
            queryClient.invalidateQueries({ queryKey: ['discoveryCatalog'] });
            queryClient.invalidateQueries({ queryKey: ['discoveryUnpromoted'] });
          }
        } catch (err) {
          console.error('Error polling crawler:', err);
        }
      }, 3000);
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [activeCrawlRunId, isCrawlerRunning, queryClient]);

  // ── Mutations ───────────────────────────────────────────────────────────────

  // Launch Crawler Mutation
  const crawlMutation = useMutation({
    mutationFn: (src: string) => api.crawlSource(src, projectScope),
    onSuccess: (data) => {
      setActiveCrawlRunId(data.run_id);
      setIsCrawlerRunning(true);
      setCrawlerLogs(`Dispatched crawler job ${data.run_id} for source ${crawlSource}...\nStreaming logs...`);
    },
  });

  // Promote Mutation
  const promoteMutation = useMutation({
    mutationFn: () => {
      const allUnpromoted = unpromotedData?.unpromoted_tables || [];
      return api.promoteDatasets(
        {
          source_tag: promoSource,
          target_tag: promoTarget,
          table_names: promoScope === 'selected' ? selectedTables : allUnpromoted,
          priority: promoPriority,
          refresh_existing: promoScope === 'full',
          steward_confirmed: stewardDeclared,
        },
        projectScope
      );
    },
    onSuccess: (data) => {
      setPromoResult(data);
      setSelectedTables([]);
      setStewardDeclared(false);
      refetchUnpromoted();
      refetchSubscriptions();
      queryClient.invalidateQueries({ queryKey: ['pipelines'] });
    },
  });

  // Schema Drift Mutation
  const handleRunDriftCheck = async () => {
    setIsCheckingDrift(true);
    setIsDriftModalOpen(true);
    try {
      const res = await api.auditDrift(catalogSourceFilter || promoSource || null);
      setDriftResult(res);
    } catch (err: any) {
      console.error('Drift check failed:', err);
    } finally {
      setIsCheckingDrift(false);
    }
  };

  // Cache Clear Action
  const [cacheClearMsg, setCacheClearMsg] = useState<string | null>(null);
  const handleClearCache = async () => {
    try {
      const res = await api.clearCache();
      setCacheClearMsg(res.message);
      queryClient.invalidateQueries();
      setTimeout(() => setCacheClearMsg(null), 4000);
    } catch (err: any) {
      setCacheClearMsg('Cache clear failed: ' + err.message);
    }
  };

  // Filter Catalog items
  const catalogTables = (catalogData?.tables || []).filter((item: CatalogTableItem) => {
    if (!catalogSearch) return true;
    const term = catalogSearch.toLowerCase();
    return (
      item.source_table.toLowerCase().includes(term) ||
      item.source_tag.toLowerCase().includes(term) ||
      (item.schema_name && item.schema_name.toLowerCase().includes(term)) ||
      (item.business_key && item.business_key.toLowerCase().includes(term)) ||
      (item.cdc_strategy && item.cdc_strategy.toLowerCase().includes(term))
    );
  });

  const unpromotedList = unpromotedData?.unpromoted_tables || [];

  return (
    <div className="p-8 space-y-8 max-w-7xl mx-auto">
      {/* Page Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white flex items-center gap-3">
            <Compass className="w-7 h-7 text-indigo-400" />
            <span>Schema Discovery & Promotion</span>
            {projectScope && (
              <span className="text-xs font-mono px-2.5 py-0.5 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-indigo-300">
                {projectScope}
              </span>
            )}
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            Discover raw database schemas, review the Tier-1 Master Catalog, and map & promote datasets to Tier-2 execution control.
          </p>
        </div>

        {/* Global Header Actions */}
        <div className="flex items-center gap-3">
          <button
            onClick={handleRunDriftCheck}
            className="flex items-center gap-2 px-3.5 py-2 rounded-xl bg-amber-500/10 hover:bg-amber-500/20 border border-amber-500/30 text-xs font-semibold text-amber-300 transition shadow-sm"
          >
            <AlertTriangle className="w-4 h-4 text-amber-400" />
            <span>Audit Schema Drift</span>
          </button>

          <button
            onClick={handleClearCache}
            className="flex items-center gap-2 px-3.5 py-2 rounded-xl bg-[#1e293b]/70 hover:bg-[#1e293b] border border-slate-700/80 text-xs font-semibold text-slate-200 transition"
          >
            <RefreshCw className="w-3.5 h-3.5 text-slate-400" />
            <span>Clear Metadata Cache</span>
          </button>
        </div>
      </div>

      {cacheClearMsg && (
        <div className="bg-indigo-600/10 border border-indigo-500/30 rounded-xl p-3 text-xs text-indigo-300 flex items-center gap-2">
          <CheckCircle2 className="w-4 h-4 text-indigo-400 flex-shrink-0" />
          <span>{cacheClearMsg}</span>
        </div>
      )}

      {/* Main 3-Stage Navigation Tabs */}
      <div className="flex border-b border-slate-800 space-x-2">
        <button
          onClick={() => setActiveTab('crawler')}
          className={`flex items-center gap-2 py-3 px-5 text-sm font-semibold border-b-2 transition duration-150 ${
            activeTab === 'crawler'
              ? 'border-indigo-500 text-indigo-400 bg-indigo-500/5 rounded-t-xl'
              : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-700'
          }`}
        >
          <Terminal className="w-4 h-4" />
          <span>1. Discover Schemas (Crawler)</span>
        </button>

        <button
          onClick={() => setActiveTab('catalog')}
          className={`flex items-center gap-2 py-3 px-5 text-sm font-semibold border-b-2 transition duration-150 ${
            activeTab === 'catalog'
              ? 'border-indigo-500 text-indigo-400 bg-indigo-500/5 rounded-t-xl'
              : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-700'
          }`}
        >
          <Layers className="w-4 h-4" />
          <span>2. Tier-1 Master Catalog</span>
          {catalogData && (
            <span className="text-[10px] px-2 py-0.5 rounded-full bg-slate-800 text-slate-300 font-mono">
              {catalogData.total_tables}
            </span>
          )}
        </button>

        <button
          onClick={() => setActiveTab('promote')}
          className={`flex items-center gap-2 py-3 px-5 text-sm font-semibold border-b-2 transition duration-150 ${
            activeTab === 'promote'
              ? 'border-indigo-500 text-indigo-400 bg-indigo-500/5 rounded-t-xl'
              : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-700'
          }`}
        >
          <Sparkles className="w-4 h-4" />
          <span>3. Table Promotion & Mapping</span>
        </button>
      </div>

      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {/* TAB 1: SCHEMA CRAWLER                                                    */}
      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {activeTab === 'crawler' && (
        <div className="space-y-6">
          <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
            <h2 className="text-base font-bold text-white flex items-center gap-2 mb-1">
              <Database className="w-5 h-5 text-indigo-400" />
              <span>Step 1 — Run Source Schema Discovery Crawler</span>
            </h2>
            <p className="text-xs text-slate-400 mb-6">
              Connects to the source database system, introspects tables and columns, infers candidate unique keys and CDC watermarks, and writes directly to the Tier-1 Master Catalog (<code className="text-indigo-300 font-mono">metadata.db_tables</code>).
            </p>

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 items-end">
              <div>
                <label className="block text-xs font-semibold text-slate-400 mb-2">
                  Select Source Database System
                </label>
                <select
                  value={crawlSource}
                  onChange={(e) => setCrawlSource(e.target.value)}
                  disabled={isCrawlerRunning}
                  className="w-full bg-[#1e293b]/70 border border-slate-700 focus:border-indigo-500 rounded-xl px-3.5 py-2.5 text-xs text-white outline-none font-medium"
                >
                  {sources.map((s: string) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}

                </select>
              </div>

              <div>
                <button
                  onClick={() => crawlMutation.mutate(crawlSource)}
                  disabled={isCrawlerRunning || !crawlSource || crawlMutation.isPending}
                  className="w-full flex items-center justify-center gap-2 px-5 py-2.5 rounded-xl bg-gradient-to-r from-indigo-600 to-indigo-700 hover:from-indigo-500 hover:to-indigo-600 text-white text-xs font-bold transition shadow-lg shadow-indigo-600/25 disabled:opacity-50"
                >
                  {isCrawlerRunning || crawlMutation.isPending ? (
                    <RefreshCw className="w-4 h-4 animate-spin" />
                  ) : (
                    <Play className="w-4 h-4" />
                  )}
                  <span>{isCrawlerRunning ? 'Crawler Running...' : '🚀 Run Schema Crawler'}</span>
                </button>
              </div>

              <div className="text-xs text-slate-400">
                <span className="block text-[11px] text-slate-500">Operation Scope:</span>
                Introspects raw schema metadata without mutating production tables or promoting to active execution.
              </div>
            </div>
          </div>

          {/* Crawler Status & Logs Console */}
          <div className="bg-[#0a0f1d] border border-slate-800 rounded-2xl overflow-hidden shadow-2xl">
            <div className="px-5 py-3.5 bg-[#0f172a] border-b border-slate-800 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <Terminal className="w-4 h-4 text-indigo-400" />
                <span className="text-xs font-bold text-white tracking-wide uppercase">
                  Crawler Output Console
                </span>
                {activeCrawlRunId && (
                  <span className="text-[11px] font-mono text-indigo-300 bg-indigo-500/10 px-2 py-0.5 rounded border border-indigo-500/20">
                    Run ID: {activeCrawlRunId}
                  </span>
                )}
              </div>

              <div className="flex items-center gap-2">
                {isCrawlerRunning && (
                  <span className="flex items-center gap-1.5 text-xs text-emerald-400 font-semibold animate-pulse">
                    <span className="w-2 h-2 rounded-full bg-emerald-400"></span>
                    <span>Live Introspection Active</span>
                  </span>
                )}
              </div>
            </div>

            <div className="p-4 font-mono text-xs text-slate-300 max-h-[460px] overflow-y-auto whitespace-pre-wrap leading-relaxed">
              {crawlerLogs ? (
                crawlerLogs
              ) : (
                <div className="text-slate-500 py-12 text-center flex flex-col items-center justify-center gap-2">
                  <Terminal className="w-8 h-8 text-slate-600" />
                  <span>No active crawler run in current session.</span>
                  <span className="text-[11px] text-slate-600">
                    Select a source system above and click "Run Schema Crawler" to stream logs.
                  </span>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {/* TAB 2: TIER-1 MASTER CATALOG                                              */}
      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {activeTab === 'catalog' && (
        <div className="space-y-6">
          {/* Headline Stats Banner */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-5 shadow-lg flex items-center gap-4">
              <div className="w-12 h-12 rounded-xl bg-indigo-500/10 text-indigo-400 flex items-center justify-center border border-indigo-500/20">
                <Database className="w-6 h-6" />
              </div>
              <div>
                <span className="text-xs uppercase font-bold text-slate-400 tracking-wider">
                  Discovered Datasets
                </span>
                <div className="text-2xl font-bold text-white mt-0.5">
                  {catalogData?.total_tables || 0}
                </div>
              </div>
            </div>

            <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-5 shadow-lg flex items-center gap-4">
              <div className="w-12 h-12 rounded-xl bg-emerald-500/10 text-emerald-400 flex items-center justify-center border border-emerald-500/20">
                <Sliders className="w-6 h-6" />
              </div>
              <div>
                <span className="text-xs uppercase font-bold text-slate-400 tracking-wider">
                  Total Discovered Columns
                </span>
                <div className="text-2xl font-bold text-white mt-0.5">
                  {(catalogData?.total_columns || 0).toLocaleString()}
                </div>
              </div>
            </div>

            <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-5 shadow-lg flex items-center gap-4">
              <div className="w-12 h-12 rounded-xl bg-purple-500/10 text-purple-400 flex items-center justify-center border border-purple-500/20">
                <Sparkles className="w-6 h-6" />
              </div>
              <div>
                <span className="text-xs uppercase font-bold text-slate-400 tracking-wider">
                  Total Source Rows
                </span>
                <div className="text-2xl font-bold text-white mt-0.5">
                  {(catalogData?.total_rows || 0).toLocaleString()}
                </div>
              </div>
            </div>
          </div>

          {/* Catalog Filter Controls */}
          <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-4 flex flex-col sm:flex-row items-center justify-between gap-4">
            <div className="flex items-center gap-3 w-full sm:w-auto">
              <label className="text-xs font-semibold text-slate-400 whitespace-nowrap">
                Source Filter:
              </label>
              <select
                value={catalogSourceFilter}
                onChange={(e) => setCatalogSourceFilter(e.target.value)}
                className="bg-[#1e293b]/70 border border-slate-700 focus:border-indigo-500 rounded-xl px-3 py-2 text-xs text-white outline-none"
              >
                <option value="">(All Sources)</option>
                {sources.map((s: string) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}

              </select>
            </div>

            <div className="relative w-full sm:w-72">
              <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
              <input
                type="text"
                placeholder="Search tables, schemas, or keys..."
                value={catalogSearch}
                onChange={(e) => setCatalogSearch(e.target.value)}
                className="w-full bg-[#1e293b]/60 border border-slate-700/80 focus:border-indigo-500 rounded-xl pl-9 pr-3 py-2 text-xs text-white placeholder-slate-500 outline-none transition"
              />
            </div>
          </div>

          {/* Master Catalog Table */}
          <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
            <div className="p-4 border-b border-slate-800 flex items-center justify-between">
              <div className="flex items-center gap-2 text-xs text-slate-400">
                <Database className="w-4 h-4 text-indigo-400" />
                <span>
                  Tier-1 Discovered Tables: <strong className="text-white">{catalogTables.length}</strong>
                </span>
              </div>
              <span className="text-[11px] text-slate-500">
                💡 Click any row to open the Column Schema Inspector drawer
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse">
                <thead>
                  <tr className="bg-[#090d16]/70 border-b border-slate-800 text-[11px] uppercase tracking-wider text-slate-400 font-semibold select-none">
                    <th className="py-3 px-4">Master Key</th>
                    <th className="py-3 px-4">Source System</th>
                    <th className="py-3 px-4">Engine / Schema</th>
                    <th className="py-3 px-4">Dataset Name</th>
                    <th className="py-3 px-4">Rows / Cols</th>
                    <th className="py-3 px-4">Business Key</th>
                    <th className="py-3 px-4">Key Confidence</th>
                    <th className="py-3 px-4">CDC Strategy</th>
                    <th className="py-3 px-4">CDC Health</th>
                    <th className="py-3 px-4">Discovered Date</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 text-xs">
                  {isLoadingCatalog ? (
                    <tr>
                      <td colSpan={10} className="py-12 text-center text-slate-500">
                        Loading Tier-1 Master Catalog...
                      </td>
                    </tr>
                  ) : catalogTables.length === 0 ? (
                    <tr>
                      <td colSpan={10} className="py-12 text-center text-slate-500">
                        No discovered tables present. Run Step 1 Schema Crawler first.
                      </td>
                    </tr>
                  ) : (
                    catalogTables.map((tbl) => {
                      const isSelected = selectedTableForCols?.table_master_key === tbl.table_master_key;
                      return (
                        <tr
                          key={tbl.table_master_key}
                          onClick={() => setSelectedTableForCols(tbl)}
                          className={`cursor-pointer transition duration-150 ${
                            isSelected
                              ? 'bg-indigo-600/15 border-l-2 border-indigo-500'
                              : 'hover:bg-[#1e293b]/40'
                          }`}
                        >
                          <td className="py-3 px-4 font-mono text-slate-400">
                            #{tbl.table_master_key}
                          </td>
                          <td className="py-3 px-4 font-medium text-white">
                            {tbl.source_tag}
                          </td>
                          <td className="py-3 px-4 text-slate-400">
                            <span className="text-[11px] font-mono uppercase bg-slate-800 px-1.5 py-0.5 rounded mr-1">
                              {tbl.database_type || 'SQL'}
                            </span>
                            <span>{tbl.schema_name || 'public'}</span>
                          </td>
                          <td className="py-3 px-4 font-bold text-indigo-300">
                            {tbl.source_table}
                          </td>
                          <td className="py-3 px-4 font-mono text-slate-300">
                            {tbl.row_count.toLocaleString()} rows / {tbl.column_count} cols
                          </td>
                          <td className="py-3 px-4 font-mono text-slate-300">
                            {tbl.business_key || <span className="text-slate-500 italic">None</span>}
                          </td>
                          <td className="py-3 px-4">
                            <span
                              className={`px-2 py-0.5 rounded-full text-[10px] font-bold ${
                                tbl.key_confidence >= 80
                                  ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                                  : tbl.key_confidence >= 50
                                  ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                                  : 'bg-slate-700/50 text-slate-400'
                              }`}
                            >
                              {tbl.key_confidence}% ({tbl.key_source})
                            </span>
                          </td>
                          <td className="py-3 px-4">
                            <span className="font-mono text-[11px] text-cyan-300 bg-cyan-500/10 px-2 py-0.5 rounded border border-cyan-500/20">
                              {tbl.cdc_strategy}
                            </span>
                          </td>
                          <td className="py-3 px-4">
                            <span
                              className={`px-2 py-0.5 rounded-full text-[10px] font-bold ${
                                tbl.cdc_health === 'HEALTHY'
                                  ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                                  : 'bg-slate-700/50 text-slate-400'
                              }`}
                            >
                              {tbl.cdc_health || 'UNPROBED'}
                            </span>
                          </td>
                          <td className="py-3 px-4 text-slate-400 text-[11px] whitespace-nowrap">
                            {tbl.insert_date ? new Date(tbl.insert_date).toLocaleDateString() : 'N/A'}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* Slide-out Column Schema Inspector Drawer */}
          {selectedTableForCols && (
            <div className="bg-[#0f172a] border border-indigo-500/40 rounded-2xl p-6 shadow-2xl relative">
              <div className="flex items-start justify-between mb-4">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs uppercase font-bold tracking-wider text-indigo-400">
                      Column Schema Inspector
                    </span>
                    <span className="text-xs font-mono bg-indigo-500/10 text-indigo-300 px-2 py-0.5 rounded border border-indigo-500/20">
                      Master Key: #{selectedTableForCols.table_master_key}
                    </span>
                  </div>
                  <h3 className="text-lg font-bold text-white mt-1">
                    {selectedTableForCols.source_tag}.{selectedTableForCols.source_table}
                  </h3>
                </div>

                <button
                  onClick={() => setSelectedTableForCols(null)}
                  className="text-slate-400 hover:text-white p-1 rounded-lg hover:bg-slate-800 transition"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              {isLoadingColumns ? (
                <div className="py-12 text-center text-slate-500 text-xs">
                  Loading column definitions...
                </div>
              ) : (columnSchemaData || []).length === 0 ? (
                <div className="py-8 text-center text-slate-500 text-xs">
                  No columns recorded for this table.
                </div>
              ) : (
                <div className="overflow-x-auto max-h-96">
                  <table className="w-full text-left text-xs border-collapse">
                    <thead>
                      <tr className="bg-[#090d16]/70 border-b border-slate-800 text-[11px] uppercase tracking-wider text-slate-400 font-semibold select-none">
                        <th className="py-2.5 px-3">Seq</th>
                        <th className="py-2.5 px-3">Column Name</th>
                        <th className="py-2.5 px-3">Source Datatype</th>
                        <th className="py-2.5 px-3">Max Length</th>
                        <th className="py-2.5 px-3">Precision / Scale</th>
                        <th className="py-2.5 px-3">Nullable</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60 font-mono">
                      {(columnSchemaData || []).map((col: CatalogColumnItem, idx: number) => (
                        <tr key={idx} className="hover:bg-[#1e293b]/40">
                          <td className="py-2 px-3 text-slate-500">{col.column_seq ?? idx + 1}</td>
                          <td className="py-2 px-3 font-semibold text-slate-200">
                            {col.column_name}
                          </td>
                          <td className="py-2 px-3 text-cyan-300">{col.data_type}</td>
                          <td className="py-2 px-3 text-slate-400">
                            {col.data_length !== null ? col.data_length : '—'}
                          </td>
                          <td className="py-2 px-3 text-slate-400">
                            {col.data_precision !== null ? `${col.data_precision}, ${col.data_scale ?? 0}` : '—'}
                          </td>
                          <td className="py-2 px-3">
                            <span
                              className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${
                                col.is_nullable === 'NO'
                                  ? 'bg-rose-500/10 text-rose-400'
                                  : 'bg-slate-800 text-slate-400'
                              }`}
                            >
                              {col.is_nullable || 'YES'}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {/* TAB 3: TABLE PROMOTION & SOURCE-TO-TARGET MAPPING                          */}
      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {activeTab === 'promote' && (
        <div className="space-y-6">
          <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl p-6 shadow-xl">
            <h2 className="text-base font-bold text-white flex items-center gap-2 mb-1">
              <Sparkles className="w-5 h-5 text-indigo-400" />
              <span>Step 3 — Table Promotion & Target Destination Mapping</span>
            </h2>
            <p className="text-xs text-slate-400 mb-6">
              Promote discovered tables from Tier-1 (<code className="text-indigo-300 font-mono">metadata.db_tables</code>) into active Tier-2 execution control (<code className="text-indigo-300 font-mono">metadata.db_tables_etl</code>). Establishes authoritative Source-to-Target mapping, executes policy reconciliation, and materializes CDC cursors.
            </p>

            {/* Source & Target Pickers */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 p-5 bg-[#090d16]/70 border border-slate-800 rounded-xl mb-6">
              <div>
                <label className="block text-xs font-semibold text-slate-300 mb-2">
                  1. Source System
                </label>
                <select
                  value={promoSource}
                  onChange={(e) => {
                    setPromoSource(e.target.value);
                    setSelectedTables([]);
                  }}
                  className="w-full bg-[#1e293b]/70 border border-slate-700 focus:border-indigo-500 rounded-xl px-3.5 py-2.5 text-xs text-white outline-none font-medium"
                >
                  {sources.map((s: string) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
                <span className="block text-[11px] text-slate-500 mt-1">Discovered source catalog origin</span>
              </div>

              <div>
                <label className="block text-xs font-semibold text-slate-300 mb-2">
                  2. Target Destination System
                </label>
                <select
                  value={promoTarget}
                  onChange={(e) => {
                    setPromoTarget(e.target.value);
                    setSelectedTables([]);
                  }}
                  className="w-full bg-[#1e293b]/70 border border-slate-700 focus:border-indigo-500 rounded-xl px-3.5 py-2.5 text-xs text-white outline-none font-medium"
                >
                  {targets.map((t: string) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>

                <span className="block text-[11px] text-slate-500 mt-1">
                  Authoritative target database for Gold publication
                </span>
              </div>

              <div>
                <label className="block text-xs font-semibold text-slate-300 mb-2">
                  3. Execution Priority Level
                </label>
                <select
                  value={promoPriority}
                  onChange={(e) => setPromoPriority(Number(e.target.value))}
                  className="w-full bg-[#1e293b]/70 border border-slate-700 focus:border-indigo-500 rounded-xl px-3.5 py-2.5 text-xs text-white outline-none font-medium"
                >
                  <option value={1}>Priority 1 (Critical / Fast-Path)</option>
                  <option value={2}>Priority 2 (Standard)</option>
                  <option value={3}>Priority 3 (Batch / Low-Priority)</option>
                </select>
                <span className="block text-[11px] text-slate-500 mt-1">Pipeline scheduling order weight</span>
              </div>
            </div>

            {/* Scope Selection */}
            <div className="mb-6">
              <label className="block text-xs font-semibold text-slate-300 mb-2">
                Promotion Scope
              </label>
              <div className="flex gap-4">
                <button
                  type="button"
                  onClick={() => setPromoScope('selected')}
                  className={`flex-1 p-3.5 rounded-xl border text-left transition ${
                    promoScope === 'selected'
                      ? 'bg-indigo-600/10 border-indigo-500 text-white'
                      : 'bg-[#1e293b]/30 border-slate-800 text-slate-400 hover:border-slate-700'
                  }`}
                >
                  <div className="font-semibold text-xs mb-1">Selected Datasets</div>
                  <p className="text-[11px] text-slate-400">
                    Choose specific unpromoted tables from the catalog checklist below.
                  </p>
                </button>

                <button
                  type="button"
                  onClick={() => setPromoScope('full')}
                  className={`flex-1 p-3.5 rounded-xl border text-left transition ${
                    promoScope === 'full'
                      ? 'bg-indigo-600/10 border-indigo-500 text-white'
                      : 'bg-[#1e293b]/30 border-slate-800 text-slate-400 hover:border-slate-700'
                  }`}
                >
                  <div className="font-semibold text-xs mb-1">Full Dataset Refresh</div>
                  <p className="text-[11px] text-slate-400">
                    Adds all unpromoted tables and refreshes governed policies for existing subscriptions.
                  </p>
                </button>
              </div>
            </div>

            {/* Checklist of unpromoted tables (only if promoScope === 'selected') */}
            {promoScope === 'selected' && (
              <div className="mb-6 bg-[#090d16]/70 border border-slate-800 rounded-xl p-4">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h4 className="text-xs font-bold text-white uppercase tracking-wider">
                      Available Discovered Tables to Promote
                    </h4>
                    <p className="text-[11px] text-slate-400">
                      Tier-1 datasets not yet configured for target system <strong className="text-indigo-300">{promoTarget}</strong>.
                    </p>
                  </div>

                  {unpromotedList.length > 0 && (
                    <button
                      type="button"
                      onClick={() => {
                        if (selectedTables.length === unpromotedList.length) {
                          setSelectedTables([]);
                        } else {
                          setSelectedTables([...unpromotedList]);
                        }
                      }}
                      className="text-xs font-semibold text-indigo-400 hover:text-indigo-300 flex items-center gap-1.5"
                    >
                      {selectedTables.length === unpromotedList.length ? (
                        <>
                          <CheckSquare className="w-3.5 h-3.5" />
                          <span>Deselect All</span>
                        </>
                      ) : (
                        <>
                          <Square className="w-3.5 h-3.5" />
                          <span>Select All ({unpromotedList.length})</span>
                        </>
                      )}
                    </button>
                  )}
                </div>

                {isLoadingUnpromoted ? (
                  <div className="py-8 text-center text-slate-500 text-xs">
                    Loading unpromoted datasets...
                  </div>
                ) : unpromotedList.length === 0 ? (
                  <div className="py-8 text-center text-emerald-400 text-xs flex flex-col items-center gap-1">
                    <CheckCircle2 className="w-6 h-6 text-emerald-400" />
                    <span>All discovered datasets for this source are already promoted to target `{promoTarget}`!</span>
                  </div>
                ) : (
                  <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2.5 max-h-60 overflow-y-auto p-1">
                    {unpromotedList.map((tableName: string) => {
                      const isChecked = selectedTables.includes(tableName);
                      return (

                        <label
                          key={tableName}
                          className={`flex items-center gap-3 p-2.5 rounded-lg border text-xs cursor-pointer select-none transition ${
                            isChecked
                              ? 'bg-indigo-600/15 border-indigo-500 text-white font-semibold'
                              : 'bg-[#1e293b]/40 border-slate-800 text-slate-300 hover:border-slate-700'
                          }`}
                        >
                          <input
                            type="checkbox"
                            checked={isChecked}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setSelectedTables((prev) => [...prev, tableName]);
                              } else {
                                setSelectedTables((prev) => prev.filter((t) => t !== tableName));
                              }
                            }}
                            className="w-4 h-4 rounded text-indigo-600 bg-slate-800 border-slate-700 focus:ring-0"
                          />
                          <span className="truncate">{tableName}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>
            )}

            {/* Steward Declaration Checkbox */}
            <div className="p-4 bg-indigo-950/20 border border-indigo-500/30 rounded-xl mb-6">
              <label className="flex items-start gap-3 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={stewardDeclared}
                  onChange={(e) => setStewardDeclared(e.target.checked)}
                  className="w-4 h-4 mt-0.5 rounded text-indigo-600 bg-slate-800 border-slate-700 focus:ring-0"
                />
                <div className="text-xs">
                  <span className="font-bold text-white block">
                    📜 Steward Governance Declaration
                  </span>
                  <span className="text-slate-400 leading-relaxed block mt-0.5">
                    I confirm onboarding and governed policy reconciliation for{' '}
                    <strong className="text-indigo-300">
                      {promoScope === 'full'
                        ? unpromotedList.length > 0
                          ? `Full Dataset (${unpromotedList.length} unpromoted datasets)`
                          : 'Full Dataset (Policies Refresh)'
                        : `${selectedTables.length} selected dataset(s)`}
                    </strong>{' '}
                    into Target System <strong className="text-indigo-300">{promoTarget}</strong> with Execution Priority Level{' '}
                    <strong className="text-indigo-300">{promoPriority}</strong>.
                  </span>
                </div>
              </label>
            </div>

            {/* Submit Promotion Button */}
            <div>
              <button
                onClick={() => promoteMutation.mutate()}
                disabled={
                  !stewardDeclared ||
                  !promoTarget ||
                  !isAdmin ||
                  promoteMutation.isPending ||
                  (promoScope === 'selected' && selectedTables.length === 0)
                }
                className="w-full flex items-center justify-center gap-2 px-6 py-3 rounded-xl bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white text-xs font-bold transition shadow-lg shadow-emerald-600/25 disabled:opacity-50 cursor-pointer disabled:cursor-not-allowed"
              >
                {promoteMutation.isPending ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <CheckCircle2 className="w-4 h-4" />
                )}
                <span>
                  {promoteMutation.isPending
                    ? 'Materializing Subscriptions & Reconciling Policies...'
                    : promoScope === 'full'
                      ? unpromotedList.length > 0
                        ? `✅ Promote Full Dataset (${unpromotedList.length} Tables) to ${promoTarget}`
                        : `🔄 Refresh Policies & Priority for ${promoTarget}`
                      : `✅ Promote ${selectedTables.length} Selected Table(s) to ${promoTarget}`}
                </span>
              </button>
              {!isAdmin && (
                <span className="block text-[11px] text-amber-400 text-center mt-2">
                  🔒 Admin role required to execute governed dataset promotion.
                </span>
              )}
            </div>
          </div>

          {/* Error Alert Banner if Mutation Failed */}
          {promoteMutation.isError && (
            <div className="bg-rose-500/10 border border-rose-500/30 rounded-2xl p-5 shadow-xl text-xs space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <AlertOctagon className="w-5 h-5 text-rose-400 flex-shrink-0" />
                  <span className="font-bold text-rose-300 text-sm">Promotion / Onboarding Operation Failed</span>
                </div>
                <button
                  onClick={() => promoteMutation.reset()}
                  className="text-slate-400 hover:text-white text-xs px-2 py-1 rounded hover:bg-slate-800"
                >
                  Dismiss
                </button>
              </div>
              <p className="text-slate-300 font-mono ml-8">
                {(promoteMutation.error as any)?.message || 'An unexpected error occurred during table promotion.'}
              </p>
            </div>
          )}

          {/* Promotion Result Alert Banner */}
          {promoResult && (
            <div className="bg-[#0f172a] border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  {promoResult.failed && promoResult.failed.length > 0 ? (
                    <AlertTriangle className="w-6 h-6 text-amber-400 flex-shrink-0" />
                  ) : (
                    <CheckCircle2 className="w-6 h-6 text-emerald-400 flex-shrink-0" />
                  )}
                  <div>
                    <h3 className="text-sm font-bold text-white">{promoResult.message}</h3>
                    <span className="text-xs text-slate-400">
                      Tier-2 execution subscriptions updated in <code className="font-mono text-indigo-300">metadata.db_tables_etl</code>
                    </span>
                  </div>
                </div>
                <button
                  onClick={() => setPromoResult(null)}
                  className="text-slate-400 hover:text-white text-xs px-2 py-1 rounded hover:bg-slate-800"
                >
                  Dismiss
                </button>
              </div>

              {/* List of Successful Tables */}
              {promoResult.successful && promoResult.successful.length > 0 && (
                <div className="space-y-1.5">
                  <span className="text-xs font-semibold text-emerald-400">
                    Successfully Onboarded Datasets ({promoResult.successful.length}):
                  </span>
                  <div className="flex flex-wrap gap-2 max-h-32 overflow-y-auto p-2.5 bg-[#090d16] rounded-xl border border-slate-800">
                    {promoResult.successful.map((tbl: string) => (
                      <span key={tbl} className="text-[11px] font-mono px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-300 border border-emerald-500/20">
                        {tbl}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* List of Failed Tables */}
              {promoResult.failed && promoResult.failed.length > 0 && (
                <div className="space-y-1.5">
                  <span className="text-xs font-semibold text-rose-400">
                    Onboarding Failures ({promoResult.failed.length}):
                  </span>
                  <div className="max-h-40 overflow-y-auto border border-rose-500/20 rounded-xl bg-rose-950/10 divide-y divide-rose-500/10 text-xs">
                    {promoResult.failed.map((f: any, idx: number) => (
                      <div key={idx} className="p-2.5 flex items-start justify-between gap-4 font-mono text-[11px]">
                        <span className="font-bold text-rose-300">{f.table_name || f.dataset || `Item #${idx+1}`}</span>
                        <span className="text-slate-400">{f.error || f.reason || JSON.stringify(f)}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Policy Refresh Breakdown */}
              {promoResult.policy_refresh && (
                <div className="space-y-2">
                  <span className="text-xs font-semibold text-slate-400">
                    Governed Policy Reconciliation:
                  </span>
                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs bg-[#090d16] p-3 rounded-xl border border-slate-800 font-mono">
                    <div className="text-emerald-400">
                      Updated: {promoResult.policy_refresh.updated.length}
                    </div>
                    <div className="text-slate-400">
                      Unchanged: {promoResult.policy_refresh.unchanged.length}
                    </div>
                    <div className="text-amber-400">
                      Skipped: {promoResult.policy_refresh.skipped.length}
                    </div>
                    <div className="text-rose-400">
                      Failed: {promoResult.policy_refresh.failed.length}
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Current Tier-2 Subscriptions Table */}
          <div className="bg-[#0f172a]/90 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
            <div className="p-4 border-b border-slate-800 flex items-center justify-between">
              <div>
                <h3 className="text-xs font-bold text-white uppercase tracking-wider flex items-center gap-2">
                  <Server className="w-4 h-4 text-indigo-400" />
                  <span>Current Tier-2 Subscriptions ({promoSource} → {promoTarget})</span>
                </h3>
                <span className="text-[11px] text-slate-400">
                  Active execution controls registered in metadata.db_tables_etl
                </span>
              </div>
              <button
                onClick={() => refetchSubscriptions()}
                className="text-xs text-slate-400 hover:text-white flex items-center gap-1"
              >
                <RefreshCw className="w-3 h-3" />
                <span>Refresh</span>
              </button>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse text-xs">
                <thead>
                  <tr className="bg-[#090d16]/70 border-b border-slate-800 text-[11px] uppercase tracking-wider text-slate-400 font-semibold select-none">
                    <th className="py-3 px-4">Dataset Name</th>
                    <th className="py-3 px-4">Target Destination</th>
                    <th className="py-3 px-4">Execution Status</th>
                    <th className="py-3 px-4">Load Method</th>
                    <th className="py-3 px-4">Business Key</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60">
                  {isLoadingSubscriptions ? (
                    <tr>
                      <td colSpan={5} className="py-8 text-center text-slate-500">
                        Loading subscriptions...
                      </td>
                    </tr>
                  ) : (subscriptionsData || []).length === 0 ? (
                    <tr>
                      <td colSpan={5} className="py-8 text-center text-slate-500">
                        No datasets have been promoted for this source and target yet.
                      </td>
                    </tr>
                  ) : (
                    (subscriptionsData || []).map((sub: Tier2SubscriptionItem, idx: number) => (
                      <tr key={idx} className="hover:bg-[#1e293b]/40">
                        <td className="py-3 px-4 font-bold text-indigo-300">
                          {sub.source_table}
                        </td>
                        <td className="py-3 px-4 font-medium text-slate-300">
                          {sub.target_tag}
                        </td>
                        <td className="py-3 px-4">
                          <span
                            className={`px-2 py-0.5 rounded-full text-[10px] font-bold ${
                              sub.execution_status.includes('Active')
                                ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                                : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                            }`}
                          >
                            {sub.execution_status}
                          </span>
                        </td>
                        <td className="py-3 px-4 font-mono text-cyan-300">
                          {sub.load_method}
                        </td>
                        <td className="py-3 px-4 font-mono text-slate-400">
                          {sub.business_key || '—'}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {/* SCHEMA DRIFT AUDIT MODAL                                                  */}
      {/* ══════════════════════════════════════════════════════════════════════════ */}
      {isDriftModalOpen && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="bg-[#0f172a] border border-slate-800 rounded-2xl max-w-3xl w-full p-6 shadow-2xl space-y-6">
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-amber-500/10 border border-amber-500/20 flex items-center justify-center text-amber-400">
                  <AlertTriangle className="w-5 h-5" />
                </div>
                <div>
                  <h3 className="text-base font-bold text-white">Schema Drift Audit</h3>
                  <p className="text-xs text-slate-400">
                    Compares Tier-1 Master Catalog keys with Tier-2 Subscriber Control keys.
                  </p>
                </div>
              </div>

              <button
                onClick={() => setIsDriftModalOpen(false)}
                className="text-slate-400 hover:text-white p-1 rounded-lg hover:bg-slate-800 transition"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {isCheckingDrift ? (
              <div className="py-12 text-center text-slate-400 flex flex-col items-center justify-center gap-2">
                <RefreshCw className="w-6 h-6 animate-spin text-amber-400" />
                <span>Running authoritative schema drift audit across metadata tables...</span>
              </div>
            ) : driftResult ? (
              <div className="space-y-4">
                <div
                  className={`p-4 rounded-xl border text-xs flex items-center gap-3 ${
                    driftResult.total_mismatches > 0
                      ? 'bg-rose-500/10 border-rose-500/30 text-rose-300'
                      : 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
                  }`}
                >
                  {driftResult.total_mismatches > 0 ? (
                    <AlertOctagon className="w-5 h-5 text-rose-400 flex-shrink-0" />
                  ) : (
                    <CheckCircle2 className="w-5 h-5 text-emerald-400 flex-shrink-0" />
                  )}
                  <div>
                    <strong className="block font-bold">{driftResult.status}</strong>
                    <span>{driftResult.message}</span>
                  </div>
                </div>

                {driftResult.mismatches.length > 0 && (
                  <div className="overflow-x-auto max-h-64 border border-slate-800 rounded-xl">
                    <table className="w-full text-left text-xs border-collapse">
                      <thead>
                        <tr className="bg-[#090d16] border-b border-slate-800 text-[11px] uppercase tracking-wider text-slate-400 font-semibold">
                          <th className="py-2.5 px-3">Source</th>
                          <th className="py-2.5 px-3">Table</th>
                          <th className="py-2.5 px-3">Tier-2 Key</th>
                          <th className="py-2.5 px-3">Catalog Key</th>
                          <th className="py-2.5 px-3">Alert</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800/60 font-mono">
                        {driftResult.mismatches.map((m, idx) => (
                          <tr key={idx} className="hover:bg-slate-800/30">
                            <td className="py-2 px-3 text-white">{m.source_tag}</td>
                            <td className="py-2 px-3 text-indigo-300">{m.source_table}</td>
                            <td className="py-2 px-3 text-rose-400">{m.etl_key || 'NULL'}</td>
                            <td className="py-2 px-3 text-emerald-400">{m.catalog_key || 'NULL'}</td>
                            <td className="py-2 px-3 text-amber-400 text-[10px]">{m.alert}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ) : null}

            <div className="flex justify-end">
              <button
                onClick={() => setIsDriftModalOpen(false)}
                className="px-4 py-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-xs font-semibold text-white transition"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
