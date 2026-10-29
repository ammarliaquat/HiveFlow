import { HiveFlowLogo } from '../common/HiveFlowLogo';
import {
  LayoutDashboard,
  Layers,
  History,
  ActivitySquare,
  Network,
  Compass,
  Lock,
  ShieldCheck,
} from 'lucide-react';

export type NavTab = 'dashboard' | 'pipelines' | 'discovery' | 'runs' | 'integrations' | 'health' | 'users';

interface SidebarProps {
  currentTab: NavTab;
  onSelectTab: (tab: NavTab) => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ currentTab, onSelectTab }) => {
  const navItems = [
    {
      id: 'dashboard' as NavTab,
      label: 'Operations Dashboard',
      icon: LayoutDashboard,
      description: 'KPIs & live pipeline health',
    },
    {
      id: 'pipelines' as NavTab,
      label: 'Pipeline Catalogue',
      icon: Layers,
      description: 'Tables, keys & column mappings',
    },
    {
      id: 'discovery' as NavTab,
      label: 'Discovery & Promotion',
      icon: Compass,
      description: 'Crawler, Tier-1 & target mapping',
    },
    {
      id: 'runs' as NavTab,
      label: 'Execution History',
      icon: History,
      description: 'Run details, stages & throughput',
    },
    {
      id: 'integrations' as NavTab,
      label: 'Integrations & Sources',
      icon: Network,
      description: 'Database connections & connectors',
    },
    {
      id: 'health' as NavTab,
      label: 'System & Health',
      icon: ActivitySquare,
      description: 'Ledger certification & DB probes',
    },
    {
      id: 'users' as NavTab,
      label: 'Access & Governance',
      icon: ShieldCheck,
      description: 'Users, roles & audit observer',
    },
  ];


  return (
    <aside className="w-64 bg-[#0e1424] border-r border-slate-800/80 flex flex-col justify-between h-screen select-none flex-shrink-0 font-sans">
      <div>
        {/* Brand */}
        <div className="h-16 px-4 border-b border-slate-800/80 flex items-center">
          <HiveFlowLogo variant="horizontal" size="md" subtitle="Lakehouse Platform" />
        </div>

        {/* Navigation */}
        <nav className="p-3 space-y-1.5">
          <div className="px-3 pt-3 pb-1.5 text-xs font-semibold tracking-wider uppercase text-slate-400">
            Platform Views
          </div>
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = currentTab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => onSelectTab(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-left transition duration-150 group ${
                  isActive
                    ? 'bg-indigo-600/15 text-indigo-200 border border-indigo-500/30 font-medium shadow-sm'
                    : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800/50 border border-transparent'
                }`}
              >
                <Icon
                  className={`w-4 h-4 flex-shrink-0 transition-colors ${
                    isActive ? 'text-indigo-400' : 'text-slate-400 group-hover:text-slate-200'
                  }`}
                />
                <div className="min-w-0">
                  <span className="text-sm font-medium block truncate">{item.label}</span>
                </div>
              </button>
            );
          })}
        </nav>
      </div>

      {/* Footer / Boundary Status */}
      <div className="p-4 border-t border-slate-800/80 bg-[#0a0f1c]/70">
        <div className="flex items-center gap-2 mb-2">
          <div className="w-2 h-2 rounded-full bg-cyan-400 animate-pulse" />
          <span className="text-xs font-semibold text-slate-300">Milestone 2 Active</span>
        </div>
        <p className="text-xs text-slate-400 leading-relaxed mb-3">
          Governed execution controls & real-time operational workflow visualization.
        </p>
        <div className="flex items-center justify-between text-xs text-slate-500 pt-2 border-t border-slate-800/50">
          <span className="flex items-center gap-1">
            <Lock className="w-3 h-3 text-cyan-400" /> Governed Control Plane
          </span>
          <span className="font-mono">v3.9.0-M2</span>
        </div>
      </div>
    </aside>
  );
};
