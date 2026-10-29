import React from 'react';
import { useAuth } from '../../context/AuthContext';
import { ThemeSelector } from '../common/ThemeSelector';
import {
  FolderKanban,
  LogOut,
  ChevronDown,
  Server,
  Play,
} from 'lucide-react';

interface TopbarProps {
  onOpenTriggerModal?: () => void;
}

export const Topbar: React.FC<TopbarProps> = ({ onOpenTriggerModal }) => {
  const { user, logout, projectScope, setProjectScope } = useAuth();

  return (
    <header className="h-16 bg-[#0e1424]/90 backdrop-blur-md border-b border-slate-800/80 px-6 flex items-center justify-between z-20 font-sans">
      {/* Left: Project Scope Selector & Breadcrumb */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2 text-xs text-slate-400">
          <FolderKanban className="w-4 h-4 text-indigo-400" />
          <span className="font-medium text-slate-300">Scope:</span>
        </div>

        {/* Project Selector Dropdown */}
        <div className="relative">
          <select
            value={projectScope || ''}
            onChange={(e) => setProjectScope(e.target.value || null)}
            className="appearance-none bg-[#151f33] border border-slate-700/80 hover:border-slate-600 rounded-xl pl-3.5 pr-9 py-2 text-xs font-semibold text-slate-100 outline-none cursor-pointer focus:ring-2 focus:ring-indigo-500/25 transition"
          >
            <option value="">All Projects (Global Scope)</option>
            {user?.allowed_projects.map((proj) => (
              <option key={proj} value={proj}>
                {proj}
              </option>
            ))}
          </select>
          <ChevronDown className="w-3.5 h-3.5 text-slate-400 absolute right-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
        </div>

        <div className="h-4 w-[1px] bg-slate-800" />

        {/* Environment Badge */}
        <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-[11px] font-semibold tracking-wide">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
          PROD ENVIRONMENT
        </div>

        {/* Authority State Pill */}
        <div className="hidden lg:flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-cyan-500/10 border border-cyan-500/20 text-cyan-400 text-[11px] font-medium">
          <Server className="w-3 h-3" />
          Metadata Ledger Verified
        </div>
      </div>

      {/* Right: Actions, Theme & User Profile */}
      <div className="flex items-center gap-3">
        {/* Built-in Theme & Mode Selector */}
        <ThemeSelector />

        {onOpenTriggerModal && (
          <button
            onClick={onOpenTriggerModal}
            className="flex items-center gap-2 px-3.5 py-1.5 bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white text-xs font-semibold rounded-xl shadow-lg shadow-indigo-600/25 transition duration-150 active:scale-95"
            title="Trigger a new governed pipeline run"
          >
            <Play className="w-3.5 h-3.5 fill-current text-indigo-100" />
            <span>Trigger Run</span>
          </button>
        )}

        {/* User Pill */}
        <div className="flex items-center gap-2.5 pl-3 pr-2 py-1.5 bg-[#151f33] border border-slate-700/70 rounded-xl">
          <div className="w-6 h-6 rounded-lg bg-indigo-600/30 border border-indigo-500/40 flex items-center justify-center text-xs font-bold text-indigo-300">
            {user?.username ? user.username.charAt(0).toUpperCase() : 'U'}
          </div>
          <div className="flex flex-col text-left">
            <span className="text-xs font-semibold text-slate-200 leading-none">
              {user?.username}
            </span>
            <span className="text-[10px] text-slate-400 leading-none mt-1">
              {user?.role}
            </span>
          </div>

          <button
            onClick={() => logout()}
            title="Sign out of console"
            className="ml-2 p-1 text-slate-400 hover:text-rose-400 hover:bg-rose-500/10 rounded-lg transition"
          >
            <LogOut className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
    </header>
  );
};
