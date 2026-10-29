// frontend/src/components/common/ThemeSelector.tsx
import React, { useState, useRef, useEffect } from 'react';
import { useTheme, THEME_PRESETS, type ThemePreset } from '../../context/ThemeContext';
import { Sun, Moon, Palette, Check, Sparkles } from 'lucide-react';

export const ThemeSelector: React.FC = () => {
  const { mode, theme, activeConfig, toggleMode, setMode, setTheme } = useTheme();
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const isLight = mode === 'light';

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };
    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isOpen]);

  return (
    <div className="relative flex items-center gap-1.5" ref={dropdownRef}>
      {/* 1. Quick Light/Dark Mode Toggle Button */}
      <button
        onClick={toggleMode}
        title={isLight ? 'Switch to Dark Mode' : 'Switch to Light Mode'}
        className={`p-2 rounded-xl border transition-all duration-200 active:scale-95 flex items-center justify-center group ${
          isLight
            ? 'bg-white hover:bg-slate-100 border-slate-200 text-slate-700 shadow-sm'
            : 'bg-slate-800/60 hover:bg-slate-800 border-slate-700/60 text-slate-300'
        }`}
        aria-label="Toggle Color Mode"
      >
        {isLight ? (
          <Moon className="w-4 h-4 text-indigo-600 transition-transform duration-300 group-hover:-rotate-12" />
        ) : (
          <Sun className="w-4 h-4 text-amber-400 transition-transform duration-300 group-hover:rotate-45" />
        )}
      </button>

      {/* 2. Theme Preset Palette Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        title="Theme & Appearance Preferences"
        className={`flex items-center gap-2 px-2.5 py-1.5 rounded-xl border transition-all duration-200 text-xs font-semibold ${
          isOpen
            ? isLight
              ? 'bg-indigo-50 border-indigo-400 text-indigo-900 shadow-md'
              : 'bg-slate-800 border-indigo-500/50 text-white shadow-lg shadow-indigo-500/10'
            : isLight
              ? 'bg-white hover:bg-slate-100 border-slate-200 text-slate-800 shadow-sm'
              : 'bg-slate-800/60 hover:bg-slate-800 border-slate-700/60 text-slate-200'
        }`}
        aria-label="Theme Settings"
      >
        <Palette className={`w-3.5 h-3.5 ${isLight ? 'text-indigo-600' : 'text-indigo-400'}`} />
        <span className="hidden sm:inline-block max-w-[90px] truncate">{activeConfig.name}</span>
        {/* Dynamic theme color indicator dot */}
        <span
          className="w-2.5 h-2.5 rounded-full border border-black/20 shadow-sm"
          style={{ backgroundColor: activeConfig.primaryColor }}
        />
      </button>

      {/* 3. Floating Theme & Appearance Menu */}
      {isOpen && (
        <div
          className={`absolute right-0 top-full mt-2 w-80 rounded-2xl border shadow-2xl z-50 p-4 transition-all duration-150 animate-in fade-in zoom-in-95 ${
            isLight
              ? 'bg-white border-slate-200 text-slate-900 shadow-xl'
              : 'bg-[#0e1424] border-slate-700/80 text-white shadow-2xl'
          }`}
        >
          {/* Header */}
          <div
            className={`flex items-center justify-between pb-3 mb-3 border-b ${
              isLight ? 'border-slate-200' : 'border-slate-800/80'
            }`}
          >
            <div className="flex items-center gap-2">
              <Sparkles className="w-4 h-4 text-indigo-500" />
              <h3
                className={`text-xs font-bold uppercase tracking-wider ${
                  isLight ? 'text-slate-900' : 'text-white'
                }`}
              >
                Display & Theme
              </h3>
            </div>
            <span
              className={`text-[10px] font-mono font-medium px-2 py-0.5 rounded-full ${
                isLight ? 'bg-slate-100 text-slate-600' : 'bg-slate-800 text-slate-400'
              }`}
            >
              {mode.toUpperCase()}
            </span>
          </div>

          {/* Mode Switcher (Dark vs Light segmented button) */}
          <div className="mb-4">
            <label
              className={`block text-[11px] font-semibold uppercase tracking-wider mb-2 ${
                isLight ? 'text-slate-600' : 'text-slate-400'
              }`}
            >
              Appearance Mode
            </label>
            <div
              className={`grid grid-cols-2 gap-1.5 p-1 rounded-xl border ${
                isLight ? 'bg-slate-100 border-slate-200' : 'bg-slate-900/80 border-slate-800/80'
              }`}
            >
              <button
                type="button"
                onClick={() => setMode('dark')}
                className={`flex items-center justify-center gap-2 py-1.5 px-3 rounded-lg text-xs font-medium transition ${
                  mode === 'dark'
                    ? 'bg-slate-800 text-white shadow-sm font-semibold'
                    : isLight
                      ? 'text-slate-600 hover:text-slate-900'
                      : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                <Moon className="w-3.5 h-3.5 text-indigo-400" />
                <span>Dark Mode</span>
              </button>
              <button
                type="button"
                onClick={() => setMode('light')}
                className={`flex items-center justify-center gap-2 py-1.5 px-3 rounded-lg text-xs font-medium transition ${
                  mode === 'light'
                    ? 'bg-white text-slate-900 shadow-sm font-semibold border border-slate-200'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                <Sun className="w-3.5 h-3.5 text-amber-500" />
                <span>Light Mode</span>
              </button>
            </div>
          </div>

          {/* Built-in Theme Presets */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label
                className={`text-[11px] font-semibold uppercase tracking-wider ${
                  isLight ? 'text-slate-600' : 'text-slate-400'
                }`}
              >
                Built-in Themes (4 Options)
              </label>
            </div>

            <div className="space-y-1.5 max-h-60 overflow-y-auto pr-0.5">
              {THEME_PRESETS.map((preset) => {
                const isActive = theme === preset.id;
                return (
                  <button
                    key={preset.id}
                    onClick={() => {
                      setTheme(preset.id as ThemePreset);
                      setIsOpen(false);
                    }}
                    className={`w-full text-left p-2.5 rounded-xl border transition duration-150 flex items-center justify-between group ${
                      isActive
                        ? isLight
                          ? 'bg-indigo-50 border-indigo-400 shadow-sm'
                          : 'bg-indigo-500/15 border-indigo-500/50 shadow-sm'
                        : isLight
                          ? 'bg-slate-50 border-slate-200 hover:bg-slate-100 hover:border-slate-300'
                          : 'bg-slate-900/50 border-slate-800/60 hover:bg-slate-800/60 hover:border-slate-700'
                    }`}
                  >
                    <div className="flex items-center gap-3 min-w-0">
                      {/* Gradient color swatch circle */}
                      <div
                        className={`w-7 h-7 rounded-lg bg-gradient-to-tr ${preset.previewGradient} flex-shrink-0 shadow-md flex items-center justify-center text-white ring-2 ring-black/10`}
                      >
                        {isActive && <Check className="w-3.5 h-3.5 stroke-[3]" />}
                      </div>

                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <span
                            className={`text-xs font-bold tracking-tight block ${
                              isLight
                                ? isActive
                                  ? 'text-indigo-900 font-extrabold'
                                  : 'text-slate-800 group-hover:text-slate-900'
                                : isActive
                                  ? 'text-white font-extrabold'
                                  : 'text-slate-200 group-hover:text-white'
                            }`}
                          >
                            {preset.name}
                          </span>
                          {isActive && (
                            <span
                              className={`px-1.5 py-0.2 rounded-full text-[9px] font-mono uppercase font-semibold ${
                                isLight
                                  ? 'bg-indigo-100 text-indigo-700'
                                  : 'bg-indigo-500/20 text-indigo-300'
                              }`}
                            >
                              ACTIVE
                            </span>
                          )}
                        </div>
                        <p
                          className={`text-[10.5px] truncate leading-snug ${
                            isLight ? 'text-slate-500' : 'text-slate-400'
                          }`}
                        >
                          {preset.tagline}
                        </p>
                      </div>
                    </div>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
