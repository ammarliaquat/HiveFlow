// frontend/src/context/ThemeContext.tsx
import React, { createContext, useContext, useEffect, useState } from 'react';

export type ColorMode = 'dark' | 'light';
export type ThemePreset = 'cyber' | 'emerald' | 'amber' | 'sunset';

export interface ThemeConfig {
  id: ThemePreset;
  name: string;
  tagline: string;
  primaryColor: string;
  accentColor: string;
  badgeClass: string;
  previewGradient: string;
  logoGradients: {
    primary: [string, string, string];
    accent: [string, string, string];
    glow: [string, string];
  };
}

export const THEME_PRESETS: ThemeConfig[] = [
  {
    id: 'cyber',
    name: 'Cyber Indigo',
    tagline: 'Signature electric indigo & cyber cyan data currents',
    primaryColor: '#6366f1',
    accentColor: '#06b6d4',
    badgeClass: 'bg-indigo-500/15 text-indigo-300 border-indigo-500/30',
    previewGradient: 'from-indigo-500 via-indigo-600 to-cyan-400',
    logoGradients: {
      primary: ['#818CF8', '#A855F7', '#E879F9'],
      accent: ['#00F0FF', '#38BDF8', '#6366F1'],
      glow: ['#00F0FF', '#C084FC'],
    },
  },
  {
    id: 'emerald',
    name: 'Emerald Matrix',
    tagline: 'Radiant jade lakehouse & live pipeline health',
    primaryColor: '#10b981',
    accentColor: '#14b8a6',
    badgeClass: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30',
    previewGradient: 'from-emerald-500 via-teal-600 to-cyan-400',
    logoGradients: {
      primary: ['#34D399', '#10B981', '#059669'],
      accent: ['#2DD4BF', '#06B6D4', '#0D9488'],
      glow: ['#34D399', '#2DD4BF'],
    },
  },
  {
    id: 'amber',
    name: 'Medallion Gold',
    tagline: 'Prestigious honey amber & certified gold tier',
    primaryColor: '#f59e0b',
    accentColor: '#fb923c',
    badgeClass: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
    previewGradient: 'from-amber-500 via-amber-600 to-orange-400',
    logoGradients: {
      primary: ['#FBBF24', '#F59E0B', '#D97706'],
      accent: ['#FDE047', '#FB923C', '#EA580C'],
      glow: ['#FDE047', '#F97316'],
    },
  },
  {
    id: 'sunset',
    name: 'Neon Sunset',
    tagline: 'Electric rose & violet hyper-contrast night-ops',
    primaryColor: '#ec4899',
    accentColor: '#8b5cf6',
    badgeClass: 'bg-pink-500/15 text-pink-300 border-pink-500/30',
    previewGradient: 'from-pink-500 via-fuchsia-600 to-indigo-400',
    logoGradients: {
      primary: ['#F472B6', '#EC4899', '#BE185D'],
      accent: ['#A78BFA', '#8B5CF6', '#6366F1'],
      glow: ['#F472B6', '#A78BFA'],
    },
  },
];

interface ThemeContextType {
  mode: ColorMode;
  theme: ThemePreset;
  activeConfig: ThemeConfig;
  setMode: (mode: ColorMode) => void;
  toggleMode: () => void;
  setTheme: (theme: ThemePreset) => void;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

const MODE_STORAGE_KEY = 'hiveflow_color_mode';
const THEME_STORAGE_KEY = 'hiveflow_theme_preset';

export const ThemeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [mode, setModeState] = useState<ColorMode>(() => {
    try {
      const saved = localStorage.getItem(MODE_STORAGE_KEY);
      if (saved === 'dark' || saved === 'light') return saved;
      return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
    } catch {
      return 'dark';
    }
  });

  const [theme, setThemeState] = useState<ThemePreset>(() => {
    try {
      const saved = localStorage.getItem(THEME_STORAGE_KEY);
      if (saved && THEME_PRESETS.some((t) => t.id === saved)) {
        return saved as ThemePreset;
      }
      return 'cyber';
    } catch {
      return 'cyber';
    }
  });

  const activeConfig = THEME_PRESETS.find((t) => t.id === theme) || THEME_PRESETS[0];

  useEffect(() => {
    const root = document.documentElement;

    // Apply color mode (dark/light)
    if (mode === 'dark') {
      root.classList.add('dark');
      root.classList.remove('light');
      root.style.colorScheme = 'dark';
    } else {
      root.classList.add('light');
      root.classList.remove('dark');
      root.style.colorScheme = 'light';
    }

    // Apply theme preset data-attribute
    root.setAttribute('data-theme', theme);

    try {
      localStorage.setItem(MODE_STORAGE_KEY, mode);
      localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // storage unavailable
    }
  }, [mode, theme]);

  const setMode = (newMode: ColorMode) => {
    setModeState(newMode);
  };

  const toggleMode = () => {
    setModeState((prev) => (prev === 'dark' ? 'light' : 'dark'));
  };

  const setTheme = (newTheme: ThemePreset) => {
    setThemeState(newTheme);
  };

  return (
    <ThemeContext.Provider
      value={{
        mode,
        theme,
        activeConfig,
        setMode,
        toggleMode,
        setTheme,
      }}
    >
      {children}
    </ThemeContext.Provider>
  );
};

export const useTheme = (): ThemeContextType => {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
};
