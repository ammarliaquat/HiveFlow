// frontend/src/components/common/HiveFlowLogo.tsx
import React from 'react';
import { useTheme } from '../../context/ThemeContext';

export interface HiveFlowLogoProps {
  /** 'mark' for icon only, 'horizontal' for logo + side text, 'stacked' for logo over text */
  variant?: 'mark' | 'horizontal' | 'stacked';
  /** Preset sizes or explicit numeric pixel size */
  size?: 'xs' | 'sm' | 'md' | 'lg' | 'xl' | number;
  /** Whether to show outer atmospheric neon glow */
  glow?: boolean;
  /** Custom class for wrapper element */
  className?: string;
  /** Subtitle text, defaults to 'LAKEHOUSE PLATFORM' */
  subtitle?: string;
  /** Company label, defaults to 'by Hiveworx' */
  company?: string;
}

const sizeMap = {
  xs: { icon: 26, text: 'text-xs', sub: 'text-[9px]' },
  sm: { icon: 34, text: 'text-sm font-bold', sub: 'text-[10px]' },
  md: { icon: 40, text: 'text-base font-extrabold', sub: 'text-[10.5px]' },
  lg: { icon: 50, text: 'text-xl font-extrabold', sub: 'text-xs' },
  xl: { icon: 64, text: 'text-2xl font-extrabold', sub: 'text-sm' },
};

export const HiveFlowMark: React.FC<{ size?: number; glow?: boolean; className?: string }> = ({
  size = 40,
  glow = true,
  className = '',
}) => {
  const uniqueId = React.useId().replace(/:/g, '');
  const gradPrimary = `hfp-${uniqueId}`;
  const gradAccent = `hfa-${uniqueId}`;
  const gradGlow = `hfg-${uniqueId}`;
  const borderGrad = `hfb-${uniqueId}`;
  const glowFilter = `hff-${uniqueId}`;

  // Read theme colors safely
  let colors = {
    primary: ['#818CF8', '#A855F7', '#E879F9'],
    accent: ['#00F0FF', '#38BDF8', '#6366F1'],
    glow: ['#00F0FF', '#C084FC'],
  };
  let isLight = false;

  try {
    // eslint-disable-next-line react-hooks/rules-of-hooks
    const themeContext = useTheme();
    if (themeContext?.activeConfig?.logoGradients) {
      colors = themeContext.activeConfig.logoGradients;
    }
    isLight = themeContext?.mode === 'light';
  } catch {
    // Used outside ThemeProvider
  }

  return (
    <div
      className={`relative inline-flex items-center justify-center flex-shrink-0 transition-all duration-300 hover:scale-105 ${className}`}
      style={{ width: size, height: size }}
    >
      {/* Outer Glow Halo */}
      {glow && (
        <div
          className="absolute inset-0 rounded-2xl blur-md pointer-events-none transition-all duration-500"
          style={{
            background: `radial-gradient(circle, ${colors.primary[0]}40 0%, ${colors.accent[0]}20 70%, transparent 100%)`,
            transform: 'scale(1.2)',
          }}
        />
      )}

      <svg
        viewBox="0 0 100 100"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className="w-full h-full relative z-10 select-none drop-shadow-md"
      >
        <defs>
          {/* Badge Background Gradient */}
          <linearGradient id={borderGrad} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor={colors.accent[0]} stopOpacity={isLight ? 0.6 : 0.8} />
            <stop offset="50%" stopColor={colors.primary[0]} stopOpacity={0.5} />
            <stop offset="100%" stopColor={colors.primary[2]} stopOpacity={isLight ? 0.4 : 0.8} />
          </linearGradient>

          <linearGradient id={gradPrimary} x1="0%" y1="100%" x2="100%" y2="0%">
            <stop offset="0%" stopColor={colors.primary[0]} />
            <stop offset="50%" stopColor={colors.primary[1]} />
            <stop offset="100%" stopColor={colors.primary[2]} />
          </linearGradient>

          <linearGradient id={gradAccent} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor={colors.accent[0]} />
            <stop offset="50%" stopColor={colors.accent[1]} />
            <stop offset="100%" stopColor={colors.accent[2]} />
          </linearGradient>

          <linearGradient id={gradGlow} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor={colors.glow[0]} />
            <stop offset="100%" stopColor={colors.glow[1]} />
          </linearGradient>

          {glow && (
            <filter id={glowFilter} x="-20%" y="-20%" width="140%" height="140%">
              <feGaussianBlur stdDeviation="2.5" result="blur" />
              <feComposite in="SourceGraphic" in2="blur" operator="over" />
            </filter>
          )}
        </defs>

        {/* Rounded Glassmorphic Badge */}
        <rect
          x="4"
          y="4"
          width="92"
          height="92"
          rx="24"
          fill={isLight ? '#FFFFFF' : '#0D1322'}
          stroke={`url(#${borderGrad})`}
          strokeWidth="2.5"
        />

        {/* Ambient Core Glow inside Badge */}
        {glow && (
          <circle
            cx="50"
            cy="50"
            r="28"
            fill={colors.primary[0]}
            opacity={isLight ? 0.15 : 0.3}
            filter={`url(#${glowFilter})`}
          />
        )}

        {/* Outer Hexagonal Flow Facets */}
        {/* Right / Upper Flow */}
        <path
          d="M50 16 L78 32 V64 L50 48 Z"
          fill={`url(#${gradAccent})`}
          opacity="0.95"
        />

        {/* Left / Lower Flow */}
        <path
          d="M50 84 L22 68 V36 L50 52 Z"
          fill={`url(#${gradPrimary})`}
          opacity="0.95"
        />

        {/* Continuous Kinetic Ribbon (S-curve stream) */}
        <path
          d="M26 36 C35 22, 65 22, 74 36 C82 48, 70 58, 50 58 C30 58, 18 68, 26 80 C35 94, 65 94, 74 80"
          stroke={`url(#${gradGlow})`}
          strokeWidth="6"
          strokeLinecap="round"
          strokeLinejoin="round"
          fill="none"
          filter={glow ? `url(#${glowFilter})` : undefined}
        />

        {/* Central Spark Nexus / Lakehouse Origin */}
        <circle cx="50" cy="50" r="5.5" fill={isLight ? '#0F172A' : '#FFFFFF'} />
        <circle cx="50" cy="50" r="8.5" stroke={colors.accent[0]} strokeWidth="1.75" fill="none" opacity="0.9" />

        {/* High-Tech Node Vertices */}
        <circle cx="50" cy="16" r="3" fill={colors.accent[0]} />
        <circle cx="78" cy="32" r="3" fill={colors.accent[1]} />
        <circle cx="78" cy="64" r="3" fill={colors.primary[1]} />
        <circle cx="50" cy="84" r="3" fill={colors.primary[0]} />
        <circle cx="22" cy="68" r="3" fill={colors.accent[0]} />
        <circle cx="22" cy="36" r="3" fill={colors.accent[1]} />
      </svg>
    </div>
  );
};

export const HiveFlowLogo: React.FC<HiveFlowLogoProps> = ({
  variant = 'horizontal',
  size = 'md',
  glow = true,
  className = '',
  subtitle = 'LAKEHOUSE PLATFORM',
  company = 'by Hiveworx',
}) => {
  const pixelSize = typeof size === 'number' ? size : sizeMap[size].icon;
  const textPreset = typeof size === 'number' ? sizeMap.md : sizeMap[size];

  let activeThemeId = 'cyber';
  let isLight = false;
  try {
    // eslint-disable-next-line react-hooks/rules-of-hooks
    const themeContext = useTheme();
    activeThemeId = themeContext?.theme || 'cyber';
    isLight = themeContext?.mode === 'light';
  } catch {
    // fallback
  }

  // Accent gradient class for "FLOW" wordmark based on theme
  const flowGradients: Record<string, string> = {
    cyber: 'from-cyan-400 via-sky-400 to-indigo-400',
    emerald: 'from-emerald-400 via-teal-400 to-cyan-400',
    amber: 'from-amber-400 via-yellow-400 to-orange-400',
    sunset: 'from-pink-400 via-rose-400 to-purple-400',
  };

  const subtitleColors: Record<string, string> = {
    cyber: isLight ? 'text-indigo-600' : 'text-cyan-400/90',
    emerald: isLight ? 'text-emerald-700' : 'text-emerald-400/90',
    amber: isLight ? 'text-amber-700' : 'text-amber-400/90',
    sunset: isLight ? 'text-rose-600' : 'text-pink-400/90',
  };

  const flowGradient = flowGradients[activeThemeId] || flowGradients.cyber;
  const subColor = subtitleColors[activeThemeId] || subtitleColors.cyber;

  if (variant === 'mark') {
    return <HiveFlowMark size={pixelSize} glow={glow} className={className} />;
  }

  if (variant === 'stacked') {
    return (
      <div className={`flex flex-col items-center text-center gap-3 select-none ${className}`}>
        <HiveFlowMark size={pixelSize} glow={glow} />
        <div>
          <div className="flex items-center justify-center gap-1.5">
            <span className={`tracking-tight ${isLight ? 'text-slate-900' : 'text-white'} font-sans ${textPreset.text}`}>
              HIVE<span className={`text-transparent bg-clip-text bg-gradient-to-r ${flowGradient}`}>FLOW</span>
            </span>
          </div>
          <span className={`block uppercase font-mono font-semibold tracking-widest mt-1 ${subColor} ${textPreset.sub}`}>
            {subtitle}
          </span>
          {company && (
            <span className={`inline-block mt-1 px-2 py-0.5 rounded-full text-[10px] font-medium ${
              isLight ? 'bg-slate-100 text-slate-600 border border-slate-200' : 'bg-slate-800/90 text-slate-300 border border-slate-700/80'
            }`}>
              {company}
            </span>
          )}
        </div>
      </div>
    );
  }

  // Horizontal variant (Default for Sidebar, Topbar, Navbar)
  return (
    <div className={`flex items-center gap-3 select-none ${className}`}>
      <HiveFlowMark size={pixelSize} glow={glow} />
      <div className="flex flex-col min-w-0">
        <div className="flex items-center gap-2">
          <span className={`tracking-tight ${isLight ? 'text-slate-900' : 'text-white'} font-sans leading-none ${textPreset.text}`}>
            HIVE<span className={`text-transparent bg-clip-text bg-gradient-to-r ${flowGradient}`}>FLOW</span>
          </span>
          {company && (
            <span className={`px-1.5 py-0.5 rounded-md text-[9px] font-bold uppercase tracking-wider leading-none ${
              isLight ? 'bg-indigo-50 text-indigo-700 border border-indigo-200' : 'bg-indigo-500/15 text-indigo-300 border border-indigo-500/30'
            }`}>
              {company.replace(/^by\s+/i, '')}
            </span>
          )}
        </div>
        <span className={`uppercase font-mono font-semibold tracking-wider mt-1 leading-none ${subColor} ${textPreset.sub}`}>
          {subtitle}
        </span>
      </div>
    </div>
  );
};
