// frontend/src/components/LoginPage.tsx
import React, { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { ShieldCheck, KeyRound, AlertCircle, Loader2 } from 'lucide-react';
import { HiveFlowLogo } from './common/HiveFlowLogo';

export const LoginPage: React.FC = () => {
  const { login, error, clearError } = useAuth();
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!username || !password) return;
    setIsSubmitting(true);
    clearError();
    try {
      await login(username, password);
    } catch {
      // Error handled in AuthContext
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#0b0f19] flex items-center justify-center p-4 relative overflow-hidden font-sans">
      {/* Background glow effects */}
      <div className="absolute top-1/4 left-1/3 w-96 h-96 bg-indigo-500/10 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute bottom-1/4 right-1/3 w-96 h-96 bg-cyan-500/10 rounded-full blur-3xl pointer-events-none" />

      <div className="w-full max-w-md relative z-10">
        <div className="bg-[#111827]/95 backdrop-blur-xl border border-slate-800/80 rounded-2xl shadow-2xl p-8">
          {/* Logo & Header */}
          <div className="flex flex-col items-center text-center mb-8">
            <HiveFlowLogo variant="stacked" size={56} subtitle="Governed Lakehouse Platform" company="by Hiveworx" />
          </div>

          {error && (
            <div className="mb-6 p-4 rounded-xl bg-rose-500/10 border border-rose-500/20 flex items-start gap-3 text-rose-300 text-sm animate-fadeIn">
              <AlertCircle className="w-5 h-5 flex-shrink-0 mt-0.5 text-rose-400" />
              <div>
                <p className="font-medium text-rose-200">Authentication Failed</p>
                <p className="text-xs text-rose-300/80 mt-0.5">{error}</p>
              </div>
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-5">
            <div>
              <label className="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-2">
                Username
              </label>
              <div className="relative">
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  required
                  placeholder="e.g. admin"
                  className="w-full bg-[#1e293b]/60 border border-slate-700/80 focus:border-indigo-500 rounded-xl px-4 py-3 text-white placeholder-slate-500 text-sm outline-none transition duration-150 focus:ring-2 focus:ring-indigo-500/20"
                />
              </div>
            </div>

            <div>
              <label className="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-2">
                Password
              </label>
              <div className="relative">
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  placeholder="Enter secure password"
                  className="w-full bg-[#1e293b]/60 border border-slate-700/80 focus:border-indigo-500 rounded-xl px-4 py-3 text-white placeholder-slate-500 text-sm outline-none transition duration-150 focus:ring-2 focus:ring-indigo-500/20"
                />
              </div>
            </div>

            <button
              type="submit"
              disabled={isSubmitting || !username || !password}
              className="w-full mt-2 bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white font-medium py-3 px-4 rounded-xl shadow-lg shadow-indigo-600/25 transition duration-150 flex items-center justify-center gap-2 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isSubmitting ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  <span>Verifying Credentials...</span>
                </>
              ) : (
                <>
                  <KeyRound className="w-4 h-4" />
                  <span>Sign In to HiveFlow Console</span>
                </>
              )}
            </button>
          </form>

          {/* Footer note */}
          <div className="mt-8 pt-6 border-t border-slate-800/80 flex items-center justify-center gap-2 text-xs text-slate-500">
            <ShieldCheck className="w-4 h-4 text-emerald-400" />
            <span>Encrypted HttpOnly Session Auth · RBAC Protected</span>
          </div>
        </div>
      </div>
    </div>
  );
};
