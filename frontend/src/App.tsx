// frontend/src/App.tsx
import React, { useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AuthProvider, useAuth } from './context/AuthContext';
import { ThemeProvider } from './context/ThemeContext';
import { LoginPage } from './components/LoginPage';
import { Sidebar, type NavTab } from './components/layout/Sidebar';
import { Topbar } from './components/layout/Topbar';
import { DashboardView } from './views/DashboardView';
import { PipelinesView } from './views/PipelinesView';
import { DiscoveryView } from './views/DiscoveryView';
import { RunsView } from './views/RunsView';
import { HealthView } from './views/HealthView';
import { IntegrationsView } from './views/IntegrationsView';
import { UsersView } from './views/UsersView';

import { Loader2, Database } from 'lucide-react';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10000,
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

const MainLayout: React.FC = () => {
  const { isAuthenticated, isLoading } = useAuth();
  const [currentTab, setCurrentTab] = useState<NavTab>('dashboard');
  const [isTriggerInDashboard, setIsTriggerInDashboard] = useState<boolean>(false);
  const [focusedRunId, setFocusedRunId] = useState<string | null>(null);

  const handleOpenTrigger = () => {
    setCurrentTab('dashboard');
    setIsTriggerInDashboard(true);
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-[#090d16] flex flex-col items-center justify-center gap-4">
        <div className="w-12 h-12 rounded-2xl bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
          <Database className="w-6 h-6 animate-pulse" />
        </div>
        <div className="flex items-center gap-2 text-slate-400 text-sm">
          <Loader2 className="w-4 h-4 animate-spin text-indigo-400" />
          <span>Validating session credentials...</span>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <LoginPage />;
  }

  return (
    <div className="flex h-screen bg-[#090d16] text-slate-100 overflow-hidden">
      {/* Sleek Sidebar */}
      <Sidebar currentTab={currentTab} onSelectTab={setCurrentTab} />

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <Topbar onOpenTriggerModal={handleOpenTrigger} />

        <main className="flex-1 overflow-y-auto">
          {currentTab === 'dashboard' && (
            <DashboardView
              focusedRunId={focusedRunId}
              isTriggerOpen={isTriggerInDashboard}
              onToggleTrigger={setIsTriggerInDashboard}
              onNavigateToRuns={(runId) => {
                if (runId) setFocusedRunId(runId);
                setCurrentTab('runs');
              }}
              onNavigateToIntegrations={() => setCurrentTab('integrations')}
            />
          )}
          {currentTab === 'pipelines' && <PipelinesView />}
          {currentTab === 'discovery' && <DiscoveryView />}
          {currentTab === 'runs' && (
            <RunsView
              initialSelectedRunId={focusedRunId}
              onOpenTriggerModal={handleOpenTrigger}
            />
          )}
          {currentTab === 'integrations' && (
            <IntegrationsView onNavigateToPipelines={(_tag) => setCurrentTab('pipelines')} />
          )}
          {currentTab === 'health' && <HealthView />}
          {currentTab === 'users' && <UsersView />}
        </main>
      </div>
    </div>
  );
};

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <AuthProvider>
          <MainLayout />
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
