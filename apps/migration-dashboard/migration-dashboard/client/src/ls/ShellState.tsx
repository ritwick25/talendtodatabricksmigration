import { createContext, useCallback, useContext, useRef, useState, type ReactNode } from 'react';

interface Toast {
  id: number;
  message: string;
  kind?: 'warn';
}

interface ShellCtx {
  drawerJobId: string | null;
  openDrawer: (pipelineId: string) => void;
  closeDrawer: () => void;
  toasts: Toast[];
  toast: (message: string, kind?: 'warn') => void;
}

const Ctx = createContext<ShellCtx | null>(null);

export function ShellStateProvider({ children }: { children: ReactNode }) {
  const [drawerJobId, setDrawerJobId] = useState<string | null>(null);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const idRef = useRef(0);

  const toast = useCallback((message: string, kind?: 'warn') => {
    const id = ++idRef.current;
    setToasts((t) => [...t.slice(-2), { id, message, kind }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 4200);
  }, []);

  return (
    <Ctx.Provider
      value={{
        drawerJobId,
        openDrawer: setDrawerJobId,
        closeDrawer: () => setDrawerJobId(null),
        toasts,
        toast,
      }}
    >
      {children}
    </Ctx.Provider>
  );
}

export function useShell() {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error('useShell must be used within ShellStateProvider');
  return ctx;
}

export function Toasts() {
  const { toasts } = useShell();
  return (
    <div className="toasts" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className={`toast ${t.kind ?? ''}`}>
          {t.message}
        </div>
      ))}
    </div>
  );
}
