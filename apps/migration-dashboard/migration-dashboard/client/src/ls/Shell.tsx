import { useState } from 'react';
import { NavLink, Outlet, useLocation } from 'react-router';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { Icon } from './icons';
import { NAV, TITLES } from './domain';
import { num, bool } from './utils';
import { ShellStateProvider, Toasts, useShell } from './ShellState';
import { JobDrawer } from './JobDrawer';
import { runAppJob, updateControl } from '../lib/api';

function toggleTheme() {
  const root = document.documentElement;
  const dark = root.dataset.theme ? root.dataset.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
  root.dataset.theme = dark ? 'light' : 'dark';
  try {
    localStorage.setItem('ls-theme', root.dataset.theme);
  } catch {
    // private window / blocked storage -- theme just won't persist
  }
}

function Sidebar() {
  const { pathname } = useLocation();
  const view = pathname.replace(/^\//, '') || 'overview';
  const { data: improvements } = useAnalyticsQuery('improvement_log_list', {});
  const { data: queueSummary } = useAnalyticsQuery('queue_summary', {});
  const total = (queueSummary ?? []).reduce((a, r) => a + num(r.cnt), 0);

  const badges: Record<string, string> = {
    pipelines: total ? String(total) : '',
    improve: improvements?.length ? String(improvements.length) : '',
  };

  return (
    <aside className="side">
      <div className="brand">
        <span className="logo">
          <Icon name="bolt" size={17} />
        </span>
        <b>Lakeshift</b>
      </div>
      <nav className="nav" aria-label="Sections">
        {NAV.map(([k, label, icon]) => (
          <NavLink key={k} to={`/${k === 'overview' ? '' : k}`} aria-current={view === k ? 'page' : undefined}>
            <Icon name={icon} />
            <span>{label}</span>
            {badges[k] && <span className="bd">{badges[k]}</span>}
          </NavLink>
        ))}
      </nav>
      <div className="side-foot">
        <b>Real workspace</b>
        <span>Every number on these screens comes from the live control-plane tables -- nothing here is simulated.</span>
      </div>
    </aside>
  );
}

function Banner() {
  const { data } = useAnalyticsQuery('control_status', {});
  const { toast } = useShell();
  const control = data?.[0];
  if (!control) return null;

  const clear = async () => {
    try {
      await updateControl({ halted: false, halt_reason: null, kill_switch: false });
      toast('Halt cleared. Resuming the queue.');
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    }
  };

  if (bool(control.halted)) {
    return (
      <div className="banner bad">
        <Icon name="stop" size={18} />
        <span>
          <b>Halted.</b> {control.halt_reason ?? 'The halt rule tripped.'} The system does not resume on its own.
        </span>
        <button className="btn sm" onClick={clear}>
          Clear halt and resume
        </button>
      </div>
    );
  }
  if (bool(control.kill_switch)) {
    return (
      <div className="banner bad">
        <Icon name="stop" size={18} />
        <span>
          <b>Kill switch set.</b> No new batches will start until it is cleared.
        </span>
        <button className="btn sm" onClick={clear}>
          Clear and resume
        </button>
      </div>
    );
  }
  return null;
}

function Topbar() {
  const { pathname } = useLocation();
  const view = pathname.replace(/^\//, '') || 'overview';
  const [t, sub] = TITLES[view] ?? TITLES.overview;
  const { data: control } = useAnalyticsQuery('control_status', {});
  const { data: queueSummary } = useAnalyticsQuery('queue_summary', {});
  const { toast } = useShell();
  const [busy, setBusy] = useState(false);

  const c = control?.[0];
  const pending = num((queueSummary ?? []).find((r) => r.status === 'PENDING')?.cnt);
  const inflight = num((queueSummary ?? []).find((r) => r.status === 'IN_PROGRESS')?.cnt);

  let runLabel = 'Idle';
  let runCls = '';
  if (bool(c?.kill_switch)) {
    runLabel = 'Kill switch';
    runCls = 'bad';
  } else if (bool(c?.halted)) {
    runLabel = 'Halted';
    runCls = 'bad';
  } else if (inflight > 0) {
    runLabel = 'Running';
    runCls = 'info live';
  }

  const runNext = async () => {
    if (!c || pending === 0) return;
    setBusy(true);
    try {
      const result = await runAppJob({ pipelineIds: '', batchSize: String(c.batch_size), maxBatches: '1' });
      toast(`Triggered run ${result.runId} for up to ${c.batch_size} pending job(s).`);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setBusy(false);
    }
  };

  const killSwitch = async () => {
    setBusy(true);
    try {
      await updateControl({ kill_switch: true });
      toast('Kill switch set. In-flight work returns to the queue.', 'warn');
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setBusy(false);
    }
  };

  return (
    <header className="top">
      <div>
        <h1>{t}</h1>
        <div className="sub">{sub}</div>
      </div>
      <div className="ctl">
        <span className={`pill ${runCls}`}>
          <i />
          {runLabel}
        </span>
        <button className="btn primary" disabled={busy || !c || pending === 0} onClick={runNext}>
          <Icon name="play" size={16} />
          {pending > 0 ? `Run next ${c?.batch_size ?? ''} queued · ${pending} pending` : 'Nothing queued'}
        </button>
        <button className="btn danger icon" disabled={busy || bool(c?.kill_switch)} onClick={killSwitch} title="Kill switch" aria-label="Kill switch">
          <Icon name="stop" size={16} />
        </button>
        <button className="btn icon" onClick={toggleTheme} title="Toggle theme" aria-label="Toggle theme">
          <Icon name="contrast" size={16} />
        </button>
      </div>
    </header>
  );
}

function ShellLayout() {
  return (
    <div className="ls">
      <div className="app">
        <Sidebar />
        <div className="main">
          <Banner />
          <Topbar />
          <Outlet />
        </div>
      </div>
      <JobDrawer />
      <Toasts />
    </div>
  );
}

export function Shell() {
  return (
    <ShellStateProvider>
      <ShellLayout />
    </ShellStateProvider>
  );
}
