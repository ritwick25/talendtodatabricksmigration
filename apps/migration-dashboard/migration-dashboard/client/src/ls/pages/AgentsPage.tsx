import { useMemo, useState } from 'react';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { StatusPill, StageDots, Empty } from '../components';
import { useShell } from '../ShellState';
import { AG, AGN } from '../domain';
import { hms, money, num, bool, text } from '../utils';

export function AgentsPage() {
  const { data: control } = useAnalyticsQuery('control_status', {});
  const { data: queueData } = useAnalyticsQuery('queue_list', {});
  const { data: logData } = useAnalyticsQuery('attempt_log_recent', {});
  const { openDrawer } = useShell();
  const [agentFilter, setAgentFilter] = useState('all');

  const rows = queueData ?? [];
  const logs = logData ?? [];
  const c = control?.[0];

  const agentStats = useMemo(() => {
    const m = new Map<string, { calls: number; cost: number; last: string; lastAt: string }>();
    for (const l of logs) {
      const prev = m.get(l.agent) ?? { calls: 0, cost: 0, last: '', lastAt: '' };
      prev.calls += 1;
      prev.cost += num(l.cost);
      if (l.ts > prev.lastAt) {
        prev.lastAt = l.ts;
        prev.last = l.error_signature ? `ERROR: ${l.error_signature}` : text(l.details) || 'ok';
      }
      m.set(l.agent, prev);
    }
    return m;
  }, [logs]);

  const latestBatchId = useMemo(() => {
    const withBatch = rows.filter((r) => r.batch_id);
    if (!withBatch.length) return null;
    return withBatch.reduce((a, r) => ((r.started_at ?? '') > (a.started_at ?? '') ? r : a)).batch_id;
  }, [rows]);

  const lane = rows
    .filter((r) => r.batch_id === latestBatchId)
    .sort((a, b) => (a.complexity_score ?? 0) - (b.complexity_score ?? 0));

  const inflightInBatch = lane.filter((j) => j.status === 'IN_PROGRESS').length;
  const failedInBatch = lane.filter((j) => j.status === 'FAILED').length;

  const filteredLogs = (agentFilter === 'all' ? logs : logs.filter((l) => l.agent === agentFilter)).slice(0, 80);

  return (
    <>
      <section className="card">
        <div className="hd">
          <h3>Control plane</h3>
          <span className="eyebrow">plain code, never an LLM</span>
        </div>
        <div className="g g-kpi" style={{ gap: 18 }}>
          <div>
            <div className="eyebrow">Current batch</div>
            <div style={{ font: '400 30px var(--f-disp)' }}>{latestBatchId ? latestBatchId.slice(0, 8) : 'None yet'}</div>
            <div className="muted mono" style={{ fontSize: 12 }}>
              {lane.length} pipelines · {inflightInBatch} in flight
            </div>
          </div>
          <div>
            <div className="eyebrow">Halt threshold · failures stop all</div>
            <div className="halt" style={{ margin: '12px 0 6px' }}>
              {Array.from({ length: c?.halt_threshold ?? 3 }).map((_, i) => (
                <i key={i} className={i < failedInBatch ? 'on' : ''} />
              ))}
            </div>
            <div className="muted mono" style={{ fontSize: 12 }}>
              {failedInBatch} failed in this batch
            </div>
          </div>
          <div>
            <div className="eyebrow">Fix cycles per job</div>
            <div style={{ font: '400 30px var(--f-disp)' }}>{c?.max_fix_cycles ?? '-'} max</div>
          </div>
          <div>
            <div className="eyebrow">Kill switch</div>
            <div style={{ marginTop: 8 }}>
              <span className={`pill ${bool(c?.kill_switch) ? 'bad' : 'ok'}`}>
                <i />
                {bool(c?.kill_switch) ? 'Set' : 'Armed, not tripped'}
              </span>
            </div>
          </div>
        </div>
      </section>

      <section>
        <div className="ribbon">
          {AG.map((a, i) => {
            const on = (agentStats.get(a.k)?.calls ?? 0) > 0 && agentFilter === 'all';
            return (
              <span key={a.k} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                {i > 0 && <span className={`arrow ${on ? 'on' : ''}`} />}
                <span className={`rb ${on ? 'on' : ''}`}>{a.name}</span>
              </span>
            );
          })}
        </div>
      </section>

      <section className="agents">
        {AG.map((a) => {
          const s = agentStats.get(a.k);
          return (
            <div key={a.k} className="ag">
              <h4>
                {a.name}
                <span className="chip info">{a.kind}</span>
              </h4>
              <div className="role">{a.role}</div>
              <div className="st">{s ? `${s.calls} call${s.calls > 1 ? 's' : ''} recently` : 'No recent activity'}</div>
              <div className="last">{s?.last ?? 'Waiting for work.'}</div>
              <div className="met">
                <span>{s?.calls ?? 0} calls</span>
                <span>{money(s?.cost ?? 0)}</span>
              </div>
            </div>
          );
        })}
        <div className="ag">
          <h4>
            Deploy and run
            <span className="chip">Plain code</span>
          </h4>
          <div className="role">Bundle deploy, job run, error classification. Never an LLM call -- no cost or call count here.</div>
          <div className="st">Control plane</div>
          <div className="last">See the Run column in Pipelines for live deploy/run status.</div>
        </div>
      </section>

      <section className="g g-21">
        <div className="card">
          <div className="hd">
            <h3>Latest batch</h3>
            <span className="eyebrow">{AGN.ctl}</span>
          </div>
          <div className="lanes">
            {lane.length === 0 && <Empty text="No batch has run yet." />}
            {lane.map((j) => (
              <div key={j.pipeline_id} className="lane">
                <div className="t">
                  <button className="btn sm" style={{ border: 0, padding: 0, background: 'none', fontWeight: 500 }} onClick={() => openDrawer(j.pipeline_id)}>
                    {j.pipeline_id}
                  </button>
                  <div className="muted mono" style={{ fontSize: 11.5 }}>
                    complexity {j.complexity_score ?? '-'}
                    {j.fix_attempts ? ` · fix ${j.fix_attempts}/${c?.max_fix_cycles ?? '-'}` : ''}
                  </div>
                </div>
                <div className="sdw">
                  <StageDots job={j} />
                </div>
                <div className="x">
                  <StatusPill status={j.status} />
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="card">
          <div className="hd">
            <h3>Live trace</h3>
            <select className="inp" style={{ flex: '0 0 auto', height: 30, fontSize: 12.5, padding: '0 8px' }} value={agentFilter} onChange={(e) => setAgentFilter(e.target.value)}>
              <option value="all">All sources</option>
              {AG.map((a) => (
                <option key={a.k} value={a.k}>
                  {a.name}
                </option>
              ))}
              <option value="MANUAL_FIX">Manual fix (human)</option>
            </select>
          </div>
          <div className="trace">
            {filteredLogs.length === 0 && <Empty text="Nothing yet." />}
            {filteredLogs.map((l) => (
              <div key={l.attempt_id} className={`lg ${l.error_signature ? 'bad' : ''}`}>
                <span className="tt">{hms(l.ts)}</span>
                <span className="aa">{AGN[l.agent] ?? l.agent}</span>
                <span className="mm">
                  <b>{l.pipeline_id}</b> {l.error_signature ?? 'ok'}
                </span>
              </div>
            ))}
          </div>
        </div>
      </section>
    </>
  );
}
