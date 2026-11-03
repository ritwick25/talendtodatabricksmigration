import { useMemo } from 'react';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { Tile, Empty } from '../components';
import { useShell } from '../ShellState';
import { fmt, money, pct, hms, num, bool } from '../utils';
import type { QueueRow } from '../domain';

function useQueue() {
  const { data } = useAnalyticsQuery('queue_list', {});
  return data ?? [];
}

function BatchOutcomes({ rows }: { rows: QueueRow[] }) {
  const batches = useMemo(() => {
    const byBatch = new Map<string, QueueRow[]>();
    for (const r of rows) {
      if (!r.batch_id) continue;
      (byBatch.get(r.batch_id) ?? byBatch.set(r.batch_id, []).get(r.batch_id)!).push(r);
    }
    return [...byBatch.entries()]
      .map(([id, js]) => ({
        id,
        first: js.filter((j) => j.status === 'COMPLETED' && num(j.fix_attempts) === 0).length,
        fixed: js.filter((j) => j.status === 'COMPLETED' && (j.fix_attempts ?? 0) > 0).length,
        failed: js.filter((j) => j.status === 'FAILED').length,
        started: js.reduce((min, j) => (j.started_at && (!min || j.started_at < min) ? j.started_at : min), '' as string),
      }))
      .sort((a, b) => a.started.localeCompare(b.started));
  }, [rows]);

  if (!batches.length) return <Empty text="No batches have run yet." />;
  const max = Math.max(1, ...batches.map((b) => b.first + b.fixed + b.failed));

  return (
    <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', height: 180, overflowX: 'auto', paddingBottom: 4 }}>
      {batches.map((b, i) => {
        const total = b.first + b.fixed + b.failed || 1;
        const h = (total / max) * 150;
        return (
          <div key={b.id} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6, minWidth: 34 }}>
            <div
              style={{
                width: 28,
                height: h,
                borderRadius: 4,
                overflow: 'hidden',
                display: 'flex',
                flexDirection: 'column-reverse',
              }}
            >
              {b.first > 0 && <div style={{ background: 'var(--ok)', height: `${(b.first / total) * 100}%` }} />}
              {b.fixed > 0 && <div style={{ background: 'var(--ok-2)', height: `${(b.fixed / total) * 100}%` }} />}
              {b.failed > 0 && <div style={{ background: 'var(--bad)', height: `${(b.failed / total) * 100}%` }} />}
            </div>
            <span className="mono muted" style={{ fontSize: 10 }}>
              B{i + 1}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function LearningCurve({ rows }: { rows: QueueRow[] }) {
  const points = useMemo(() => {
    const byBatch = new Map<string, QueueRow[]>();
    for (const r of rows) {
      if (!r.batch_id) continue;
      (byBatch.get(r.batch_id) ?? byBatch.set(r.batch_id, []).get(r.batch_id)!).push(r);
    }
    return [...byBatch.entries()]
      .map(([id, js]) => {
        const done = js.filter((j) => j.status === 'COMPLETED' || j.status === 'FAILED');
        const first = js.filter((j) => j.status === 'COMPLETED' && num(j.fix_attempts) === 0).length;
        const started = js.reduce((min, j) => (j.started_at && (!min || j.started_at < min) ? j.started_at : min), '' as string);
        return { id, rate: done.length ? pct(first, done.length) : 0, started };
      })
      .sort((a, b) => a.started.localeCompare(b.started));
  }, [rows]);

  if (points.length < 2) return <Empty text="The curve appears after two batches." />;
  const W = 360,
    H = 160,
    pl = 30,
    pr = 14,
    pt = 14,
    pb = 20,
    pw = W - pl - pr,
    ph = H - pt - pb;
  const xy = points.map((p, i) => [pl + (pw * i) / (points.length - 1), pt + ph * (1 - p.rate / 100)] as const);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" role="img" aria-label="First-pass success rate by batch">
      {[0, 50, 100].map((v) => {
        const y = pt + ph * (1 - v / 100);
        return (
          <g key={v}>
            <line className="ln" x1={pl} x2={W - pr} y1={y} y2={y} />
            <text x={pl - 6} y={y + 3} textAnchor="end">
              {v}%
            </text>
          </g>
        );
      })}
      <polyline
        fill="none"
        style={{ stroke: 'var(--accent)' }}
        strokeWidth={2}
        strokeLinejoin="round"
        points={xy.map((p) => p.join(',')).join(' ')}
      />
      {xy.map(([x, y], i) => (
        <circle key={i} cx={x} cy={y} r={i === xy.length - 1 ? 5 : 3.5} style={{ fill: 'var(--surface)', stroke: 'var(--accent)' }} strokeWidth={2} />
      ))}
    </svg>
  );
}

function Donut({ parts, label }: { parts: Array<{ v: number; c: string }>; label: string }) {
  const tot = parts.reduce((a, p) => a + p.v, 0);
  if (!tot) return <Empty text="No pipelines yet." />;
  const R = 44,
    C = 2 * Math.PI * R;
  let off = 0;
  return (
    <svg viewBox="0 0 120 120" width={150} height={150} role="img" aria-label={label}>
      <circle cx={60} cy={60} r={R} fill="none" style={{ stroke: 'var(--surface-3)' }} strokeWidth={14} />
      {parts.map((p, i) => {
        if (!p.v) return null;
        const len = C * (p.v / tot);
        const el = (
          <circle
            key={i}
            cx={60}
            cy={60}
            r={R}
            fill="none"
            strokeWidth={14}
            style={{ stroke: p.c }}
            strokeDasharray={`${Math.max(0, len - 2)} ${C - Math.max(0, len - 2)}`}
            strokeDashoffset={-off}
            transform="rotate(-90 60 60)"
          />
        );
        off += len;
        return el;
      })}
      <text x={60} y={64} textAnchor="middle" className="big">
        {tot}
      </text>
      <text x={60} y={80} textAnchor="middle">
        {label}
      </text>
    </svg>
  );
}

export function OverviewPage() {
  const rows = useQueue();
  const { data: summary } = useAnalyticsQuery('queue_summary', {});
  const { data: control } = useAnalyticsQuery('control_status', {});
  const { data: improvements } = useAnalyticsQuery('improvement_log_list', {});
  const { data: logs } = useAnalyticsQuery('attempt_log_recent', {});
  const { openDrawer } = useShell();

  const byStatus = useMemo(() => {
    const m: Record<string, { cnt: number }> = {};
    for (const r of summary ?? []) m[r.status] = r;
    return m;
  }, [summary]);

  const done = num(byStatus.COMPLETED?.cnt);
  const failed = num(byStatus.FAILED?.cnt);
  const pending = num(byStatus.PENDING?.cnt);
  const inflight = num(byStatus.IN_PROGRESS?.cnt);
  const total = done + failed + pending + inflight;
  const fin = done + failed;
  const completedRows = rows.filter((r) => r.status === 'COMPLETED');
  const first = completedRows.filter((r) => num(r.fix_attempts) === 0).length;
  const avgFix = completedRows.length ? completedRows.reduce((a, r) => a + num(r.fix_attempts), 0) / completedRows.length : 0;
  const totalCost = rows.reduce((a, r) => a + num(r.cost), 0);
  const uncertain = completedRows.filter((r) => bool(r.java_uncertain)).length;
  const c = control?.[0];
  const merged = (improvements ?? []).filter((p) => bool(p.merged)).length;
  const closed = (improvements ?? []).filter((p) => !bool(p.merged)).length;

  const fixBuckets = [0, 1, 2].map((n) => completedRows.filter((r) => num(r.fix_attempts) === n).length);

  return (
    <>
      <section className="g g-kpi">
        <Tile label="Pipelines completed" value={<>{done}<small> / {total}</small></>} sub={`${failed} failed · ${pending} queued`} subCls={failed ? 'bad' : 'ok'} />
        <Tile label="First-pass rate" value={<>{pct(first, fin)}<small>%</small></>} sub={`${first} of ${fin} without a fix cycle`} />
        <Tile label="Average fix cycles" value={avgFix.toFixed(2)} sub={`limit ${c?.max_fix_cycles ?? '-'} per pipeline`} />
        <Tile label="Data validated" value="—" sub="validation not built yet" />
        <Tile label="Spend" value={money(totalCost)} sub={`${c?.global_daily_dbu_cap ?? '-'} DBU/day cap`} />
        <Tile label="Self-improvement" value={(improvements ?? []).length} sub={`${merged} merged · ${closed} closed`} />
      </section>

      <section className="g g-21">
        <div className="card">
          <div className="hd">
            <h3>Batch outcomes</h3>
            <span className="eyebrow">by batch_id</span>
          </div>
          <BatchOutcomes rows={rows} />
          <div className="legend">
            <span>
              <i style={{ background: 'var(--ok)' }} />
              First pass
            </span>
            <span>
              <i style={{ background: 'var(--ok-2)' }} />
              After a fix
            </span>
            <span>
              <i style={{ background: 'var(--bad)' }} />
              Failed
            </span>
          </div>
        </div>
        <div className="card">
          <div className="hd">
            <h3>First-pass rate by batch</h3>
            <span className="eyebrow">the learning curve</span>
          </div>
          <LearningCurve rows={rows} />
        </div>
      </section>

      <section className="g g-3">
        <div className="card">
          <div className="hd">
            <h3>Status</h3>
          </div>
          <div style={{ display: 'flex', gap: 18, alignItems: 'center', flexWrap: 'wrap' }}>
            <Donut
              parts={[
                { v: done, c: 'var(--ok)' },
                { v: inflight, c: 'var(--accent)' },
                { v: pending, c: 'var(--idle)' },
                { v: failed, c: 'var(--bad)' },
              ]}
              label="pipelines"
            />
            <div className="legend" style={{ flexDirection: 'column', margin: 0 }}>
              <span><i style={{ background: 'var(--ok)' }} />Completed <b className="mono">{done}</b></span>
              <span><i style={{ background: 'var(--accent)' }} />In flight <b className="mono">{inflight}</b></span>
              <span><i style={{ background: 'var(--idle)' }} />Queued <b className="mono">{pending}</b></span>
              <span><i style={{ background: 'var(--bad)' }} />Failed <b className="mono">{failed}</b></span>
            </div>
          </div>
        </div>
        <div className="card">
          <div className="hd">
            <h3>Every pipeline</h3>
            <span className="eyebrow">click to open</span>
          </div>
          <div className="waffle">
            {rows.length === 0 && <Empty text="No pipelines yet." />}
            {[...rows]
              .sort((a, b) => (a.complexity_score ?? 0) - (b.complexity_score ?? 0))
              .map((j) => {
                const cls =
                  j.status === 'COMPLETED'
                    ? (j.fix_attempts ?? 0) > 0
                      ? 'fix'
                      : 'ok'
                    : j.status === 'FAILED'
                      ? 'bad'
                      : j.status === 'IN_PROGRESS'
                        ? 'now'
                        : 'q';
                return (
                  <button
                    key={j.pipeline_id}
                    className={cls}
                    title={`${j.pipeline_id} · complexity ${j.complexity_score ?? '-'}`}
                    aria-label={j.pipeline_id}
                    onClick={() => openDrawer(j.pipeline_id)}
                  />
                );
              })}
          </div>
          <div className="legend">
            <span><i style={{ background: 'var(--ok)' }} />First pass</span>
            <span><i style={{ background: 'var(--ok-2)' }} />Fixed</span>
            <span><i style={{ background: 'var(--bad)' }} />Failed</span>
            <span><i style={{ background: 'var(--surface-3)' }} />Queued</span>
          </div>
        </div>
        <div className="card">
          <div className="hd">
            <h3>Fix cycles used</h3>
            <span className="eyebrow">completed only</span>
          </div>
          {[0, 1, 2].map((n) => (
            <div key={n} style={{ display: 'grid', gridTemplateColumns: '70px 1fr 26px', gap: 10, alignItems: 'center', margin: '9px 0' }}>
              <span className="muted mono" style={{ fontSize: 12 }}>
                {n} fix{n === 1 ? '' : 'es'}
              </span>
              <span className="bar ok">
                <i style={{ width: `${pct(fixBuckets[n], completedRows.length)}%` }} />
              </span>
              <b className="mono num" style={{ fontSize: 12 }}>
                {fixBuckets[n]}
              </b>
            </div>
          ))}
          <p className="hint" style={{ marginTop: 12 }}>
            Hard cap: {c?.max_fix_cycles ?? '-'} fix cycle(s), then the pipeline is marked failed.
          </p>
        </div>
      </section>

      <section className="card">
        <div className="hd">
          <h3>Recent activity</h3>
        </div>
        <div className="trace" style={{ height: 'auto', maxHeight: 260 }}>
          {(logs ?? []).length === 0 && <Empty text="Nothing yet." />}
          {(logs ?? []).slice(0, 25).map((l) => (
            <div key={l.attempt_id} className={`lg ${l.error_signature ? 'bad' : ''}`}>
              <span className="tt">{hms(l.ts)}</span>
              <span className="aa">{l.agent}</span>
              <span className="mm">
                <b>{l.pipeline_id}</b> {l.error_signature ?? 'ok'}
              </span>
            </div>
          ))}
        </div>
      </section>
      <p className="hint">
        {fmt(uncertain)} completed pipeline{uncertain === 1 ? '' : 's'} carry a best-effort Java block and should be checked first.
      </p>
    </>
  );
}
