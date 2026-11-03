import { useMemo, useState } from 'react';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { StatusPill, StageDots, ComplexityBar, Empty } from '../components';
import { useShell } from '../ShellState';
import { money, bool } from '../utils';
import type { QueueRow } from '../domain';

const TABS: Array<[string, string]> = [
  ['all', 'All'],
  ['queued', 'Queued'],
  ['inflight', 'In flight'],
  ['done', 'Completed'],
  ['failed', 'Failed'],
  ['review', 'Needs review'],
];

export function PipelinesPage() {
  const { data } = useAnalyticsQuery('queue_list', {});
  const rows = data ?? [];
  const { openDrawer } = useShell();
  const [filter, setFilter] = useState('all');
  const [q, setQ] = useState('');

  const flt: Record<string, (j: QueueRow) => boolean> = {
    all: () => true,
    queued: (j) => j.status === 'PENDING',
    inflight: (j) => j.status === 'IN_PROGRESS',
    done: (j) => j.status === 'COMPLETED',
    failed: (j) => j.status === 'FAILED',
    review: (j) => j.status === 'COMPLETED' && (bool(j.java_uncertain) || bool(j.spec_uncertain)),
  };

  const filtered = useMemo(() => {
    const ql = q.toLowerCase();
    return rows
      .filter((j) => flt[filter](j) && (!ql || j.pipeline_id.toLowerCase().includes(ql)))
      .sort((a, b) => (a.complexity_score ?? 0) - (b.complexity_score ?? 0));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows, filter, q]);

  return (
    <section className="card">
      <div className="hd">
        <div className="seg" role="group" aria-label="Filter">
          {TABS.map(([k, l]) => (
            <button key={k} aria-pressed={filter === k} onClick={() => setFilter(k)}>
              {l}
            </button>
          ))}
        </div>
        <input
          className="inp"
          style={{ flex: '0 1 240px', height: 32 }}
          placeholder="Search pipelines"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          aria-label="Search pipelines"
        />
      </div>
      <div className="tw">
        <table>
          <thead>
            <tr>
              <th>Pipeline</th>
              <th>Status</th>
              <th>Stage</th>
              <th className="r">Batch</th>
              <th className="r">Fix</th>
              <th>Complexity</th>
              <th>Java</th>
              <th>Validation</th>
              <th className="r">Cost</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 && (
              <tr>
                <td colSpan={9}>
                  <Empty text="No pipelines match." />
                </td>
              </tr>
            )}
            {filtered.map((j) => (
              <tr key={j.pipeline_id} className="click" onClick={() => openDrawer(j.pipeline_id)}>
                <td className="nm">
                  {j.pipeline_id}
                  <small>{j.source ?? 'Volume'}</small>
                </td>
                <td>
                  <StatusPill status={j.status} />
                </td>
                <td>
                  <StageDots job={j} />
                </td>
                <td className="r num mono">{j.batch_id ? j.batch_id.slice(0, 8) : '-'}</td>
                <td className="r num mono">{j.fix_attempts ?? 0}</td>
                <td>
                  <ComplexityBar value={j.complexity_score} />
                </td>
                <td>
                  {bool(j.java_uncertain) && <span className="chip warn">uncertain</span>}
                  {bool(j.spec_uncertain) && <span className="chip warn">spec</span>}
                  {!bool(j.java_uncertain) && !bool(j.spec_uncertain) && <span className="muted">-</span>}
                </td>
                <td>
                  <span className="muted">not run</span>
                </td>
                <td className="r num mono">{money(j.cost)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
