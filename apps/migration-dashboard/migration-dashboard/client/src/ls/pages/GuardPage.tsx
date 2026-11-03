import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { PROTECTED_PATHS } from '../domain';

export function GuardPage() {
  const { data } = useAnalyticsQuery('control_status', {});
  const c = data?.[0];
  if (!c) return null;

  const limits: Array<[string, string]> = [
    ['Batch size', String(c.batch_size)],
    ['Fix cycles per pipeline', String(c.max_fix_cycles)],
    ['Halt at failures per batch', String(c.halt_threshold)],
    ['Per-job run timeout', `${c.per_job_run_timeout_min} min`],
    ['Daily DBU cap', c.global_daily_dbu_cap != null ? `${c.global_daily_dbu_cap} DBU` : '—'],
  ];

  return (
    <>
      <section className="g g-3">
        <div className="card">
          <div className="hd">
            <h3>Limits</h3>
            <span className="eyebrow">stored in the control table</span>
          </div>
          {limits.map(([a, b]) => (
            <div key={a} className="opt">
              <span>{a}</span>
              <b className="mono">{b}</b>
            </div>
          ))}
        </div>
        <div className="card">
          <div className="hd">
            <h3>Protected paths</h3>
            <span className="eyebrow">no agent write access</span>
          </div>
          {PROTECTED_PATHS.map((p) => (
            <div key={p} className="opt">
              <span className="mono" style={{ fontSize: 12.5 }}>
                {p}
              </span>
              <span className="chip">locked</span>
            </div>
          ))}
          <p className="hint" style={{ marginTop: 10 }}>
            Enforced in plain code by the improver agent (<span className="mono">PROTECTED_PREFIXES</span>), not left to
            the model.
          </p>
        </div>
        <div className="card">
          <div className="hd">
            <h3>Source and destination</h3>
            <span className="eyebrow">set on Sources</span>
          </div>
          <div className="opt">
            <span>Source volume</span>
            <b className="mono" style={{ fontSize: 12 }}>
              {c.source_path ?? '—'}
            </b>
          </div>
          <div className="opt">
            <span>Destination schema</span>
            <b className="mono" style={{ fontSize: 12 }}>
              {c.destination_path ?? '—'}
            </b>
          </div>
        </div>
      </section>
      <section className="card">
        <div className="hd">
          <h3>Rules the control plane enforces</h3>
        </div>
        <div className="g g-3" style={{ gap: 14 }}>
          {[
            ['Halt rule', `${c.halt_threshold} failed pipelines in one batch stop the whole process. It does not resume on its own.`],
            ['Fix cap', `Each pipeline gets ${c.max_fix_cycles} fix cycle(s). Every fix must change the code and use the real error.`],
            ['Infra errors', 'Cluster and quota errors are classified separately and do not consume a fix cycle.'],
            ['Kill switch', 'Stops new batches immediately from the topbar. In-flight work returns to the queue.'],
            ['No raw data', 'Agents see schemas, errors and at most a few masked rows -- never full datasets.'],
            ['Nothing deleted', "Talend jobs are never changed. Only this system's own generated dev jobs are touched."],
          ].map(([a, b]) => (
            <div key={a}>
              <b>{a}</b>
              <p className="hint" style={{ marginTop: 4 }}>
                {b}
              </p>
            </div>
          ))}
        </div>
      </section>
      <p className="hint">Limits above come straight from the control table -- nothing on this page is simulated.</p>
    </>
  );
}
