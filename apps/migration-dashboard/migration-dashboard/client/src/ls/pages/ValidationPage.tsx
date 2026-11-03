import { useMemo, useState } from 'react';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { Tile, Empty } from '../components';
import { Icon } from '../icons';
import { useShell } from '../ShellState';
import { runValidationJob } from '../../lib/api';

export function ValidationPage() {
  const { data } = useAnalyticsQuery('queue_list', {});
  const { data: validationData } = useAnalyticsQuery('validation_log_list', {});
  const { toast } = useShell();
  const rows = data ?? [];
  const done = rows.filter((r) => r.status === 'COMPLETED');
  const [selId, setSelId] = useState<string | null>(null);
  const sel = done.find((j) => j.pipeline_id === selId) ?? done[0];
  const [busy, setBusy] = useState(false);

  const latestByPipeline = useMemo(() => {
    const list = validationData ?? [];
    const m = new Map<string, (typeof list)[number]>();
    for (const v of list) {
      if (!m.has(v.pipeline_id)) m.set(v.pipeline_id, v); // already DESC by created_at
    }
    return m;
  }, [validationData]);

  const matches = (validationData ?? []).filter((v) => v.talend_row_count != null).length;
  const checked = (validationData ?? []).filter((v) => v.status === 'DESTINATION_ONLY').length;
  const notValidated = done.length - new Set((validationData ?? []).map((v) => v.pipeline_id)).size;

  const runSel = async () => {
    if (!sel) return;
    setBusy(true);
    try {
      const result = await runValidationJob(sel.pipeline_id);
      toast(`Validation run ${result.runId} started for ${sel.pipeline_id}.`);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setBusy(false);
    }
  };

  const runAll = async () => {
    setBusy(true);
    try {
      const result = await runValidationJob();
      toast(`Validation run ${result.runId} started for every completed pipeline not yet validated.`);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setBusy(false);
    }
  };

  const v = sel ? latestByPipeline.get(sel.pipeline_id) : undefined;

  return (
    <>
      <section className="g g-kpi">
        <Tile label="Talend baseline matches" value={matches} sub="no Talend baseline exists yet -- always 0" />
        <Tile label="Destination checked" value={checked} sub="real row count + checksum computed" subCls={checked ? 'ok' : undefined} />
        <Tile label="Not validated" value={Math.max(0, notValidated)} sub="completed, no validation run yet" />
        <div className="card kpi">
          <div className="l">Run validation</div>
          <div className="row" style={{ marginTop: 6 }}>
            <button className="btn sm" disabled={busy} onClick={runAll}>
              <Icon name="check" size={14} />
              Validate all completed
            </button>
          </div>
          <p className="hint" style={{ marginTop: 8 }}>
            Computes real destination stats via lineage + control.destination_path. See Guardrails for what this does
            and does not cover.
          </p>
        </div>
      </section>

      <section className="split">
        <div className="card">
          <div className="hd">
            <h3>Pipelines</h3>
            <span className="eyebrow">completed only</span>
          </div>
          <div className="vl">
            {done.length === 0 && <Empty text="No completed pipelines yet." />}
            {done.map((j) => {
              const jv = latestByPipeline.get(j.pipeline_id);
              return (
                <button key={j.pipeline_id} className="vi" aria-current={sel?.pipeline_id === j.pipeline_id} onClick={() => setSelId(j.pipeline_id)}>
                  <span className="n">{j.pipeline_id}</span>
                  <span className={`pill ${jv ? (jv.status === 'DESTINATION_ONLY' ? 'info' : 'warn') : ''}`}>
                    <i />
                    {jv ? jv.status.replace(/_/g, ' ').toLowerCase() : 'not run'}
                  </span>
                </button>
              );
            })}
          </div>
        </div>

        <div className="g" style={{ minWidth: 0 }}>
          {!sel ? (
            <div className="card">
              <Empty text="Completed pipelines appear here for validation." />
            </div>
          ) : (
            <div className="card">
              <div className="hd">
                <div>
                  <h3 style={{ fontSize: 16 }}>{sel.pipeline_id}</h3>
                  <span className="muted mono" style={{ fontSize: 12 }}>
                    Talend baseline vs destination table
                  </span>
                </div>
                <span className={`pill ${v ? (v.status === 'DESTINATION_ONLY' ? 'info' : 'warn') : ''}`}>
                  <i />
                  {v ? v.status.replace(/_/g, ' ').toLowerCase() : 'not run'}
                </span>
              </div>
              <div className="row" style={{ marginBottom: 14 }}>
                <button className="btn sm" disabled={busy} onClick={runSel}>
                  <Icon name="check" size={14} />
                  {v ? 'Re-run validation' : 'Run validation'}
                </button>
                <button className="btn sm" disabled>
                  <Icon name="spark" size={14} />
                  Analyze with AI
                </button>
                <button className="btn sm primary" disabled>
                  <Icon name="wand" size={14} />
                  Apply patch and re-run
                </button>
              </div>
              <div className="tw">
                <table>
                  <thead>
                    <tr>
                      <th>Check</th>
                      <th>Talend</th>
                      <th>Destination</th>
                      <th>Result</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>Destination table</td>
                      <td className="mono muted">—</td>
                      <td className="mono">{v?.destination_table ?? '—'}</td>
                      <td className="muted">{v ? v.status.replace(/_/g, ' ').toLowerCase() : 'not run'}</td>
                    </tr>
                    <tr>
                      <td>Row count</td>
                      <td className="mono muted">{v?.talend_row_count ?? '—'}</td>
                      <td className="mono">{v?.destination_row_count ?? '—'}</td>
                      <td className="muted">{v?.talend_row_count != null ? 'comparable' : 'no baseline'}</td>
                    </tr>
                    <tr>
                      <td>Whole-row checksum</td>
                      <td className="mono muted">{v?.talend_column_checksums ?? '—'}</td>
                      <td className="mono" style={{ maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {v?.destination_column_checksums ?? '—'}
                      </td>
                      <td className="muted">{v?.talend_column_checksums != null ? 'comparable' : 'no baseline'}</td>
                    </tr>
                    <tr>
                      <td>Per-column checksums</td>
                      <td className="mono muted">—</td>
                      <td className="mono muted">not computed</td>
                      <td className="muted">not built yet</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              {v?.notes && (
                <p className="hint" style={{ marginTop: 10 }}>
                  {v.notes}
                </p>
              )}
              {!v && (
                <p className="hint" style={{ marginTop: 10 }}>
                  No validation run yet. The Talend columns stay blank even after a run -- no Talend baseline exists for
                  these jobs.
                </p>
              )}
            </div>
          )}
        </div>
      </section>
    </>
  );
}
