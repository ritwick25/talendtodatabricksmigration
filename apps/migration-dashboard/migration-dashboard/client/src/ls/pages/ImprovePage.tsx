import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { Tile, Empty } from '../components';
import { bool } from '../utils';

function safeParse(json: unknown): Record<string, unknown> | null {
  if (!json) return null;
  if (typeof json === 'object') return json as Record<string, unknown>;
  if (typeof json !== 'string') return null;
  try {
    return JSON.parse(json);
  } catch {
    return null;
  }
}

export function ImprovePage() {
  const { data } = useAnalyticsQuery('improvement_log_list', {});
  const list = data ?? [];
  const merged = list.filter((p) => bool(p.merged));
  const closed = list.filter((p) => !bool(p.merged));

  return (
    <>
      <section className="g g-kpi">
        <Tile label="Improvement cycles" value={list.length} sub="rows in improvement_log" />
        <Tile label="Merged" value={merged.length} sub="knowledge/prompts changed" subCls="ok" />
        <Tile label="Closed" value={closed.length} sub="rejected by the eval gate" subCls={closed.length ? 'bad' : undefined} />
      </section>

      <section className="card">
        <div className="hd">
          <h3>Improvement history</h3>
          <span className="eyebrow">from talend_migration.migration.improvement_log</span>
        </div>
        <div className="g" style={{ gap: 12 }}>
          {list.length === 0 && <Empty text="The improver opens its first entry after the first batch with failures." />}
          {list.map((p) => {
            const before = safeParse(p.before_metrics);
            const after = safeParse(p.after_metrics);
            return (
              <div key={p.improvement_id} className="pr">
                <div className="row" style={{ justifyContent: 'space-between' }}>
                  <h4>{p.topic}</h4>
                  <span className={`pill ${bool(p.merged) ? 'ok' : 'bad'}`}>
                    <i />
                    {bool(p.merged) ? 'Merged' : 'Closed by gate'}
                  </span>
                </div>
                <div className="br">{p.what}</div>
                {p.why && <div style={{ fontSize: 13 }}>{p.why}</div>}
                {(before || after) && (
                  <div className="mt">
                    {Object.keys(before ?? after ?? {}).map((k) => (
                      <div key={k}>
                        {k}
                        <b>
                          {String(before?.[k] ?? '—')} → {String(after?.[k] ?? '—')}
                        </b>
                      </div>
                    ))}
                  </div>
                )}
                {p.pr_url && (
                  <a className="muted mono" style={{ fontSize: 11.5 }} href={p.pr_url} target="_blank" rel="noopener noreferrer">
                    {p.pr_url}
                  </a>
                )}
              </div>
            );
          })}
        </div>
      </section>

      <section className="card">
        <div className="hd">
          <h3>What the improver may edit</h3>
        </div>
        <p className="hint">
          The improver changes only files outside the protected paths listed on Guardrails. A change merges only after the
          eval gate passes against the regenerated sample. No GitHub remote is configured for this repo, so "open a PR"
          today means a local branch + commit, ready to push once a remote exists.
        </p>
      </section>
    </>
  );
}
