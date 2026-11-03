import { useEffect, useState } from 'react';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import { useShell } from './ShellState';
import { Icon } from './icons';
import { StatusPill } from './components';
import { formatSeconds, bool, text } from './utils';
import { fetchGeneratedCode, submitFixedCode, runManualFixJob, runAppJob } from '../lib/api';

type AttemptRow = {
  attempt_id: string;
  ts: string;
  agent: string;
  model: string | null;
  cost: number | null;
  error_signature: string | null;
  injection_suspected: boolean | null;
  details: unknown;
};

type Detail = {
  status: string;
  java_uncertain: boolean | string | null;
  spec_uncertain: boolean | string | null;
  run_started_at: string | null;
  run_completed_at: string | null;
  error_signature: string | null;
};

const AGENT_LABEL: Record<string, string> = {
  SPEC: 'Spec',
  VERIFIER: 'Verifier',
  JAVAROUTINE: 'Java routine',
  CODEGEN: 'Code generator',
  CODE_REVIEW: 'Code review',
  MANUAL_FIX: 'Manual fix (human)',
};

function parseDetails(raw: unknown): Record<string, unknown> | null {
  if (raw === null || raw === undefined) return null;
  if (typeof raw === 'object') return raw as Record<string, unknown>;
  if (typeof raw !== 'string') return null;
  try {
    return JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return null;
  }
}

// Real timestamps; parse to epoch ms rather than comparing the raw strings
// -- confirmed correct in this data, but numeric comparison is simply the
// correct way to order timestamps regardless of string formatting.
const byTsDesc = (a: AttemptRow, b: AttemptRow) => new Date(b.ts).getTime() - new Date(a.ts).getTime();

type StageStatus = 'done' | 'bad' | 'now' | 'q';

function computeStages(attempts: AttemptRow[], detail: Detail | undefined): Array<{ key: string; label: string; status: StageStatus; message: string }> {
  const byAgent = new Map<string, AttemptRow[]>();
  for (const a of attempts) {
    (byAgent.get(a.agent) ?? byAgent.set(a.agent, []).get(a.agent)!).push(a);
  }
  const latest = (agent: string) => byAgent.get(agent)?.slice().sort(byTsDesc)[0];

  const spec = latest('SPEC');
  const verifier = latest('VERIFIER');
  const verifierDetails = verifier ? parseDetails(verifier.details) : null;
  const verifierApproved = verifierDetails?.approved;
  const java = byAgent.get('JAVAROUTINE');
  const codegen = byAgent.get('CODEGEN');
  const review = latest('CODE_REVIEW');
  const reviewDetails = review ? parseDetails(review.details) : null;
  const reviewApproved = reviewDetails?.approved;

  const stages: Array<{ key: string; label: string; status: StageStatus; message: string }> = [];

  stages.push({ key: 'spec', label: 'Spec', status: spec ? 'done' : 'q', message: spec ? `${byAgent.get('SPEC')?.length ?? 1} round(s)` : 'not started' });

  stages.push({
    key: 'verify',
    label: 'Verify',
    status: !verifier ? 'q' : verifierApproved === false ? 'bad' : 'done',
    message: !verifier ? 'not started' : verifierApproved === false ? 'not approved' : 'approved',
  });

  if (java?.length) {
    const anyUncertain = bool(detail?.java_uncertain);
    stages.push({ key: 'java', label: 'Java', status: anyUncertain ? 'bad' : 'done', message: `${java.length} block(s)${anyUncertain ? ' -- best-effort' : ''}` });
  }

  stages.push({ key: 'codegen', label: 'Codegen', status: codegen?.length ? 'done' : 'q', message: codegen?.length ? `${codegen.length} attempt(s)` : 'not started' });

  stages.push({
    key: 'review',
    label: 'Review',
    status: !review ? 'q' : reviewApproved === true ? 'done' : 'bad',
    message: !review ? 'not started' : reviewApproved === true ? 'approved' : text((reviewDetails?.findings as unknown[] | undefined)?.[0] ?? 'blocked'),
  });

  let deployStatus: StageStatus = 'q';
  let deployMessage = 'not started';
  if (detail?.status === 'COMPLETED') {
    deployStatus = 'done';
    deployMessage = 'run succeeded';
  } else if (detail?.run_started_at && !detail?.run_completed_at && detail?.status === 'IN_PROGRESS') {
    deployStatus = 'now';
    deployMessage = 'running';
  } else if (detail?.error_signature) {
    deployStatus = 'bad';
    deployMessage = detail.error_signature;
  } else if (detail?.run_started_at) {
    deployStatus = 'bad';
    deployMessage = 'failed';
  }
  stages.push({ key: 'run', label: 'Deploy & run', status: deployStatus, message: deployMessage });

  return stages;
}

// One clean line per attempt, reading the agent-specific shape of its
// `details` JSON instead of dumping the raw object -- that raw dump (full
// findings arrays, token counts, etc.) is what made the Attempts list
// unreadable.
function summarizeAttempt(a: AttemptRow): string {
  if (a.error_signature) return a.error_signature;
  const d = parseDetails(a.details);
  if (!d) return 'no details recorded';
  switch (a.agent) {
    case 'SPEC':
      return d.regenerated_for_verifier ? 'regenerated after verifier feedback' : `complexity ${text(d.complexity_score ?? '?')}/100`;
    case 'VERIFIER':
      return d.approved ? 'approved' : 'not approved -- requested changes';
    case 'JAVAROUTINE':
      return `${text(d.block_id ?? 'block')} -> ${text(d.tier ?? 'translated')}`;
    case 'CODEGEN':
      return d.fix_cycle ? `fix cycle ${text(d.fix_cycle)}` : 'first pass';
    case 'CODE_REVIEW': {
      if (d.approved) return 'approved';
      const findings = (d.findings as Array<Record<string, unknown>> | undefined) ?? [];
      const top = findings[0];
      return top ? `blocked -- ${text(top.category)}: ${text(top.description)}` : 'blocked';
    }
    case 'MANUAL_FIX':
      return text(d.result ?? 'applied');
    default:
      return text(d);
  }
}

interface AgentSummary {
  agent: string;
  rounds: number;
  ok: boolean;
  last: string;
}

function summarizeByAgent(attempts: AttemptRow[]): AgentSummary[] {
  const byAgent = new Map<string, AttemptRow[]>();
  for (const a of attempts) {
    (byAgent.get(a.agent) ?? byAgent.set(a.agent, []).get(a.agent)!).push(a);
  }
  return [...byAgent.entries()]
    .map(([agent, rows]) => {
      const sorted = rows.slice().sort(byTsDesc);
      const latest = sorted[0];
      const latestDetails = parseDetails(latest.details);
      return {
        agent,
        rounds: rows.length,
        ok: !latest.error_signature && latestDetails?.approved !== false,
        last: summarizeAttempt(latest),
      };
    })
    .sort((a, b) => a.agent.localeCompare(b.agent));
}

// Polls while the drawer is open so an actively-running job (SPEC -> ... ->
// RUN takes minutes) doesn't show a frozen snapshot from whenever the
// drawer happened to first load -- confirmed as the real cause of a
// "Review blocked" view that was actually stale by the time it was read:
// the job had since been re-reviewed, approved, deployed and run.
const POLL_MS = 8000;

function DrawerBody({ pipelineId }: { pipelineId: string }) {
  const { closeDrawer, toast } = useShell();

  const { data: detailRows } = useAnalyticsQuery('job_detail', { pipeline_id: sql.string(pipelineId) });
  const { data: attempts } = useAnalyticsQuery('attempt_log_for_job', { pipeline_id: sql.string(pipelineId) });
  const detail = detailRows?.[0];
  const stages = computeStages(attempts ?? [], detail);
  const agentSummaries = summarizeByAgent(attempts ?? []);
  const isCompleted = detail?.status === 'COMPLETED';

  const [code, setCode] = useState<string | null>(null);
  const [codeLoading, setCodeLoading] = useState(false);
  const [codeError, setCodeError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [rerunning, setRerunning] = useState(false);

  useEffect(() => {
    setCode(null);
    setCodeLoading(true);
    setCodeError(null);
    fetchGeneratedCode(pipelineId)
      .then((res) => setCode(res.code))
      .catch((err) => setCodeError(err instanceof Error ? err.message : String(err)))
      .finally(() => setCodeLoading(false));
  }, [pipelineId]);

  const saveAndRedeploy = async () => {
    if (code === null) return;
    setSaving(true);
    try {
      const { codeVolumePath } = await submitFixedCode(pipelineId, code);
      const result = await runManualFixJob(pipelineId, codeVolumePath);
      toast(`Deploying fixed code -- run ${result.runId} started.`);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setSaving(false);
    }
  };

  const rerunFull = async () => {
    setRerunning(true);
    try {
      await runAppJob({ pipelineIds: pipelineId, batchSize: '', maxBatches: '1' });
      toast(`Full migration re-run triggered for ${pipelineId}. This view refreshes automatically every ${POLL_MS / 1000}s.`);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'warn');
    } finally {
      setRerunning(false);
    }
  };

  return (
    <>
      <div className="scrim" onClick={closeDrawer} />
      <aside className="drawer" role="dialog" aria-label={pipelineId}>
        <div className="dh">
          <div>
            <div className="eyebrow">{detail?.source ?? '—'} · batch {detail?.batch_id ?? '-'}</div>
            <h2>{pipelineId}</h2>
            <div className="row" style={{ marginTop: 8 }}>
              {detail && <StatusPill status={detail.status} />}
              {bool(detail?.java_uncertain) && <span className="chip warn">java uncertain</span>}
              {bool(detail?.spec_uncertain) && <span className="chip warn">spec uncertain</span>}
            </div>
          </div>
          <button className="btn icon" onClick={closeDrawer} aria-label="Close">
            <Icon name="x" size={16} />
          </button>
        </div>
        <div className="db">
          {detail && (
            <div className="kv">
              <div>
                Complexity<b>{detail.complexity_score ?? '—'}/100</b>
              </div>
              <div>
                Fix cycles<b>{detail.fix_attempts ?? 0}</b>
              </div>
              <div>
                Convert<b>{formatSeconds(detail.convert_duration_seconds)}</b>
              </div>
              <div>
                Run<b>{formatSeconds(detail.run_duration_seconds)}</b>
              </div>
              <div>
                Total<b>{formatSeconds(detail.total_duration_seconds)}</b>
              </div>
            </div>
          )}
          <div className="sec">
            <h5>Pipeline stages</h5>
            <div className="wf">
              {stages.map((s) => (
                <div key={s.key} className={s.status === 'q' ? '' : s.status}>
                  <b>{s.label}</b>
                  {s.message}
                </div>
              ))}
            </div>
          </div>

          {/* A COMPLETED job never shows error text, even if a stale value somehow
              lingers in the row -- a completed run has nothing to show here. */}
          {!isCompleted && detail?.error_signature && (
            <div className="note" style={{ background: 'var(--bad-soft)', color: 'var(--bad)' }}>
              <b>{detail.error_signature}</b>
            </div>
          )}
          {!isCompleted && detail?.last_error && (
            <details>
              <summary className="hint" style={{ cursor: 'pointer' }}>
                Full log
              </summary>
              <pre className="code" style={{ marginTop: 8, maxHeight: 260, overflow: 'auto' }}>
                {detail.last_error}
              </pre>
            </details>
          )}
          <div className="row">
            <button className="btn" disabled={rerunning} onClick={() => void rerunFull()}>
              <Icon name="replay" size={16} />
              Re-run full migration
            </button>
          </div>

          <div className="sec">
            <h5>Generated code</h5>
            {codeLoading && <div className="empty">Loading…</div>}
            {/* Not deployed through the current readable path isn't a problem with the
                job itself (it may well be COMPLETED) -- informational styling, not red. */}
            {codeError && (
              <div className="note" style={{ background: 'var(--accent-soft)', color: 'var(--accent)' }}>
                {codeError}
              </div>
            )}
            {!codeLoading && !codeError && (
              <>
                <textarea
                  className="code mono"
                  style={{ width: '100%', minHeight: 320, resize: 'vertical' }}
                  value={code ?? ''}
                  onChange={(e) => setCode(e.target.value)}
                  spellCheck={false}
                />
                <div className="row" style={{ marginTop: 10 }}>
                  <button className="btn sm primary" disabled={saving} onClick={() => void saveAndRedeploy()}>
                    <Icon name="wand" size={14} />
                    Save &amp; redeploy
                  </button>
                </div>
              </>
            )}
          </div>

          <div className="sec">
            <h5>Agent summary</h5>
            <div className="tw">
              <table>
                <thead>
                  <tr>
                    <th>Agent</th>
                    <th className="r">Rounds</th>
                    <th>Last outcome</th>
                  </tr>
                </thead>
                <tbody>
                  {agentSummaries.length === 0 && (
                    <tr>
                      <td colSpan={3} className="muted">
                        No attempts yet.
                      </td>
                    </tr>
                  )}
                  {agentSummaries.map((s) => (
                    <tr key={s.agent}>
                      <td className="nm">{AGENT_LABEL[s.agent] ?? s.agent}</td>
                      <td className="r num mono">{s.rounds}</td>
                      <td className={s.ok ? '' : 'muted'} style={{ color: s.ok ? undefined : 'var(--bad)' }}>
                        {s.last}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="sec">
            <details>
              <summary className="hint" style={{ cursor: 'pointer' }}>
                All attempts (raw)
              </summary>
              <div className="tl" style={{ marginTop: 8 }}>
                {(attempts ?? []).map((a) => (
                  <div key={a.attempt_id}>
                    <span className={`pt ${a.error_signature ? 'bad' : 'ok'}`} />
                    <div>
                      <b>{AGENT_LABEL[a.agent] ?? a.agent}</b> <code>{a.model ?? ''}</code>
                      <div className="muted">{summarizeAttempt(a)}</div>
                    </div>
                  </div>
                ))}
              </div>
            </details>
          </div>
        </div>
      </aside>
    </>
  );
}

export function JobDrawer() {
  const { drawerJobId, closeDrawer } = useShell();
  const open = drawerJobId !== null;
  const pipelineId = drawerJobId ?? '';
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (!open) return;
    const id = setInterval(() => setTick((t) => t + 1), POLL_MS);
    return () => clearInterval(id);
  }, [open, pipelineId]);

  useEffect(() => {
    const onEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeDrawer();
    };
    document.addEventListener('keydown', onEsc);
    return () => document.removeEventListener('keydown', onEsc);
  }, [closeDrawer]);

  if (!open) return null;
  return <DrawerBody key={`${pipelineId}-${tick}`} pipelineId={pipelineId} />;
}
