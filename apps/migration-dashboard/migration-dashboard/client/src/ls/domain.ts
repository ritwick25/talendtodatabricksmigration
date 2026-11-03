// Real agent roster. Matches the literal `agent` values written to
// attempt_log by src/orchestration/process_job.py -- not simulated.
export const AG = [
  { k: 'SPEC', name: 'Spec', kind: 'LLM', role: 'Reads the raw Talend files and writes a structured job spec.' },
  { k: 'VERIFIER', name: 'Verifier', kind: 'LLM', role: 'Re-reads the raw files and audits the spec for omissions.' },
  { k: 'JAVAROUTINE', name: 'Java routine', kind: 'LLM', role: 'Translates custom Java and unit-tests it in isolation.' },
  { k: 'CODEGEN', name: 'Code generator', kind: 'LLM', role: 'Writes and minimally fixes PySpark and Lakeflow code.' },
  { k: 'CODE_REVIEW', name: 'Code review', kind: 'LLM', role: 'Checks for secrets, forbidden APIs and missing Java blocks.' },
  { k: 'IMPROVER', name: 'Improver', kind: 'LLM', role: 'Turns failures into gated knowledge pull requests.' },
] as const;

export const AGN: Record<string, string> = Object.fromEntries(AG.map((a) => [a.k, a.name]));
AGN.MANUAL_FIX = 'Manual fix (human)';
AGN.ctl = 'Control plane';

// Observable stage groups, limited to what the queue table's timing columns
// can actually distinguish (started_at / spec_started_at / codegen_completed_at
// / run_started_at / run_completed_at) -- no per-substage timestamps exist, so
// no finer-grained dots are shown than that.
export const STG = ['spec', 'codegen', 'run'] as const;
export const STGL: Record<string, string> = {
  spec: 'Spec / Verify / Java',
  codegen: 'Codegen / Review / Deploy',
  run: 'Run',
};

export type QueueRow = {
  pipeline_id: string;
  source: string | null;
  status: string;
  fix_attempts: number | null;
  complexity_score: number | null;
  java_uncertain: boolean | null;
  spec_uncertain: boolean | null;
  batch_id: string | null;
  error_signature: string | null;
  last_error: string | null;
  started_at: string | null;
  spec_started_at: string | null;
  codegen_completed_at: string | null;
  run_started_at: string | null;
  run_completed_at: string | null;
  convert_duration_seconds: number | null;
  run_duration_seconds: number | null;
  total_duration_seconds: number | null;
  cost: number;
  updated_at: string | null;
};

export function deriveStage(j: QueueRow): 'spec' | 'codegen' | 'run' | null {
  if (j.status === 'PENDING') return null;
  if (j.run_started_at) return 'run';
  if (j.codegen_completed_at) return 'codegen';
  return 'spec';
}

export const TITLES: Record<string, [string, string]> = {
  overview: ['Migration overview', 'Talend to Databricks, batch by batch'],
  sources: ['Sources', 'Choose the Talend export and the migration destination'],
  agents: ['Agent control room', 'Six agents under one deterministic control plane'],
  pipelines: ['Pipelines', 'Every job, its stage and its outcome'],
  validation: ['Data validation', 'Compare with Talend, patch what differs'],
  improve: ['Self-improvement', 'Failures become knowledge, gated by evaluation'],
  guard: ['Guardrails', 'Limits the agents cannot change'],
};

export const NAV: Array<[string, string, string]> = [
  ['overview', 'Overview', 'grid'],
  ['sources', 'Sources', 'folder'],
  ['agents', 'Agents', 'spark'],
  ['pipelines', 'Pipelines', 'rows'],
  ['validation', 'Validation', 'check'],
  ['improve', 'Self-improvement', 'trend'],
  ['guard', 'Guardrails', 'shield'],
];

// Verbatim from src/agents/improver_agent.py PROTECTED_PREFIXES.
export const PROTECTED_PATHS = [
  'src/orchestration/',
  'src/evaluation/',
  'eval/thresholds.yml',
  'databricks.yml',
  'resources/',
  '.github/workflows/',
  'CODEOWNERS',
];
