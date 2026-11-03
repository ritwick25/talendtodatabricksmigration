export interface RunJobResult {
  runId: number;
}

async function parseOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    let message = `${res.status} ${res.statusText}`;
    try {
      const body = await res.json();
      if (body?.error) message = body.error;
    } catch {
      // body wasn't JSON -- keep the status line
    }
    throw new Error(message);
  }
  return res.json() as Promise<T>;
}

export async function runAppJob(params: {
  pipelineIds?: string;
  batchSize?: string;
  maxBatches?: string;
}): Promise<RunJobResult> {
  const res = await fetch('/api/jobs/apprun/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      params: {
        job_parameters: {
          pipeline_ids: params.pipelineIds ?? '',
          batch_size: params.batchSize ?? '',
          max_batches: params.maxBatches ?? '1',
        },
      },
    }),
  });
  return parseOrThrow<RunJobResult>(res);
}

export async function runValidationJob(pipelineIds?: string): Promise<RunJobResult> {
  const res = await fetch('/api/jobs/validate/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ params: { job_parameters: { pipeline_ids: pipelineIds ?? '' } } }),
  });
  return parseOrThrow<RunJobResult>(res);
}

export async function runManualFixJob(pipelineId: string, codeVolumePath: string): Promise<RunJobResult> {
  const res = await fetch('/api/jobs/manualfix/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      params: { job_parameters: { pipeline_id: pipelineId, code_volume_path: codeVolumePath } },
    }),
  });
  return parseOrThrow<RunJobResult>(res);
}

export async function fetchGeneratedCode(pipelineId: string): Promise<{ pipeline_id: string; path: string; code: string }> {
  const res = await fetch(`/api/code/${encodeURIComponent(pipelineId)}`);
  return parseOrThrow(res);
}

const FIX_VOLUME_PREFIX = '/Volumes/talend_migration/samples/talend_export';

export async function submitFixedCode(pipelineId: string, code: string): Promise<{ codeVolumePath: string }> {
  const relativePath = `manual_fixes/${pipelineId}/job.py`;
  const uploadRes = await fetch(`/api/files/files/upload?path=${encodeURIComponent(relativePath)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain' },
    body: code,
  });
  await parseOrThrow(uploadRes);
  return { codeVolumePath: `${FIX_VOLUME_PREFIX}/${relativePath}` };
}

export async function uploadSourceFile(relativePath: string, file: File): Promise<void> {
  const res = await fetch(`/api/files/files/upload?path=${encodeURIComponent(`raw/${relativePath}`)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream' },
    body: file,
  });
  await parseOrThrow(res);
}

export interface ControlUpdate {
  kill_switch?: boolean;
  halted?: boolean;
  halt_reason?: string | null;
  batch_size?: number;
  max_fix_cycles?: number;
  halt_threshold?: number;
  per_job_run_timeout_min?: number;
  source_path?: string;
  destination_path?: string;
}

export async function updateControl(fields: ControlUpdate): Promise<{ success: true }> {
  const res = await fetch('/api/control/update', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(fields),
  });
  return parseOrThrow(res);
}
