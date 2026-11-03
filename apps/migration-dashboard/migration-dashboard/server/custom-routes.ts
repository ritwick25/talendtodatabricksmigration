import type { Application } from 'express';
import { z } from 'zod';
import { getWorkspaceClient, getWarehouseId } from '@databricks/appkit';

const PIPELINE_ID_RE = /^[A-Za-z0-9_-]+$/;

const controlUpdateSchema = z
  .object({
    kill_switch: z.boolean().optional(),
    halted: z.boolean().optional(),
    halt_reason: z.string().nullable().optional(),
    batch_size: z.number().int().positive().optional(),
    max_fix_cycles: z.number().int().positive().optional(),
    halt_threshold: z.number().int().positive().optional(),
    per_job_run_timeout_min: z.number().int().positive().optional(),
    source_path: z.string().optional(),
    destination_path: z.string().optional(),
  })
  .refine((v) => Object.keys(v).length > 0, { message: 'at least one field required' });

export function registerCustomRoutes(app: Application) {
  // Settings panel: flip the kill switch, clear a halt, or change batch/fix-cycle
  // defaults without leaving the app. Writes go straight to the control table
  // via statementExecution -- the analytics plugin's query files are read-only.
  app.post('/api/control/update', async (req, res) => {
    const parsed = controlUpdateSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.message });
      return;
    }
    const fields = parsed.data;
    const typeMap: Record<string, string> = {
      kill_switch: 'BOOLEAN',
      halted: 'BOOLEAN',
      halt_reason: 'STRING',
      batch_size: 'INT',
      max_fix_cycles: 'INT',
      halt_threshold: 'INT',
      per_job_run_timeout_min: 'INT',
      source_path: 'STRING',
      destination_path: 'STRING',
    };
    const setClauses: string[] = [];
    const parameters: { name: string; type: string; value?: string }[] = [];
    for (const [key, value] of Object.entries(fields)) {
      setClauses.push(`${key} = :${key}`);
      parameters.push({
        name: key,
        type: typeMap[key],
        value: value === null ? undefined : String(value),
      });
    }
    setClauses.push('updated_at = current_timestamp()');

    try {
      const client = getWorkspaceClient({});
      const warehouseId = await getWarehouseId();
      await client.statementExecution.executeStatement({
        warehouse_id: warehouseId,
        statement: `UPDATE talend_migration.migration.control SET ${setClauses.join(', ')} WHERE id = 1`,
        parameters,
        wait_timeout: '30s',
      });
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  // Code viewer/editor: the generated job.py lives as a Workspace FILE object
  // (not a UC Volume), so the files plugin can't reach it -- export/import
  // via the Workspace API directly. A dynamic jobs.list()/get() lookup was
  // tried here to find jobs deployed through an older, separate per-job
  // bundle (code under .../.bundle/mig_<pipeline_id>/...) -- confirmed via
  // live logs that it always comes back empty, because the app's own
  // service principal has no permission on any job or workspace path beyond
  // the handful explicitly granted to it (apprun/manualfix/validate jobs,
  // and this one generated/ directory). Listing or reading those older jobs
  // would need a real permission grant on each one, which isn't something to
  // do unilaterally here. So: only the current bundle_tools.deploy_dev()
  // path is readable, and jobs never touched by it get an honest message
  // instead of a guess.
  app.get('/api/code/:pipelineId', async (req, res) => {
    const { pipelineId } = req.params;
    if (!PIPELINE_ID_RE.test(pipelineId)) {
      res.status(400).json({ error: 'invalid pipeline_id' });
      return;
    }
    try {
      const client = getWorkspaceClient({});
      const ownerUserName = process.env.GENERATED_CODE_OWNER ?? 'eb99383c-d9a0-4e65-9a0f-45dfeeb0b830';
      const path = `/Workspace/Users/${ownerUserName}/talend_migration/generated/${pipelineId}/job.py`;
      const exported = await client.workspace.export({ path, format: 'AUTO' });
      const code = exported.content ? Buffer.from(exported.content, 'base64').toString('utf-8') : '';
      res.json({ pipeline_id: pipelineId, path, code });
    } catch {
      // Don't surface the raw 404 body here -- it's almost always this
      // exact, known cause (see the comment above), not a real error. Avoid
      // saying "hasn't been deployed" here -- a job can well be COMPLETED
      // (deployed and run successfully) while still hitting this, if it was
      // deployed through the older per-job bundle this viewer can't read.
      res.status(404).json({
        error:
          `This viewer can't read "${pipelineId}"'s code -- it was deployed through an older path outside the ` +
          `app's current pipeline (the job itself may have run successfully regardless). Use "Re-run full ` +
          'migration" to redeploy it through the current system, which writes to a path this viewer can read.',
      });
    }
  });
}
