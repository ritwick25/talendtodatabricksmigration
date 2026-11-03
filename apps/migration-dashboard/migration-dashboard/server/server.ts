import { createApp, analytics, files, jobs, server } from '@databricks/appkit';
import { z } from 'zod';
import { registerCustomRoutes } from './custom-routes';

// Both jobs use Databricks' job-level `parameters` feature
// ({{job.parameters.x}} templated into the task's own args), triggered
// via the SDK's job_parameters field on run-now -- not any of the
// taskType-mapped shapes (python_params etc. override a task's own
// argv instead). Omitting taskType passes params through to the SDK
// as-is, so job_parameters here lands exactly where it needs to.
createApp({
  plugins: [
    analytics(),
    files({
      // Volumes default to publicRead() (read-only) unless given an
      // explicit policy -- confirmed denying every upload from both the
      // Sources page and "Save & redeploy" ([appkit:files] Policy denied
      // "upload" on volume "files" in the app's own logs). The app is
      // already gated by workspace access; allowAll() just lets signed-in
      // users write the fix/raw-export files this app's own features need.
      volumes: { files: { policy: files.policy.allowAll() } },
    }),
    jobs({
      jobs: {
        apprun: {
          params: z.object({
            job_parameters: z.object({
              pipeline_ids: z.string().optional(),
              batch_size: z.string().optional(),
              max_batches: z.string().optional(),
            }),
          }),
        },
        manualfix: {
          params: z.object({
            job_parameters: z.object({
              pipeline_id: z.string(),
              code_volume_path: z.string(),
            }),
          }),
        },
        validate: {
          params: z.object({
            job_parameters: z.object({
              pipeline_ids: z.string().optional(),
            }),
          }),
        },
      },
    }),
    server(),
  ],
  onPluginsReady(appkit) {
    appkit.server.extend((app) => {
      registerCustomRoutes(app);
    });
  },
}).catch(console.error);
