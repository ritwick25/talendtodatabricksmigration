-- @param pipeline_id STRING = job_01
SELECT
  pipeline_id,
  source,
  status,
  fix_attempts,
  complexity_score,
  java_uncertain,
  spec_uncertain,
  batch_id,
  error_signature,
  last_error,
  started_at,
  spec_started_at,
  codegen_completed_at,
  run_started_at,
  run_completed_at,
  convert_duration_seconds,
  run_duration_seconds,
  total_duration_seconds,
  updated_at
FROM talend_migration.migration.queue
WHERE pipeline_id = :pipeline_id
