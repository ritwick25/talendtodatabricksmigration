-- @param pipeline_id STRING = job_01
SELECT
  pipeline_id,
  attempt_id,
  ts,
  agent,
  model,
  cost,
  error_signature,
  injection_suspected,
  details
FROM talend_migration.migration.attempt_log
WHERE pipeline_id = :pipeline_id
ORDER BY ts DESC
LIMIT 50
