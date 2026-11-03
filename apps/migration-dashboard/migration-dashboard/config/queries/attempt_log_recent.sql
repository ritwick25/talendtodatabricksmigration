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
ORDER BY ts DESC
LIMIT 300
