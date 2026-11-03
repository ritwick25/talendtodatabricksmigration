SELECT
  batch_id,
  started_at,
  ended_at,
  job_ids,
  completed_count,
  failed_count,
  halted,
  halt_reason,
  cost
FROM talend_migration.migration.batch_log
ORDER BY started_at ASC
LIMIT 100
