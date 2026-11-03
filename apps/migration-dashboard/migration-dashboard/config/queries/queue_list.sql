SELECT
  q.pipeline_id,
  q.source,
  q.status,
  q.fix_attempts,
  q.complexity_score,
  q.java_uncertain,
  q.spec_uncertain,
  q.batch_id,
  q.error_signature,
  q.last_error,
  q.started_at,
  q.spec_started_at,
  q.codegen_completed_at,
  q.run_started_at,
  q.run_completed_at,
  q.convert_duration_seconds,
  q.run_duration_seconds,
  q.total_duration_seconds,
  COALESCE(c.cost, 0) AS cost,
  q.updated_at
FROM talend_migration.migration.queue q
LEFT JOIN (
  SELECT pipeline_id, SUM(cost) AS cost
  FROM talend_migration.migration.attempt_log
  GROUP BY pipeline_id
) c ON q.pipeline_id = c.pipeline_id
ORDER BY
  CASE status
    WHEN 'IN_PROGRESS' THEN 0
    WHEN 'PENDING' THEN 1
    WHEN 'FAILED' THEN 2
    WHEN 'COMPLETED' THEN 3
    ELSE 4
  END,
  complexity_score ASC
