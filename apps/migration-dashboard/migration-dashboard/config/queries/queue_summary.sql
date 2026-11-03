SELECT
  status,
  COUNT(*) AS cnt,
  ROUND(AVG(convert_duration_seconds), 1) AS avg_convert_seconds,
  ROUND(AVG(run_duration_seconds), 1) AS avg_run_seconds
FROM talend_migration.migration.queue
GROUP BY status
