SELECT
  improvement_id,
  created_at,
  topic,
  what,
  why,
  before_metrics,
  after_metrics,
  pr_url,
  merged
FROM talend_migration.migration.improvement_log
ORDER BY created_at DESC
LIMIT 100
