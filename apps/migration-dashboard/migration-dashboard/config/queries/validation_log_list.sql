SELECT
  validation_id,
  pipeline_id,
  lineage_id,
  created_at,
  destination_type,
  destination_system,
  destination_table,
  destination_row_count,
  destination_column_checksums,
  talend_row_count,
  talend_column_checksums,
  status,
  notes
FROM talend_migration.migration.validation_log
ORDER BY created_at DESC
LIMIT 200
