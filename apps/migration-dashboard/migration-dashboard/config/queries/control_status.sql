SELECT
  kill_switch,
  halted,
  halt_reason,
  batch_size,
  max_fix_cycles,
  halt_threshold,
  per_job_run_timeout_min,
  global_daily_dbu_cap,
  source_path,
  destination_path,
  updated_at
FROM talend_migration.migration.control
WHERE id = 1
