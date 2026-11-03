"""The real driver loop: batch after batch until the queue is empty or
halted. Every decision about continuing, retrying, or stopping is read
from migration.control / made by batch_runner.py's pure logic -- this
module's only job is to wire real jobs through it and persist state.

Usage:
    python3 -m src.orchestration.batch_loop --profile dev [--max-batches N]
"""
from __future__ import annotations

import argparse
import inspect
import json
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# As a Databricks Jobs spark_python_task, this file runs via exec() with
# only its own directory on sys.path -- the bundle root (where the `src`
# package lives) isn't added automatically the way `python -m src...`
# does locally. Confirmed by an actual job run failing with
# "ModuleNotFoundError: No module named 'src'" before this was added.
# `__file__` isn't set in this exec() context either (confirmed by a
# second real failure, NameError on __file__) -- co_filename survives
# because it's what compile()'s filename argument sets, and is exactly
# what the traceback itself renders as the file path.
sys.path.insert(0, str(Path(inspect.currentframe().f_code.co_filename).resolve().parents[2]))

from src.orchestration import control, queue_ops
from src.orchestration.process_job import process_job
from src.tools.sql_exec import esc, exec_sql, pick_warehouse

CATALOG = "talend_migration"
SCHEMA = "migration"


def write_batch_log(batch_id, started_at, job_ids, completed_count, failed_count, halted, halt_reason, warehouse_id, profile):
    job_ids_json = json.dumps(job_ids).replace("'", "''")
    reason_sql = "NULL" if halt_reason is None else f"'{esc(halt_reason)}'"
    statement = f"""
    INSERT INTO {CATALOG}.{SCHEMA}.batch_log
    (batch_id, started_at, ended_at, job_ids, completed_count, failed_count, halted, halt_reason, cost)
    VALUES ('{esc(batch_id)}', '{started_at.isoformat()}', current_timestamp(),
            '{job_ids_json}', {completed_count}, {failed_count}, {str(halted).lower()}, {reason_sql}, 0.0)
    """
    exec_sql(statement.strip(), warehouse_id, profile)


def _process_one(pipeline_id, profile, warehouse_id, ctl):
    """Runs in a worker thread. Never raises -- any exception becomes a
    synthetic FAILED-shaped result so the batch's bookkeeping (which
    expects a status string either way) doesn't need a second except
    path for the concurrent case."""
    try:
        outcome = process_job(pipeline_id, profile, warehouse_id, ctl.max_fix_cycles, ctl.per_job_run_timeout_min)
        return pipeline_id, outcome.status, outcome, None
    except Exception as e:
        return pipeline_id, "FAILED", None, str(e)[:4000]


def run_one_batch(
    warehouse_id: str, profile: str, ctl: control.ControlState,
    explicit_pipeline_ids: list[str] | None = None,
) -> bool:
    """Returns True if the batch halted the process.

    Processes the batch's jobs concurrently (spec rule 1: "concurrency
    5") since each job's work is I/O-bound (model calls, Databricks API
    calls), not CPU-bound -- sequential processing was leaving most of
    the wall-clock time idle on network waits. Tradeoff: once dispatched,
    all jobs in the batch run to completion even if an early result
    would have tripped the halt rule -- true mid-flight cancellation of
    a running job's network calls isn't something a thread pool can do
    cleanly, so halt/kill-switch checks happen between batches, not
    mid-batch. The threshold logic below still only counts toward
    halting; it just can't preempt in-flight work from this batch.
    """
    batch_id = str(uuid.uuid4())
    if explicit_pipeline_ids:
        # App-triggered single/bulk run: the caller named exactly which
        # jobs to (re)run, regardless of their current status or the
        # control table's batch_size. Still mark them IN_PROGRESS first,
        # same as the queue-driven path, for consistent resumability.
        pipeline_ids = explicit_pipeline_ids
        for pid in pipeline_ids:
            queue_ops.update_job_status(pid, "IN_PROGRESS", warehouse_id, profile, batch_id=batch_id)
    else:
        pipeline_ids = queue_ops.select_next_batch(batch_id, warehouse_id, profile, batch_size=ctl.batch_size)
    if not pipeline_ids:
        print("queue is empty")
        return False

    print(f"\n=== batch {batch_id}: {pipeline_ids} ===")
    started_at = datetime.now(timezone.utc)
    completed_count = 0
    failed_count = 0
    infra_streak = 0
    halted = False
    halt_reason = None

    with ThreadPoolExecutor(max_workers=len(pipeline_ids)) as pool:
        futures = {
            pool.submit(_process_one, pid, profile, warehouse_id, ctl): pid
            for pid in pipeline_ids
        }
        for future in as_completed(futures):
            pipeline_id, status, outcome, error = future.result()
            print(f"-- {pipeline_id}: {status} --")

            if outcome is None:  # unhandled exception path
                queue_ops.update_job_status(
                    pipeline_id, "FAILED", warehouse_id, profile, last_error=error,
                    error_signature=(error or "")[:220], notes="unhandled exception in process_job",
                )
                failed_count += 1
                infra_streak = 0
                if failed_count >= ctl.halt_threshold:
                    halted, halt_reason = True, f"{failed_count} jobs FAILED in this batch (threshold {ctl.halt_threshold})"
                continue

            if status == "COMPLETED":
                completed_count += 1
                infra_streak = 0
                queue_ops.update_job_status(
                    pipeline_id, "COMPLETED", warehouse_id, profile,
                    fix_attempts=len(outcome.attempts) - 1,
                    java_uncertain=outcome.java_uncertain,
                    spec_uncertain=outcome.spec_uncertain,
                    error_signature=None, last_error=None,
                    **outcome.timing,
                )
            elif status == "FAILED":
                failed_count += 1
                infra_streak = 0
                queue_ops.update_job_status(
                    pipeline_id, "FAILED", warehouse_id, profile,
                    fix_attempts=len(outcome.attempts),
                    last_error=(outcome.diagnosis or "")[:4000],
                    error_signature=outcome.error_summary,
                    **outcome.timing,
                )
                if failed_count >= ctl.halt_threshold:
                    halted, halt_reason = True, f"{failed_count} jobs FAILED in this batch (threshold {ctl.halt_threshold})"
            elif status == "INFRA_RETRY_EXHAUSTED":
                infra_streak += 1
                queue_ops.update_job_status(
                    pipeline_id, "PENDING", warehouse_id, profile,
                    last_error=(outcome.diagnosis or "")[:4000], error_signature=outcome.error_summary,
                )
                if infra_streak >= 3:
                    halted, halt_reason = True, f"{infra_streak} consecutive infra failures"

    write_batch_log(batch_id, started_at, pipeline_ids, completed_count, failed_count, halted, halt_reason, warehouse_id, profile)

    if halted:
        control.set_halted(True, halt_reason, warehouse_id, profile)
        queue_ops.requeue_in_progress_to_pending(warehouse_id, profile)
        print(f"HALTED: {halt_reason}")

    return halted


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="dev")
    p.add_argument(
        "--max-batches", default=None,
        help="Stop after this many batches regardless of queue state. "
             "String type (not int) since Databricks job parameters are always strings -- "
             "an empty string from an unset job parameter means 'no limit', not an error.",
    )
    p.add_argument(
        "--pipeline-ids", default=None,
        help="Comma-separated explicit job list (single or bulk, app-triggered). "
             "Runs exactly these once and exits -- does not fall through to queue-driven batches.",
    )
    p.add_argument(
        "--batch-size", default=None,
        help="Override control.batch_size for this run only (ignored with --pipeline-ids). String type, same reason as --max-batches.",
    )
    args = p.parse_args()
    explicit_pipeline_ids = [p.strip() for p in args.pipeline_ids.split(",") if p.strip()] if args.pipeline_ids else None
    max_batches = int(args.max_batches) if args.max_batches else None
    batch_size_override = int(args.batch_size) if args.batch_size else None

    warehouse_id = pick_warehouse(args.profile)
    batches_run = 0
    while True:
        ctl = control.read_control(warehouse_id, args.profile)
        if batch_size_override:
            ctl.batch_size = batch_size_override
        if ctl.kill_switch:
            print("kill_switch is set -- exiting without starting a new batch")
            queue_ops.requeue_in_progress_to_pending(warehouse_id, args.profile)
            break
        if ctl.halted:
            print(f"control.halted is set ({ctl.halt_reason}) -- exiting. Reset it manually to resume.")
            break

        halted = run_one_batch(warehouse_id, args.profile, ctl, explicit_pipeline_ids=explicit_pipeline_ids)
        batches_run += 1
        if explicit_pipeline_ids:
            print("explicit --pipeline-ids run complete, exiting (not falling through to queue-driven batches)")
            break
        if halted:
            break
        if max_batches and batches_run >= max_batches:
            print(f"reached --max-batches {max_batches}, stopping")
            break

        # Check if the queue is actually empty (no PENDING/IN_PROGRESS left)
        resp = exec_sql(
            f"SELECT count(*) FROM {CATALOG}.{SCHEMA}.queue WHERE status IN ('PENDING', 'IN_PROGRESS')",
            warehouse_id, args.profile,
        )
        remaining = int(resp["result"]["data_array"][0][0])
        if remaining == 0:
            print("queue is empty -- all jobs processed")
            break


if __name__ == "__main__":
    main()
