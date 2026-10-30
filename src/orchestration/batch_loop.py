"""The real driver loop: batch after batch until the queue is empty or
halted. Every decision about continuing, retrying, or stopping is read
from migration.control / made by batch_runner.py's pure logic -- this
module's only job is to wire real jobs through it and persist state.

Usage:
    python3 -m src.orchestration.batch_loop --profile dev [--max-batches N]
"""
from __future__ import annotations

import argparse
import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

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


def run_one_batch(warehouse_id: str, profile: str, ctl: control.ControlState) -> bool:
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
                queue_ops.update_job_status(pipeline_id, "FAILED", warehouse_id, profile, last_error=error, notes="unhandled exception in process_job")
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
                )
            elif status == "FAILED":
                failed_count += 1
                infra_streak = 0
                queue_ops.update_job_status(
                    pipeline_id, "FAILED", warehouse_id, profile,
                    fix_attempts=len(outcome.attempts),
                    last_error=(outcome.diagnosis or "")[:4000],
                )
                if failed_count >= ctl.halt_threshold:
                    halted, halt_reason = True, f"{failed_count} jobs FAILED in this batch (threshold {ctl.halt_threshold})"
            elif status == "INFRA_RETRY_EXHAUSTED":
                infra_streak += 1
                queue_ops.update_job_status(pipeline_id, "PENDING", warehouse_id, profile, last_error=(outcome.diagnosis or "")[:4000])
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
    p.add_argument("--max-batches", type=int, default=None, help="stop after this many batches regardless of queue state")
    args = p.parse_args()

    warehouse_id = pick_warehouse(args.profile)
    batches_run = 0
    while True:
        ctl = control.read_control(warehouse_id, args.profile)
        if ctl.kill_switch:
            print("kill_switch is set -- exiting without starting a new batch")
            queue_ops.requeue_in_progress_to_pending(warehouse_id, args.profile)
            break
        if ctl.halted:
            print(f"control.halted is set ({ctl.halt_reason}) -- exiting. Reset it manually to resume.")
            break

        halted = run_one_batch(warehouse_id, args.profile, ctl)
        batches_run += 1
        if halted:
            break
        if args.max_batches and batches_run >= args.max_batches:
            print(f"reached --max-batches {args.max_batches}, stopping")
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
