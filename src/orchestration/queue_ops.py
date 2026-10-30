"""Plain-code access to migration.queue: batch selection, status
transitions, and the resumability behavior (on restart, IN_PROGRESS
items are picked up again before any new PENDING ones -- never silently
dropped mid-step).
"""
from __future__ import annotations

from src.tools.sql_exec import esc, exec_sql, rows

CATALOG = "talend_migration"
SCHEMA = "migration"
TABLE = f"{CATALOG}.{SCHEMA}.queue"


def select_next_batch(batch_id: str, warehouse_id: str, profile: str = "dev", batch_size: int = 5) -> list[str]:
    """Resumable batch selection: if a previous run left IN_PROGRESS rows
    (crash mid-batch), return those first instead of picking new PENDING
    ones -- the caller re-runs each from the start of its current step.
    """
    resumed = exec_sql(
        f"SELECT pipeline_id FROM {TABLE} WHERE status = 'IN_PROGRESS' "
        f"ORDER BY complexity_score ASC, pipeline_id ASC LIMIT {batch_size}",
        warehouse_id, profile,
    )
    resumed_ids = [r[0] for r in rows(resumed)]
    if resumed_ids:
        return resumed_ids

    pending = exec_sql(
        f"SELECT pipeline_id FROM {TABLE} WHERE status = 'PENDING' "
        f"ORDER BY complexity_score ASC, pipeline_id ASC LIMIT {batch_size}",
        warehouse_id, profile,
    )
    pipeline_ids = [r[0] for r in rows(pending)]
    for pid in pipeline_ids:
        exec_sql(
            f"UPDATE {TABLE} SET status = 'IN_PROGRESS', batch_id = '{esc(batch_id)}', "
            f"updated_at = current_timestamp() WHERE pipeline_id = '{esc(pid)}'",
            warehouse_id, profile,
        )
    return pipeline_ids


def update_job_status(pipeline_id: str, status: str, warehouse_id: str, profile: str = "dev", **fields) -> None:
    """fields may include any of: fix_attempts (int), last_error (str),
    error_signature (str), code_hash (str), commit_sha (str), cost
    (float), deployed_job_id (str), last_run_id (str), notes (str),
    java_uncertain/spec_uncertain/unresolved_dependencies (bool),
    validation_status (str, always left NULL by this system itself).
    """
    sets = [f"status = '{esc(status)}'", "updated_at = current_timestamp()"]
    for key, value in fields.items():
        if value is None:
            sets.append(f"{key} = NULL")
        elif isinstance(value, bool):
            sets.append(f"{key} = {str(value).lower()}")
        elif isinstance(value, (int, float)):
            sets.append(f"{key} = {value}")
        else:
            sets.append(f"{key} = '{esc(str(value))}'")
    exec_sql(
        f"UPDATE {TABLE} SET {', '.join(sets)} WHERE pipeline_id = '{esc(pipeline_id)}'",
        warehouse_id, profile,
    )


def requeue_in_progress_to_pending(warehouse_id: str, profile: str = "dev") -> None:
    """Used by the kill switch and halt paths: return any unfinished
    work to PENDING rather than leaving it stuck IN_PROGRESS."""
    exec_sql(
        f"UPDATE {TABLE} SET status = 'PENDING', updated_at = current_timestamp() "
        f"WHERE status = 'IN_PROGRESS'",
        warehouse_id, profile,
    )


def requeue_for_knowledge_update(pipeline_ids: list[str], warehouse_id: str, profile: str = "dev") -> None:
    """The only way a FAILED job is retried: the improvement cycle
    targeted its root cause and explicitly re-queues it, resetting the
    fix counter."""
    if not pipeline_ids:
        return
    id_list = ", ".join(f"'{esc(p)}'" for p in pipeline_ids)
    exec_sql(
        f"UPDATE {TABLE} SET status = 'PENDING', fix_attempts = 0, "
        f"updated_at = current_timestamp() WHERE pipeline_id IN ({id_list})",
        warehouse_id, profile,
    )
