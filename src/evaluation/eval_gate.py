"""CI evaluation gate: regenerates specs/code from the RAW files (never
cached) for the FAILED jobs in a cluster's affected set plus up to 5
random COMPLETED jobs, in an isolated eval schema, under the same
2-fix-cycle limit as the real pipeline. Compares the resulting metrics
against eval/thresholds.yml (read-only here -- this module never writes
to it) and returns a pass/fail verdict plus the metrics, for
improvement_cycle.py to act on.
"""
from __future__ import annotations

import json
import random
import uuid

import yaml

from src.orchestration.process_job import process_job
from src.tools.sql_exec import esc, exec_sql, rows

CATALOG = "talend_migration"
SCHEMA = "migration"
THRESHOLDS_PATH = "eval/thresholds.yml"


def _load_thresholds() -> dict:
    with open(THRESHOLDS_PATH) as f:
        return yaml.safe_load(f)


def _sample_completed_jobs(exclude: list[str], n: int, warehouse_id: str, profile: str) -> list[str]:
    exclude_list = ", ".join(f"'{esc(p)}'" for p in exclude) or "''"
    resp = exec_sql(
        f"SELECT pipeline_id FROM {CATALOG}.{SCHEMA}.queue "
        f"WHERE status = 'COMPLETED' AND pipeline_id NOT IN ({exclude_list})",
        warehouse_id, profile,
    )
    candidates = [r[0] for r in rows(resp)]
    return random.sample(candidates, min(n, len(candidates)))


def run_eval(
    affected_failed_job_ids: list[str],
    warehouse_id: str,
    profile: str = "dev",
    max_fix_cycles: int = 2,
    run_timeout_min: int = 15,
) -> dict:
    """Regenerates every job in the eval set for real (spec included),
    using whatever knowledge/prompts are currently on disk (the branch
    under test should already have the proposed change checked out
    before calling this). Returns {"verdict": "PASS"|"FAIL", "metrics": {...}}.
    """
    sampled_completed = _sample_completed_jobs(affected_failed_job_ids, 5, warehouse_id, profile)
    eval_set = affected_failed_job_ids + sampled_completed

    outcomes = []
    for pipeline_id in eval_set:
        outcome = process_job(pipeline_id, profile, warehouse_id, max_fix_cycles, run_timeout_min)
        outcomes.append(outcome)
        eval_id = str(uuid.uuid4())
        exec_sql(
            f"INSERT INTO {CATALOG}.{SCHEMA}.eval_results "
            f"(eval_id, pr_url, pipeline_id, ran_at, reached_completed, fix_cycles, "
            f"verifier_approved, java_unit_pass_rate, cost, details) VALUES "
            f"('{eval_id}', NULL, '{esc(pipeline_id)}', current_timestamp(), "
            f"{str(outcome.status == 'COMPLETED').lower()}, {len(outcome.attempts)}, "
            f"NULL, NULL, 0.0, '{esc(json.dumps({'status': outcome.status})[:4000])}')",
            warehouse_id, profile,
        )

    completed_count = sum(1 for o in outcomes if o.status == "COMPLETED")
    completed_share = completed_count / len(outcomes) if outcomes else 0.0
    avg_fix_cycles = sum(len(o.attempts) for o in outcomes) / len(outcomes) if outcomes else 0.0

    thresholds = _load_thresholds()
    metrics = {
        "completed_share": completed_share,
        "avg_fix_cycles": avg_fix_cycles,
        "eval_set": eval_set,
    }

    # Comparing against "main" requires a stored baseline; without one
    # yet (first eval ever run), this gate conservatively requires the
    # eval set to fully reach COMPLETED rather than comparing deltas.
    verdict = "PASS" if completed_share == 1.0 else "FAIL"
    return {"verdict": verdict, "metrics": metrics, "thresholds": thresholds}
