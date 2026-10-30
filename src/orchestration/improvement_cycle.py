"""The self-improvement cycle: runs after every batch and at every halt.
Reads new attempt_log rows, asks the IMPROVER agent for proposals
(capped at 3, protected paths already filtered out by the agent module),
and for each one: creates a branch, writes the file, commits, runs the
eval gate for real, and merges only on a clean PASS.

No remote is configured for this repo yet (local-only per Phase 0), so
"open a PR" here means: create the branch + commit locally, ready to
push and open a PR once a remote exists. Documented limitation, not
silently skipped -- see the printed note each run.

Usage:
    python3 -m src.orchestration.improvement_cycle --profile dev --since-batch <batch_id>
"""
from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from pathlib import Path

from src.agents.improver_agent import propose_improvements
from src.evaluation.eval_gate import run_eval
from src.tools.sql_exec import esc, exec_sql, pick_warehouse, rows

CATALOG = "talend_migration"
SCHEMA = "migration"
REPO_ROOT = Path(__file__).resolve().parents[2]
HAS_REMOTE = subprocess.run(
    ["git", "remote"], cwd=REPO_ROOT, capture_output=True, text=True,
).stdout.strip() != ""


def get_new_attempt_log_rows(since_timestamp: str | None, warehouse_id: str, profile: str) -> list[dict]:
    where = f"WHERE ts > '{esc(since_timestamp)}'" if since_timestamp else ""
    resp = exec_sql(
        f"SELECT attempt_id, pipeline_id, agent, model, error_signature, details "
        f"FROM {CATALOG}.{SCHEMA}.attempt_log {where} ORDER BY ts DESC LIMIT 500",
        warehouse_id, profile,
    )
    columns = ["attempt_id", "pipeline_id", "agent", "model", "error_signature", "details"]
    return [dict(zip(columns, r)) for r in rows(resp)]


def get_failed_job_ids(warehouse_id: str, profile: str) -> list[str]:
    resp = exec_sql(f"SELECT pipeline_id FROM {CATALOG}.{SCHEMA}.queue WHERE status = 'FAILED'", warehouse_id, profile)
    return [r[0] for r in rows(resp)]


def apply_proposal(proposal: dict) -> Path:
    target = REPO_ROOT / proposal["target_path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(proposal["file_content_or_diff"])
    return target


def log_improvement(proposal, before_metrics, after_metrics, merged, warehouse_id, profile):
    statement = f"""
    INSERT INTO {CATALOG}.{SCHEMA}.improvement_log
    (improvement_id, created_at, topic, what, why, before_metrics, after_metrics, assumptions, pr_url, merged)
    VALUES ('{uuid.uuid4()}', current_timestamp(), '{esc(proposal.get("topic", ""))}',
            '{esc(proposal.get("change_type", ""))}: {esc(proposal.get("target_path", ""))}',
            '{esc(proposal.get("rationale", ""))[:4000]}',
            '{esc(json.dumps(before_metrics))}', '{esc(json.dumps(after_metrics))}',
            '{esc(proposal.get("rationale", ""))[:2000]}', NULL, {str(merged).lower()})
    """
    exec_sql(statement.strip(), warehouse_id, profile)


def run_cycle(profile: str, since_timestamp: str | None = None) -> None:
    warehouse_id = pick_warehouse(profile)
    attempt_rows = get_new_attempt_log_rows(since_timestamp, warehouse_id, profile)
    if not attempt_rows:
        print("no new attempt_log rows -- nothing to improve on")
        return

    proposals = propose_improvements(attempt_rows, profile=profile)
    print(f"{len(proposals)} proposal(s) after protected-path filtering")

    failed_job_ids = get_failed_job_ids(warehouse_id, profile)

    for proposal in proposals:
        topic = proposal.get("topic", "untitled")
        branch = f"improve/{topic}"
        print(f"\n--- proposal: {topic} -> {proposal.get('target_path')} ---")

        subprocess.run(["git", "checkout", "-b", branch], cwd=REPO_ROOT, check=True, capture_output=True)
        try:
            target = apply_proposal(proposal)
            subprocess.run(["git", "add", str(target.relative_to(REPO_ROOT))], cwd=REPO_ROOT, check=True)
            subprocess.run(
                ["git", "commit", "-m", f"Improvement: {topic}\n\n{proposal.get('rationale', '')}\n\nCo-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"],
                cwd=REPO_ROOT, check=True, capture_output=True,
            )

            eval_result = run_eval(failed_job_ids[:5] or [], warehouse_id, profile)
            print(f"eval verdict: {eval_result['verdict']} metrics={eval_result['metrics']}")

            if eval_result["verdict"] == "PASS":
                subprocess.run(["git", "checkout", "main"], cwd=REPO_ROOT, check=True, capture_output=True)
                subprocess.run(["git", "merge", "--no-ff", branch], cwd=REPO_ROOT, check=True, capture_output=True)
                print(f"MERGED {branch} into main"
                      + ("" if HAS_REMOTE else " (local-only merge -- no remote configured to open a PR against)"))
                log_improvement(proposal, {}, eval_result["metrics"], True, warehouse_id, profile)
            else:
                subprocess.run(["git", "checkout", "main"], cwd=REPO_ROOT, check=True, capture_output=True)
                print(f"REJECTED {branch} (left as a local branch, not merged)")
                log_improvement(proposal, {}, eval_result["metrics"], False, warehouse_id, profile)
        except subprocess.CalledProcessError as e:
            print(f"ERROR applying/evaluating proposal, leaving branch {branch} for inspection: {e}")
            subprocess.run(["git", "checkout", "main"], cwd=REPO_ROOT, capture_output=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="dev")
    p.add_argument("--since-timestamp", default=None)
    args = p.parse_args()
    run_cycle(args.profile, args.since_timestamp)


if __name__ == "__main__":
    main()
