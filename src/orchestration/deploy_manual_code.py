"""App-triggered manual fix: a human edited the generated code in the
dashboard UI and wants it deployed as-is, skipping CODEGEN entirely.
Still goes through the same plain-code gates (syntax, secret scan) and
updates queue/attempt_log the same way a normal attempt would, so the
history stays honest about what happened and why.

Usage:
    python3 -m src.orchestration.deploy_manual_code \\
        --profile dev --pipeline-id job_01_customer_enrichment \\
        --code-volume-path /Volumes/talend_migration/samples/talend_export/app_edits/job_01_customer_enrichment/job.py
"""
from __future__ import annotations

import argparse
import inspect
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(inspect.currentframe().f_code.co_filename).resolve().parents[2]))

from databricks.sdk import WorkspaceClient

from src.orchestration.process_job import log_attempt
from src.tools.bundle_tools import _classify_error_text, _extract_error_summary, deploy_dev, generate_bundle, run_job
from src.tools.code_checks import check_syntax, scan_for_secrets
from src.tools.sql_exec import pick_warehouse
from src.orchestration import queue_ops


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="dev")
    p.add_argument("--pipeline-id", required=True)
    p.add_argument("--code-volume-path", required=True, help="Volume path to the edited job.py")
    p.add_argument("--run-timeout-min", default="15")
    p.add_argument(
        "--pip-dependencies", default="",
        help="Comma-separated pip packages the edited code needs on the job's serverless environment (e.g. openpyxl).",
    )
    args = p.parse_args()
    pip_dependencies = [d.strip() for d in args.pip_dependencies.split(",") if d.strip()]

    warehouse_id = pick_warehouse(args.profile)
    client = WorkspaceClient(profile=args.profile)
    code = client.files.download(args.code_volume_path).contents.read().decode("utf-8")

    print(f"=== manual fix: {args.pipeline_id} from {args.code_volume_path} ({len(code)} chars) ===")
    started_at = time.time()
    queue_ops.update_job_status(args.pipeline_id, "IN_PROGRESS", warehouse_id, args.profile)

    syntax_ok, syntax_err = check_syntax(code)
    if not syntax_ok:
        print(f"syntax error: {syntax_err}")
        queue_ops.update_job_status(args.pipeline_id, "FAILED", warehouse_id, args.profile, last_error=f"manual fix syntax error: {syntax_err}")
        log_attempt(args.pipeline_id, "MANUAL_FIX", "human", {"result": "syntax_error", "error": syntax_err}, warehouse_id, args.profile)
        return

    secrets_ok, secret_findings = scan_for_secrets(code)
    if not secrets_ok:
        print(f"secret(s) detected: {secret_findings}")
        queue_ops.update_job_status(
            args.pipeline_id, "FAILED", warehouse_id, args.profile,
            last_error=f"manual fix: secret(s) detected: {secret_findings}",
            error_signature=f"secret(s) detected: {secret_findings}"[:220],
        )
        log_attempt(args.pipeline_id, "MANUAL_FIX", "human", {"result": "secret_detected", "findings": secret_findings}, warehouse_id, args.profile)
        return

    generate_bundle(args.pipeline_id, code)
    deploy_result = deploy_dev(args.pipeline_id, code=code, pip_dependencies=pip_dependencies, profile=args.profile)
    if not deploy_result.success:
        is_infra = _classify_error_text(deploy_result.output)
        print(f"deploy failed (infra={is_infra}): {deploy_result.output[-2000:]}")
        queue_ops.update_job_status(
            args.pipeline_id, "FAILED", warehouse_id, args.profile,
            last_error=f"manual fix deploy failed: {deploy_result.output[-4000:]}",
            error_signature=_extract_error_summary(deploy_result.output),
        )
        log_attempt(args.pipeline_id, "MANUAL_FIX", "human", {"result": "deploy_failed", "is_infra": is_infra}, warehouse_id, args.profile)
        return

    run_started_at = time.time()
    run_result = run_job(deploy_result.job_id, profile=args.profile, timeout_min=int(args.run_timeout_min))
    run_completed_at = time.time()

    timing = {
        "convert_duration_seconds": round(run_started_at - started_at, 1),
        "run_duration_seconds": round(run_completed_at - run_started_at, 1),
        "total_duration_seconds": round(run_completed_at - started_at, 1),
    }

    if run_result.success:
        print("manual fix: run succeeded")
        queue_ops.update_job_status(args.pipeline_id, "COMPLETED", warehouse_id, args.profile, error_signature=None, last_error=None, **timing)
        log_attempt(args.pipeline_id, "MANUAL_FIX", "human", {"result": "success"}, warehouse_id, args.profile)
    else:
        print(f"manual fix: run failed: {(run_result.error or '')[:2000]}")
        queue_ops.update_job_status(
            args.pipeline_id, "FAILED", warehouse_id, args.profile,
            last_error=f"manual fix run failed: {(run_result.error or '')[:4000]}",
            error_signature=run_result.error_summary, **timing,
        )
        log_attempt(args.pipeline_id, "MANUAL_FIX", "human", {"result": "run_failed"}, warehouse_id, args.profile)


if __name__ == "__main__":
    main()
