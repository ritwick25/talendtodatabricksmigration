"""Phase 3 driver: run the SPEC agent then the VERIFIER agent on one or
more sample jobs, write the result to migration.job_spec, and print a
human-readable summary (spec summary, verifier findings, complexity
ranking, component inventory) for review.

This enforces the plain-code parts of the spec/verify loop itself: the
MAX_SPEC_ROUNDS cap and whether to regenerate are decided here, never by
an LLM. The orchestrator (Phase 6) will call the same functions; this
script is the standalone entry point for exercising them before the full
batch loop exists.

Usage:
    python3 -m src.orchestration.run_spec_and_verify --profile dev \\
        --pipeline-dir /Volumes/talend_migration/samples/talend_export/samples/job_01_customer_enrichment
"""
from __future__ import annotations

import argparse
import json
import subprocess

from src.agents.spec_agent import run_spec_agent
from src.agents.verifier_agent import MAX_SPEC_ROUNDS, run_verifier_agent
from src.tools.read_raw_files import read_raw_files

CATALOG = "talend_migration"
SCHEMA = "migration"


def _sql_execute(statement: str, warehouse_id: str, profile: str) -> dict:
    payload = json.dumps({"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s"})
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", profile, "--json", payload],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr)
    out = json.loads(result.stdout)
    if out.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(out.get("status", {}).get("error", {}).get("message", out))
    return out


def _pick_warehouse(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "warehouses", "list", "--profile", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    warehouses = json.loads(result.stdout)
    return warehouses[0]["id"]


def _next_spec_version(pipeline_id: str, warehouse_id: str, profile: str) -> int:
    out = _sql_execute(
        f"SELECT coalesce(max(spec_version), 0) FROM {CATALOG}.{SCHEMA}.job_spec "
        f"WHERE pipeline_id = '{pipeline_id}'",
        warehouse_id, profile,
    )
    current = int(out.get("result", {}).get("data_array", [["0"]])[0][0])
    return current + 1


def _write_job_spec(pipeline_id, spec, spec_version, verifier_result, uncertainties, warehouse_id, profile):
    def esc(s: str) -> str:
        return s.replace("'", "''")

    statement = f"""
    INSERT INTO {CATALOG}.{SCHEMA}.job_spec
    (pipeline_id, spec_json, spec_version, verifier_result, uncertainties, updated_at)
    VALUES (
      '{esc(pipeline_id)}',
      '{esc(json.dumps(spec))}',
      {spec_version},
      '{esc(json.dumps(verifier_result))}',
      '{esc(json.dumps(uncertainties))}',
      current_timestamp()
    )
    """
    _sql_execute(statement.strip(), warehouse_id, profile)


def spec_and_verify_one(pipeline_id: str, pipeline_dir: str, profile: str, warehouse_id: str) -> dict:
    raw_files = read_raw_files(pipeline_dir, profile=profile)
    if not raw_files:
        raise RuntimeError(f"No raw files found under {pipeline_dir}")

    spec_result = run_spec_agent(pipeline_id, raw_files, profile=profile)
    spec = spec_result["spec"]

    verifier_result = None
    for round_num in range(1, MAX_SPEC_ROUNDS + 1):
        verifier_result = run_verifier_agent(pipeline_id, raw_files, spec, profile=profile)["result"]
        if verifier_result.get("approved"):
            break
        if round_num < MAX_SPEC_ROUNDS:
            # Regenerate affected parts: for this driver, simplest correct
            # behavior is a full re-run with the verifier's findings folded
            # into the spec agent's input as extra context.
            raw_files_with_feedback = dict(raw_files)
            raw_files_with_feedback["__verifier_feedback__.txt"] = (
                "Previous spec round was not approved. Verifier findings:\n"
                + json.dumps(verifier_result, indent=2)
                + "\nFix these specific issues in this round."
            )
            spec_result = run_spec_agent(pipeline_id, raw_files_with_feedback, profile=profile)
            spec = spec_result["spec"]

    spec_uncertain = not (verifier_result or {}).get("approved", False)
    spec_version = _next_spec_version(pipeline_id, warehouse_id, profile)
    _write_job_spec(
        pipeline_id, spec, spec_version, verifier_result, spec.get("uncertainties", []),
        warehouse_id, profile,
    )

    return {
        "pipeline_id": pipeline_id,
        "spec_version": spec_version,
        "spec": spec,
        "verifier_result": verifier_result,
        "spec_uncertain": spec_uncertain,
    }


def print_summary(result: dict) -> None:
    spec = result["spec"]
    print(f"\n=== {result['pipeline_id']} (spec_version {result['spec_version']}) ===")
    print(f"complexity_score: {spec.get('complexity_score')} -- {spec.get('complexity_rationale')}")
    print(f"component_inventory: {json.dumps(spec.get('component_inventory', {}))}")
    print(f"java_blocks: {len(spec.get('java_blocks', []))}")
    print(f"uncertainties: {spec.get('uncertainties', [])}")
    vr = result["verifier_result"] or {}
    print(f"verifier approved: {vr.get('approved')}")
    if not vr.get("approved"):
        print(f"  missing: {vr.get('missing')}")
        print(f"  wrong: {vr.get('wrong')}")
        print(f"  extra: {vr.get('extra')}")
    print(f"spec_uncertain flag: {result['spec_uncertain']}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="dev")
    p.add_argument("--pipeline-dir", action="append", required=True, help="repeatable")
    args = p.parse_args()

    warehouse_id = _pick_warehouse(args.profile)
    for pipeline_dir in args.pipeline_dir:
        pipeline_id = pipeline_dir.rstrip("/").rsplit("/", 1)[-1]
        result = spec_and_verify_one(pipeline_id, pipeline_dir, args.profile, warehouse_id)
        print_summary(result)


if __name__ == "__main__":
    main()
