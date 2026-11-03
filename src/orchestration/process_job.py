"""Wires one job's full pipeline (spec -> verify -> javaroutine ->
codegen -> checks -> review -> deploy -> run) into the generate/deploy
callables batch_runner.run_job_with_fix_cycles expects. This is where
the real agents and tools meet the plain-code fix-cycle logic -- the
decision logic itself (batch_runner.py) has zero knowledge of any of
this and is unit-tested separately with fakes.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

from src.agents.code_review_agent import MAX_REVIEW_ROUNDS, review_code
from src.agents.codegen_agent import FALLBACK_MODEL, PRIMARY_MODEL, generate_code
from src.agents.javaroutine_agent import translate_java_block
from src.agents.spec_agent import run_spec_agent
from src.agents.verifier_agent import MAX_SPEC_ROUNDS, run_verifier_agent
from src.orchestration.batch_runner import JobOutcome, RunResult, run_job_with_fix_cycles
from src.tools.bundle_tools import _classify_error_text, _extract_error_summary, deploy_dev, generate_bundle, run_job
from src.tools.code_checks import check_syntax, scan_for_secrets
from src.tools.read_raw_files import extract_java_source, read_raw_files
from src.tools.sql_exec import esc, exec_sql

VOLUME_SAMPLES = "/Volumes/talend_migration/samples/talend_export/samples"
CATALOG = "talend_migration"
SCHEMA = "migration"


def log_attempt(pipeline_id, agent, model, details, warehouse_id, profile, commit_sha="unknown", cost=0.0):
    statement = f"""
    INSERT INTO {CATALOG}.{SCHEMA}.attempt_log
    (attempt_id, pipeline_id, ts, agent, model, prompt_version, knowledge_version,
     git_sha, cost, error_signature, code_hash, injection_suspected, details)
    VALUES ('{uuid.uuid4()}', '{esc(pipeline_id)}', current_timestamp(), '{esc(agent)}',
            '{esc(model)}', '1', 'v0', '{esc(commit_sha)}', {cost}, NULL, NULL, false,
            '{esc(json.dumps(details)[:8000])}')
    """
    exec_sql(statement.strip(), warehouse_id, profile)


def _sql_array(values: list[str]) -> str:
    if not values:
        return "array()"
    return "array(" + ", ".join(f"'{esc(str(v))}'" for v in values) + ")"


def write_lineage(pipeline_id: str, spec: dict, warehouse_id: str, profile: str, spec_version: int = 1) -> None:
    """Writes the SPEC agent's lineage[] entries (source/target table and
    column info, read straight from the raw Talend XML's elementParameters --
    e.g. tDBOutput's TABLE/SCHEMA -- never guessed) into the lineage table.
    Used by the user's own separate data-validation process, not read back
    by this system."""
    for entry in spec.get("lineage", []):
        statement = f"""
        INSERT INTO {CATALOG}.{SCHEMA}.lineage
        (pipeline_id, spec_version, lineage_id, subjob_id, component_name, component_type,
         source_type, source_system, source_table, source_columns,
         target_type, target_system, target_table, target_columns,
         column_lineage, transformation_summary, confidence, commit_sha, created_at)
        VALUES ('{esc(pipeline_id)}', {spec_version}, '{uuid.uuid4()}', '{esc(str(entry.get("subjob_id", "")))}',
                '{esc(str(entry.get("component_name", "")))}', '{esc(str(entry.get("component_type", "")))}',
                '{esc(str(entry.get("source_type", "")))}', '{esc(str(entry.get("source_system", "")))}',
                '{esc(str(entry.get("source_table", "")))}', {_sql_array(entry.get("source_columns") or [])},
                '{esc(str(entry.get("target_type", "")))}', '{esc(str(entry.get("target_system", "")))}',
                '{esc(str(entry.get("target_table", "")))}', {_sql_array(entry.get("target_columns") or [])},
                '{esc(str(entry.get("column_lineage", "")))}', '{esc(str(entry.get("transformation_summary", "")))}',
                '{esc(str(entry.get("confidence", "")))}', 'unknown', current_timestamp())
        """
        exec_sql(statement.strip(), warehouse_id, profile)


def produce_spec(pipeline_id: str, raw_files: dict, profile: str, warehouse_id: str) -> tuple[dict, bool]:
    """Returns (spec, spec_uncertain). Regenerates on VERIFIER disapproval
    up to MAX_SPEC_ROUNDS -- spec rounds never consume a fix cycle."""
    spec_result = run_spec_agent(pipeline_id, raw_files, profile=profile)
    spec = spec_result["spec"]
    log_attempt(pipeline_id, "SPEC", spec_result["model_endpoint"], {"complexity_score": spec.get("complexity_score")}, warehouse_id, profile)

    verifier_result = None
    for round_num in range(1, MAX_SPEC_ROUNDS + 1):
        verifier_out = run_verifier_agent(pipeline_id, raw_files, spec, profile=profile)
        verifier_result = verifier_out["result"]
        log_attempt(pipeline_id, "VERIFIER", verifier_out["model_endpoint"], verifier_result, warehouse_id, profile)
        if verifier_result.get("approved"):
            return spec, False
        if round_num < MAX_SPEC_ROUNDS:
            feedback_files = dict(raw_files)
            feedback_files["__verifier_feedback__.txt"] = (
                "Previous spec round was not approved. Fix these specific issues:\n"
                + json.dumps(verifier_result, indent=2)
            )
            spec_result = run_spec_agent(pipeline_id, feedback_files, profile=profile)
            spec = spec_result["spec"]
            log_attempt(pipeline_id, "SPEC", spec_result["model_endpoint"], {"regenerated_for_verifier": True}, warehouse_id, profile)

    return spec, True  # ran out of spec rounds without approval -> spec_uncertain


def translate_all_java_blocks(pipeline_id: str, raw_files: dict, spec: dict, profile: str, warehouse_id: str) -> dict:
    translated = {}
    for block in spec.get("java_blocks", []):
        block_id = block["block_id"]
        if "routine" in block.get("location", "").lower() or (
            "." in block_id and not block_id.split(".")[0].startswith("t")
        ):
            continue  # routine-method reference, not an inline node -- out of scope here
        try:
            java_source = extract_java_source(raw_files, block_id)
        except ValueError:
            continue
        translation = translate_java_block(block_id, java_source, block.get("tags", []), context={}, profile=profile)
        translated[block_id] = translation
        log_attempt(pipeline_id, "JAVAROUTINE", PRIMARY_MODEL, {"block_id": block_id, "tier": translation["translation_tier"]}, warehouse_id, profile)
    return translated


def make_generate_code_fn(pipeline_id, spec, translated_java_blocks, profile, warehouse_id, timing):
    fix_cycle_counter = {"n": 0}

    def generate_code_fn(previous_code, previous_error):
        fix_cycle = fix_cycle_counter["n"]
        fix_cycle_counter["n"] += 1
        result = generate_code(
            spec, translated_java_blocks, fix_cycle=fix_cycle,
            previous_code=previous_code, previous_error=previous_error, profile=profile,
        )
        timing["codegen_completed_at"] = time.time()
        log_attempt(pipeline_id, "CODEGEN", result["model_endpoint"], {"fix_cycle": fix_cycle}, warehouse_id, profile)
        return json.dumps({"code": result["code"], "libraries": result.get("bundle_resources", {}).get("libraries", [])})

    return generate_code_fn


def make_deploy_and_run_fn(pipeline_id, translated_java_blocks, profile, warehouse_id, run_timeout_min, timing):
    def deploy_and_run_fn(code_payload: str) -> RunResult:
        payload = json.loads(code_payload)
        code, libraries = payload["code"], payload["libraries"]

        syntax_ok, syntax_err = check_syntax(code)
        if not syntax_ok:
            msg = f"syntax error: {syntax_err}"
            return RunResult(False, False, msg, None, error_summary=msg[:220])

        secrets_ok, secret_findings = scan_for_secrets(code)
        if not secrets_ok:
            msg = f"secret(s) detected: {secret_findings}"
            return RunResult(False, False, msg, None, error_summary=msg[:220])

        for _ in range(MAX_REVIEW_ROUNDS):
            review = review_code(code, list(translated_java_blocks.keys()), profile=profile)
            log_attempt(pipeline_id, "CODE_REVIEW", PRIMARY_MODEL, review, warehouse_id, profile)
            if review.get("approved"):
                break
            high_findings = [f for f in review.get("findings", []) if f.get("severity") == "high"]
            if not high_findings:
                break
            # High-severity-only findings fed back is a review round, not
            # a fix cycle -- but this driver has no separate CODEGEN call
            # for review rounds yet, so for now a persistent high finding
            # surfaces as a code-classified failure, consuming a real fix
            # cycle. Documented limitation: Phase 7 should close this gap.
            msg = f"CODE REVIEW blocked (high severity): {json.dumps(high_findings)}"
            top = high_findings[0]
            summary = f"CODE_REVIEW blocked: {top.get('category', 'finding')} -- {top.get('description', '')}"
            return RunResult(False, False, msg, None, error_summary=summary[:220])

        generate_bundle(pipeline_id, code)  # local copy for git history/debugging
        deploy_result = deploy_dev(pipeline_id, code=code, libraries=libraries, profile=profile)
        if not deploy_result.success:
            is_infra = _classify_error_text(deploy_result.output)
            return RunResult(
                False, is_infra, deploy_result.output[-3000:], deploy_result.output,
                error_summary=_extract_error_summary(deploy_result.output),
            )

        timing["run_started_at"] = time.time()
        run_result = run_job(deploy_result.job_id, profile=profile, timeout_min=run_timeout_min)
        timing["run_completed_at"] = time.time()
        return run_result

    return deploy_and_run_fn


def process_job(pipeline_id: str, profile: str, warehouse_id: str, max_fix_cycles: int, run_timeout_min: int) -> JobOutcome:
    timing = {"started_at": time.time()}
    queue_ops_update_started(pipeline_id, warehouse_id, profile, timing["started_at"])

    pipeline_dir = f"{VOLUME_SAMPLES}/{pipeline_id}"
    raw_files = read_raw_files(pipeline_dir, profile=profile)
    spec, spec_uncertain = produce_spec(pipeline_id, raw_files, profile, warehouse_id)
    write_lineage(pipeline_id, spec, warehouse_id, profile)
    translated_java_blocks = translate_all_java_blocks(pipeline_id, raw_files, spec, profile, warehouse_id)

    generate_code_fn = make_generate_code_fn(pipeline_id, spec, translated_java_blocks, profile, warehouse_id, timing)
    deploy_and_run_fn = make_deploy_and_run_fn(pipeline_id, translated_java_blocks, profile, warehouse_id, run_timeout_min, timing)

    outcome = run_job_with_fix_cycles(pipeline_id, generate_code_fn, deploy_and_run_fn, max_fix_cycles=max_fix_cycles)
    outcome.spec_uncertain = spec_uncertain
    outcome.java_uncertain = any(b.get("translation_uncertain") for b in translated_java_blocks.values())
    outcome.timing = compute_timing(timing)
    return outcome


def queue_ops_update_started(pipeline_id, warehouse_id, profile, started_at_epoch):
    ts = datetime.fromtimestamp(started_at_epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    exec_sql(
        f"UPDATE {CATALOG}.{SCHEMA}.queue SET started_at = '{ts}' WHERE pipeline_id = '{esc(pipeline_id)}'",
        warehouse_id, profile,
    )


def compute_timing(timing: dict) -> dict:
    """Real, measured durations -- never estimated. codegen_completed_at
    is the last successful codegen call before the deploy that actually
    ran; run_started_at/run_completed_at bracket the real Databricks job
    execution. Any key can be missing if the job failed before reaching
    that phase."""
    started_at = timing.get("started_at")
    codegen_completed_at = timing.get("codegen_completed_at")
    run_started_at = timing.get("run_started_at")
    run_completed_at = timing.get("run_completed_at")

    result = {}
    if started_at and codegen_completed_at:
        result["convert_duration_seconds"] = round(codegen_completed_at - started_at, 1)
    if run_started_at and run_completed_at:
        result["run_duration_seconds"] = round(run_completed_at - run_started_at, 1)
    if started_at:
        end = run_completed_at or time.time()
        result["total_duration_seconds"] = round(end - started_at, 1)
    return result
