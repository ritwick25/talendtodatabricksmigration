"""Wires one job's full pipeline (spec -> verify -> javaroutine ->
codegen -> checks -> review -> deploy -> run) into the generate/deploy
callables batch_runner.run_job_with_fix_cycles expects. This is where
the real agents and tools meet the plain-code fix-cycle logic -- the
decision logic itself (batch_runner.py) has zero knowledge of any of
this and is unit-tested separately with fakes.
"""
from __future__ import annotations

import json
import uuid

from src.agents.code_review_agent import MAX_REVIEW_ROUNDS, review_code
from src.agents.codegen_agent import FALLBACK_MODEL, PRIMARY_MODEL, generate_code
from src.agents.javaroutine_agent import translate_java_block
from src.agents.spec_agent import run_spec_agent
from src.agents.verifier_agent import MAX_SPEC_ROUNDS, run_verifier_agent
from src.orchestration.batch_runner import JobOutcome, RunResult, run_job_with_fix_cycles
from src.tools.bundle_tools import _classify_error_text, deploy_dev, generate_bundle, run_job
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


def make_generate_code_fn(pipeline_id, spec, translated_java_blocks, profile, warehouse_id):
    fix_cycle_counter = {"n": 0}

    def generate_code_fn(previous_code, previous_error):
        fix_cycle = fix_cycle_counter["n"]
        fix_cycle_counter["n"] += 1
        result = generate_code(
            spec, translated_java_blocks, fix_cycle=fix_cycle,
            previous_code=previous_code, previous_error=previous_error, profile=profile,
        )
        log_attempt(pipeline_id, "CODEGEN", result["model_endpoint"], {"fix_cycle": fix_cycle}, warehouse_id, profile)
        return json.dumps({"code": result["code"], "libraries": result.get("bundle_resources", {}).get("libraries", [])})

    return generate_code_fn


def make_deploy_and_run_fn(pipeline_id, translated_java_blocks, profile, warehouse_id, run_timeout_min):
    def deploy_and_run_fn(code_payload: str) -> RunResult:
        payload = json.loads(code_payload)
        code, libraries = payload["code"], payload["libraries"]

        syntax_ok, syntax_err = check_syntax(code)
        if not syntax_ok:
            return RunResult(False, False, f"syntax error: {syntax_err}", None)

        secrets_ok, secret_findings = scan_for_secrets(code)
        if not secrets_ok:
            return RunResult(False, False, f"secret(s) detected: {secret_findings}", None)

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
            return RunResult(False, False, f"CODE REVIEW blocked (high severity): {json.dumps(high_findings)}", None)

        bundle_dir = generate_bundle(pipeline_id, code, libraries=libraries)
        deploy_result = deploy_dev(pipeline_id, profile=profile)
        if not deploy_result.success:
            is_infra = _classify_error_text(deploy_result.output)
            return RunResult(False, is_infra, deploy_result.output[-3000:], deploy_result.output)

        run_result = run_job(pipeline_id, profile=profile, timeout_min=run_timeout_min)
        return run_result

    return deploy_and_run_fn


def process_job(pipeline_id: str, profile: str, warehouse_id: str, max_fix_cycles: int, run_timeout_min: int) -> JobOutcome:
    pipeline_dir = f"{VOLUME_SAMPLES}/{pipeline_id}"
    raw_files = read_raw_files(pipeline_dir, profile=profile)
    spec, spec_uncertain = produce_spec(pipeline_id, raw_files, profile, warehouse_id)
    translated_java_blocks = translate_all_java_blocks(pipeline_id, raw_files, spec, profile, warehouse_id)

    generate_code_fn = make_generate_code_fn(pipeline_id, spec, translated_java_blocks, profile, warehouse_id)
    deploy_and_run_fn = make_deploy_and_run_fn(pipeline_id, translated_java_blocks, profile, warehouse_id, run_timeout_min)

    outcome = run_job_with_fix_cycles(pipeline_id, generate_code_fn, deploy_and_run_fn, max_fix_cycles=max_fix_cycles)
    outcome.spec_uncertain = spec_uncertain
    outcome.java_uncertain = any(b.get("translation_uncertain") for b in translated_java_blocks.values())
    return outcome
