"""Phase 5 driver: CODEGEN + CODE REVIEW + generate_bundle + deploy_dev +
run_job on ONE complex-Java sample, end to end, for real. Not the full
orchestrator (no fix cycles here -- that's batch_runner.py, already
unit-tested, wired into the batch loop in Phase 6) -- this is the
single-pass proof that every stage actually works together.

Usage:
    python3 -m src.orchestration.run_phase5_e2e --profile dev --pipeline-id job_01_customer_enrichment
"""
from __future__ import annotations

import argparse
import json

from src.agents.code_review_agent import review_code
from src.agents.codegen_agent import generate_code
from src.agents.javaroutine_agent import translate_java_block
from src.agents.spec_agent import run_spec_agent
from src.agents.verifier_agent import run_verifier_agent
from src.tools.bundle_tools import deploy_dev, generate_bundle, run_job
from src.tools.code_checks import check_syntax, scan_for_secrets
from src.tools.read_raw_files import extract_java_source, read_raw_files

VOLUME_SAMPLES = "/Volumes/talend_migration/samples/talend_export/samples"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="dev")
    p.add_argument("--pipeline-id", required=True)
    args = p.parse_args()
    pipeline_id = args.pipeline_id
    pipeline_dir = f"{VOLUME_SAMPLES}/{pipeline_id}"

    print("=== 1. SPEC + VERIFIER ===")
    raw_files = read_raw_files(pipeline_dir, profile=args.profile)
    spec_result = run_spec_agent(pipeline_id, raw_files, profile=args.profile)
    spec = spec_result["spec"]
    verifier_result = run_verifier_agent(pipeline_id, raw_files, spec, profile=args.profile)["result"]
    print(f"complexity_score={spec.get('complexity_score')} verifier_approved={verifier_result.get('approved')}")

    print("\n=== 2. JAVAROUTINE (translate every Java block) ===")
    translated_java_blocks = {}
    for block in spec.get("java_blocks", []):
        block_id = block["block_id"]
        if "." in block_id and block_id.split(".")[0][0:1].isupper():
            # A routine method reference (e.g. "InventoryRoutines.computeDelta"),
            # not an inline node -- out of scope for this single-job driver;
            # the real pipeline translates routine files separately.
            print(f"  skipping routine reference {block_id} (not an inline node)")
            continue
        java_source = extract_java_source(raw_files, block_id)
        translation = translate_java_block(block_id, java_source, block.get("tags", []), context={}, profile=args.profile)
        translated_java_blocks[block_id] = translation
        print(f"  {block_id}: tier={translation['translation_tier']}")

    print("\n=== 3. CODEGEN ===")
    codegen_result = generate_code(spec, translated_java_blocks, fix_cycle=0, profile=args.profile)
    code = codegen_result["code"]
    print(f"generated {len(code.splitlines())} lines, model={codegen_result['model_endpoint']}")

    print("\n=== 4. plain-code checks ===")
    syntax_ok, syntax_err = check_syntax(code)
    print(f"syntax_ok={syntax_ok} err={syntax_err}")
    secrets_ok, secret_findings = scan_for_secrets(code)
    print(f"secrets_ok={secrets_ok} findings={secret_findings}")

    print("\n=== 5. CODE REVIEW ===")
    expected_block_ids = list(translated_java_blocks.keys())
    review_result = review_code(code, expected_block_ids, profile=args.profile)
    print(f"approved={review_result.get('approved')} findings={json.dumps(review_result.get('findings', []), indent=2)}")

    print("\n=== 6. generate_bundle + deploy_dev ===")
    libraries = codegen_result.get("bundle_resources", {}).get("libraries", [])
    bundle_dir = generate_bundle(pipeline_id, code, libraries=libraries)
    print(f"bundle written to {bundle_dir}")
    deploy_result = deploy_dev(pipeline_id, profile=args.profile)
    print(f"deploy success={deploy_result.success}")
    print(deploy_result.output[-3000:])
    if not deploy_result.success:
        return

    print("\n=== 7. run_job ===")
    run_result = run_job(pipeline_id, profile=args.profile, timeout_min=15)
    print(f"run success={run_result.success} is_infra_error={run_result.is_infra_error}")
    print((run_result.logs or "")[-5000:])


if __name__ == "__main__":
    main()
