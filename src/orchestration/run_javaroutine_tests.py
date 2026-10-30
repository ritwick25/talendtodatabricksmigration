"""Phase 4 driver: translate real Java blocks from the sample jobs and
unit-test each translation against its java_cases fixture BEFORE any
full job run -- exactly the order the spec requires. Writes results to
migration.java_block_log and prints pass/fail per case + case source.

Usage:
    python3 -m src.orchestration.run_javaroutine_tests --profile dev
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.agents.javaroutine_agent import run_unit_cases, translate_java_block
from src.tools.read_raw_files import extract_java_source, read_raw_files
from src.tools.sql_exec import esc, exec_sql, pick_warehouse

REPO_ROOT = Path(__file__).resolve().parents[2]
VOLUME_SAMPLES = "/Volumes/talend_migration/samples/talend_export/samples"

# A representative, non-tJavaFlex selection (simple per-row calling
# convention) across distinct patterns, for this first real Phase 4 run.
BLOCKS_TO_TEST = [
    ("job_01_customer_enrichment", "tJavaRow_1", ["STATEFUL", "NUMERIC", "EXTERNAL_LIB"]),
    ("job_12_db_upsert_truncate", "tJavaRow_1", ["STATEFUL", "TALEND_INTERNALS", "EXTERNAL_LIB"]),
    ("job_15_checksum_reconciliation", "tJavaRow_1", ["NUMERIC", "EXTERNAL_LIB"]),
    ("job_17_regex_string_parser", "tJavaRow_1", ["STRING-HEAVY"]),
    ("job_19_control_flow_recursive", "tJavaRow_1", ["CONTROL FLOW"]),
]


def write_java_block_log(pipeline_id, block_id, tags, tier, passed, total, case_source, warehouse_id, profile):
    statement = f"""
    INSERT INTO talend_migration.migration.java_block_log
    (pipeline_id, block_id, tags, translation_tier, unit_cases_passed, unit_cases_total, case_source, updated_at)
    VALUES ('{esc(pipeline_id)}', '{esc(block_id)}', '{esc(json.dumps(tags))}', '{esc(tier)}',
            {passed}, {total}, '{esc(case_source or "UNKNOWN")}', current_timestamp())
    """
    exec_sql(statement.strip(), warehouse_id, profile)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="dev")
    args = p.parse_args()
    warehouse_id = pick_warehouse(args.profile)

    for pipeline_id, block_id, tags in BLOCKS_TO_TEST:
        print(f"\n=== {pipeline_id} :: {block_id} ===")
        raw_files = read_raw_files(f"{VOLUME_SAMPLES}/{pipeline_id}", profile=args.profile)
        java_source = extract_java_source(raw_files, block_id)

        case_path = REPO_ROOT / "samples" / "talend_export_src" / "java_cases" / f"{pipeline_id}_{block_id}.json"
        java_case = json.loads(case_path.read_text())

        translation = translate_java_block(block_id, java_source, tags, context={}, profile=args.profile)
        print(f"translation_tier: {translation['translation_tier']}")
        print(f"rationale: {translation['rationale'][:200]}...")

        try:
            test_result = run_unit_cases(translation["python_code"], java_case)
        except Exception as e:
            print(f"EXECUTION ERROR: {e}")
            test_result = {"total": len(java_case.get("cases", [])), "passed": 0,
                            "case_source": java_case.get("source"), "failures": [{"error": str(e)}]}

        print(f"unit cases: {test_result['passed']}/{test_result['total']} passed "
              f"(source: {test_result['case_source']})")
        if test_result["failures"]:
            print(f"failures: {json.dumps(test_result['failures'], indent=2, default=str)[:1000]}")

        write_java_block_log(
            pipeline_id, block_id, tags, translation["translation_tier"],
            test_result["passed"], test_result["total"], test_result["case_source"],
            warehouse_id, args.profile,
        )


if __name__ == "__main__":
    main()
