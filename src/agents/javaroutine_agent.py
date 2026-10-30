"""JAVAROUTINE agent: translates one Java block to Python per
prompts/javaroutine_agent.md, then unit-tests the translation against
that block's java_cases fixture BEFORE the full job runs. Mismatches
against JAVA_EXECUTED cases must be fixed within the orchestrator's
normal fix-cycle budget (plain code enforces that cap elsewhere, in
batch_runner.py); mismatches against DERIVED cases are logged but do not
block the run -- that distinction is enforced here, not left to the
model.
"""
from __future__ import annotations

import json
from pathlib import Path

import mlflow

from src.tools.model_client import parse_json_response, query_model

PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "javaroutine_agent.md"
PROMPT_VERSION = "1"


def _load_prompt() -> str:
    return PROMPT_PATH.read_text()


@mlflow.trace(name="javaroutine_agent.translate")
def translate_java_block(
    block_id: str,
    java_source: str,
    tags: list[str],
    context: dict,
    model_endpoint: str = "databricks-claude-sonnet-5",
    profile: str = "dev",
) -> dict:
    system_prompt = _load_prompt()
    user_content = (
        f"block_id: {block_id}\n"
        f"tags: {tags}\n"
        f"context: {json.dumps(context)}\n\n"
        f"JAVA SOURCE:\n{java_source}"
    )
    response = query_model(system_prompt, user_content, model_endpoint, profile=profile)
    result = parse_json_response(response.text)
    result["cost"] = {
        "prompt_tokens": response.prompt_tokens,
        "completion_tokens": response.completion_tokens,
    }
    return result


def run_unit_cases(python_code: str, java_case: dict, exec_namespace: dict | None = None) -> dict:
    """Execute python_code (must define a function matching the case's
    calling convention -- see each java_cases file's structure) against
    every case, comparing against expected_output.

    Returns {"total": n, "passed": n, "case_source": "...", "failures": [...]}.
    This is plain code: no model call decides pass/fail, only direct
    value comparison.
    """
    namespace = dict(exec_namespace or {})
    exec(python_code, namespace)  # noqa: S102 -- translated code, run in a sandboxed job context upstream
    func = namespace.get("run")
    if func is None:
        raise ValueError("Translated code must define a top-level `run(input: dict) -> dict` function")

    failures = []
    passed = 0
    for case in java_case.get("cases", []):
        try:
            actual = func(case["input"])
        except Exception as e:  # noqa: BLE001 -- we want to record any failure, not crash the batch
            failures.append({"case_id": case["case_id"], "error": str(e)})
            continue
        expected = {k: v for k, v in case["expected_output"].items() if not k.startswith("_")}
        actual_filtered = {k: v for k, v in actual.items() if k in expected}
        if actual_filtered == expected:
            passed += 1
        else:
            failures.append({
                "case_id": case["case_id"],
                "expected": expected,
                "actual": actual_filtered,
            })

    return {
        "total": len(java_case.get("cases", [])),
        "passed": passed,
        "case_source": java_case.get("source"),
        "failures": failures,
    }
