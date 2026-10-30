"""CODE REVIEW agent: reviews one generated module for forbidden APIs,
embedded secrets, unsafe I/O, missing Java blocks, and obvious bugs.
See prompts/code_review_agent.md. Runs after the plain-code py_compile
+ secret-scan checks (src/tools/code_checks.py) -- those are free and
catch the cheap cases before spending a review call.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.tools.model_client import query_model_json
from src.tools.tracing import trace

PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "code_review_agent.md"
PROMPT_VERSION = "1"

MAX_REVIEW_ROUNDS = 3  # per spec guardrail #5 -- plain-code enforced, not agent-decided


@trace("code_review_agent.review")
def review_code(
    code: str,
    expected_java_block_ids: list[str],
    model_endpoint: str = "databricks-claude-sonnet-5",
    profile: str = "dev",
) -> dict:
    system_prompt = PROMPT_PATH.read_text()
    user_content = (
        f"expected_java_block_ids (every one of these must appear somewhere in the code): "
        f"{json.dumps(expected_java_block_ids)}\n\nCODE:\n{code}"
    )
    result, response = query_model_json(system_prompt, user_content, model_endpoint, profile=profile)
    result["prompt_version"] = PROMPT_VERSION
    result["cost"] = {
        "prompt_tokens": response.prompt_tokens,
        "completion_tokens": response.completion_tokens,
    }
    return result
