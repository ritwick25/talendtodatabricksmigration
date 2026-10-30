"""VERIFIER agent: independently re-reads the same raw files and audits a
SPEC agent's job spec for omissions, hallucinations, wrong connection
types, and missed Java. Runs on a different model from the SPEC agent by
default -- see prompts/verifier_agent.md for the contract.
"""
from __future__ import annotations

import json
from pathlib import Path


from src.tools.model_client import query_model_json
from src.tools.tracing import trace

PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "verifier_agent.md"
PROMPT_VERSION = "1"

MAX_SPEC_ROUNDS = 2  # per spec guardrail #5 -- plain-code enforced cap, not agent-decided


def _load_prompt() -> str:
    return PROMPT_PATH.read_text()


@trace("verifier_agent.run")
def run_verifier_agent(
    pipeline_id: str,
    raw_files: dict[str, str],
    spec: dict,
    model_endpoint: str = "databricks-llama-4-maverick",
    profile: str = "dev",
) -> dict:
    """Returns {"result": {"approved", "missing", "wrong", "extra"}, "cost": {...}}."""
    system_prompt = _load_prompt()
    files_block = "\n\n".join(
        f"--- FILE: {path} ---\n{content}" for path, content in raw_files.items()
    )
    user_content = (
        f"pipeline_id: {pipeline_id}\n\n"
        f"RAW FILES:\n{files_block}\n\n"
        f"SPEC TO AUDIT (produced by a different agent/model):\n"
        f"{json.dumps(spec, indent=2)}"
    )
    result, response = query_model_json(system_prompt, user_content, model_endpoint, profile=profile)
    return {
        "result": result,
        "prompt_version": PROMPT_VERSION,
        "model_endpoint": model_endpoint,
        "cost": {
            "prompt_tokens": response.prompt_tokens,
            "completion_tokens": response.completion_tokens,
        },
    }
