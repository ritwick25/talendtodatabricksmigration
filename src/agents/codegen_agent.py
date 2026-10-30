"""CODEGEN/FIXER agent: spec + translated Java blocks -> a full
PySpark module; on a fix pass, previous code + exact error/logs ->
minimal targeted change. See prompts/codegen_agent.md for the contract.
Code-hash dedup and the 2-fix-cycle cap live in
src/orchestration/batch_runner.py, not here.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.tools.model_client import query_model_json
from src.tools.tracing import trace

PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "codegen_agent.md"
PROMPT_VERSION = "1"

PRIMARY_MODEL = "databricks-claude-sonnet-5"
FALLBACK_MODEL = "databricks-claude-opus-5"  # used automatically on fix_cycle 2


def _load_prompt() -> str:
    return PROMPT_PATH.read_text()


@trace("codegen_agent.generate")
def generate_code(
    spec: dict,
    translated_java_blocks: dict[str, dict],
    fix_cycle: int = 0,
    previous_code: str | None = None,
    previous_error: str | None = None,
    error_playbook_hits: list[dict] | None = None,
    profile: str = "dev",
) -> dict:
    """fix_cycle 0 = first pass. fix_cycle 1 uses PRIMARY_MODEL, fix_cycle
    2 automatically switches to FALLBACK_MODEL per spec rule 3 ("Uses the
    stronger fallback model on fix cycle 2")."""
    model_endpoint = FALLBACK_MODEL if fix_cycle >= 2 else PRIMARY_MODEL
    system_prompt = _load_prompt()

    parts = [f"fix_cycle: {fix_cycle}", f"job spec:\n{json.dumps(spec, indent=2)}"]
    parts.append(f"translated Java blocks (by block_id):\n{json.dumps(translated_java_blocks, indent=2)}")
    if previous_code is not None:
        parts.append(f"PREVIOUS CODE (must produce something different from this):\n{previous_code}")
    if previous_error is not None:
        parts.append(f"EXACT ERROR FROM PREVIOUS RUN:\n{previous_error}")
    if error_playbook_hits:
        parts.append(f"matching error_playbook entries:\n{json.dumps(error_playbook_hits, indent=2)}")

    result, response = query_model_json(system_prompt, "\n\n".join(parts), model_endpoint, profile=profile)
    result["model_endpoint"] = model_endpoint
    result["prompt_version"] = PROMPT_VERSION
    result["cost"] = {
        "prompt_tokens": response.prompt_tokens,
        "completion_tokens": response.completion_tokens,
    }
    return result
