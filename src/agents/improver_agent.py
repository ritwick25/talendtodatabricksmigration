"""IMPROVER agent: reads new attempt_log rows after a batch (or halt)
and proposes knowledge/prompt changes for recurring root causes. See
prompts/improver_agent.md. The 3-proposals-per-cycle cap and the
protected-path rejection are enforced here in plain code, never left to
the model.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.tools.model_client import query_model_json
from src.tools.tracing import trace

PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "improver_agent.md"
PROMPT_VERSION = "1"
MAX_PROPOSALS_PER_CYCLE = 3

PROTECTED_PREFIXES = (
    "src/orchestration/", "src/evaluation/", "eval/thresholds.yml",
    "databricks.yml", "resources/", ".github/workflows/", "CODEOWNERS",
)


def _is_protected(path: str) -> bool:
    return any(path.startswith(p) for p in PROTECTED_PREFIXES)


@trace("improver_agent.propose")
def propose_improvements(
    attempt_log_rows: list[dict],
    model_endpoint: str = "databricks-claude-opus-5",
    profile: str = "dev",
) -> list[dict]:
    """Returns at most MAX_PROPOSALS_PER_CYCLE proposals, with any
    proposal targeting a protected path dropped (and logged) rather
    than silently allowed through."""
    system_prompt = PROMPT_PATH.read_text()
    user_content = f"attempt_log rows since the last cycle:\n{json.dumps(attempt_log_rows, indent=2, default=str)}"
    result, _response = query_model_json(system_prompt, user_content, model_endpoint, profile=profile)

    proposals = result.get("proposals", [])
    safe_proposals = []
    for proposal in proposals:
        target = proposal.get("target_path", "")
        if _is_protected(target):
            print(f"REJECTED improvement proposal targeting protected path: {target}")
            continue
        safe_proposals.append(proposal)

    return safe_proposals[:MAX_PROPOSALS_PER_CYCLE]
