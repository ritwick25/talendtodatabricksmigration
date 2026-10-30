"""Plain-code wrapper for calling Databricks Model Serving chat endpoints.

No understanding of Talend/Java happens here -- this module only sends
messages to a serving endpoint and reports back token usage so callers can
record cost. Agents (spec_agent.py, verifier_agent.py, ...) own the prompts
and the interpretation of responses.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient


@dataclass
class ModelResponse:
    text: str
    prompt_tokens: int
    completion_tokens: int
    model_endpoint: str


def query_model(
    system_prompt: str,
    user_content: str,
    model_endpoint: str,
    profile: str = "dev",
    temperature: float = 0.0,
    max_tokens: int = 8000,
) -> ModelResponse:
    """Send one chat completion request to a Databricks serving endpoint.

    Raises on any transport/auth error -- callers decide whether that is an
    infra failure (per the orchestrator's classify_error) or something to
    surface directly.
    """
    client = WorkspaceClient(profile=profile)
    response = client.serving_endpoints.query(
        name=model_endpoint,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    choice = response.choices[0]
    text = choice.message.content
    usage = response.usage
    return ModelResponse(
        text=text,
        prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
        model_endpoint=model_endpoint,
    )


def parse_json_response(text: str) -> dict:
    """Agents are prompted to emit only JSON, but strip code fences if a
    model wraps its answer in ```json ... ``` anyway."""
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.split("```")[1]
        if stripped.startswith("json"):
            stripped = stripped[4:]
    return json.loads(stripped.strip())
