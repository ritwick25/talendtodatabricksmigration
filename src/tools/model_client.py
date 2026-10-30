"""Plain-code wrapper for calling Databricks Model Serving chat endpoints.

No understanding of Talend/Java happens here -- this module only sends
messages to a serving endpoint and reports back token usage so callers can
record cost. Agents (spec_agent.py, verifier_agent.py, ...) own the prompts
and the interpretation of responses.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole


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
    max_tokens: int = 8000,
) -> ModelResponse:
    """Send one chat completion request to a Databricks serving endpoint.

    Raises on any transport/auth error -- callers decide whether that is an
    infra failure (per the orchestrator's classify_error) or something to
    surface directly. No `temperature` param: the Claude models served
    here reject it (BAD_REQUEST), so determinism is left to the prompt.

    Uses generous HTTP/retry timeouts: these are reasoning models given a
    whole job's raw files, and the SDK's 60s-per-attempt default is too
    short for that, even though the call does eventually succeed.
    """
    config = Config(profile=profile, http_timeout_seconds=600, retry_timeout_seconds=900)
    client = WorkspaceClient(config=config)
    response = client.serving_endpoints.query(
        name=model_endpoint,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
            ChatMessage(role=ChatMessageRole.USER, content=user_content),
        ],
        max_tokens=max_tokens,
    )
    choice = response.choices[0]
    text = _extract_text(choice.message.content)
    usage = response.usage
    return ModelResponse(
        text=text,
        prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
        model_endpoint=model_endpoint,
    )


def _extract_text(content) -> str:
    """Some models return message content as a plain string; reasoning
    models (as served here) return a list of typed blocks instead (e.g.
    {"type": "reasoning", ...} + {"type": "text", "text": "..."}). Join
    just the text blocks -- the reasoning block's content is opaque and
    not meant to be parsed."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            block.get("text", "") for block in content if block.get("type") == "text"
        )
    raise TypeError(f"Unexpected message content type: {type(content)}")


_TRAILING_COMMA_RE = re.compile(r",(\s*[}\]])")


def parse_json_response(text: str) -> dict:
    """Agents are prompted to emit only JSON, but strip code fences if a
    model wraps its answer in ```json ... ``` anyway, and repair the one
    malformation that shows up occasionally in practice (a trailing
    comma before a closing brace/bracket) before giving up.

    Raises json.JSONDecodeError (with the full text attached as
    `.response_text`) if the response still isn't valid JSON -- callers
    that can re-prompt should catch this and retry with the error
    message fed back, rather than treating it as a semantic failure.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.split("```")[1]
        if stripped.startswith("json"):
            stripped = stripped[4:]
    stripped = stripped.strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        repaired = _TRAILING_COMMA_RE.sub(r"\1", stripped)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError as e:
            e.response_text = text  # noqa: B018 -- attached for callers/logging
            raise


def query_model_json(
    system_prompt: str,
    user_content: str,
    model_endpoint: str,
    profile: str = "dev",
    max_tokens: int = 8000,
    max_format_retries: int = 2,
) -> tuple[dict, ModelResponse]:
    """query_model + parse_json_response, but re-prompts (feeding the
    exact parse error back) if the model's output isn't valid JSON --
    this happens occasionally (trailing comma, stray prose) regardless
    of how firmly the prompt says "JSON only". This is a format retry,
    separate from and not counted against any semantic retry budget
    (spec rounds, fix cycles) the caller enforces elsewhere.
    """
    content = user_content
    last_response = None
    for attempt in range(max_format_retries + 1):
        last_response = query_model(system_prompt, content, model_endpoint, profile=profile, max_tokens=max_tokens)
        try:
            return parse_json_response(last_response.text), last_response
        except json.JSONDecodeError as e:
            if attempt == max_format_retries:
                raise
            content = (
                user_content
                + f"\n\nYour previous response was not valid JSON: {e}. "
                "Return ONLY the corrected JSON, nothing else."
            )
    raise RuntimeError("unreachable")  # pragma: no cover
