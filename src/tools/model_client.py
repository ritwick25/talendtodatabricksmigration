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

    Uses the SDK's low-level api_client.do() (raw JSON body) rather than
    the typed serving_endpoints.query(), for two reasons: (1) it needs
    to send `"thinking": {"type": "disabled"}` -- an Anthropic-specific
    field the typed method can't express (extra_params is Dict[str, str]
    only) -- because without it these reasoning models can spend the
    entire max_tokens budget on invisible reasoning and return an
    almost-empty answer; (2) it honors Config's http_timeout_seconds,
    unlike the `databricks` CLI subprocess (tried first), which has its
    own hardcoded ~60s inactivity timeout that's too short for a full
    job spec generation.
    """
    body = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "max_tokens": max_tokens,
    }
    if "claude" in model_endpoint:
        # "thinking" is an Anthropic-specific field -- other model
        # families served here (llama, gpt-oss, qwen, ...) reject it
        # outright with "unknown field", so only send it for Claude.
        body["thinking"] = {"type": "disabled"}

    config = Config(profile=profile, http_timeout_seconds=600, retry_timeout_seconds=900)
    client = WorkspaceClient(config=config)
    response = client.api_client.do(
        "POST", f"/serving-endpoints/{model_endpoint}/invocations", body=body,
    )
    choice = response["choices"][0]
    text = _extract_text(choice["message"]["content"])
    usage = response.get("usage", {})
    return ModelResponse(
        text=text,
        prompt_tokens=usage.get("prompt_tokens", 0) or 0,
        completion_tokens=usage.get("completion_tokens", 0) or 0,
        model_endpoint=model_endpoint,
    )


def _extract_text(content) -> str:
    """Normally a plain string with thinking disabled; defensively also
    handle the typed-block list shape in case a future call re-enables
    reasoning (join just the "text" blocks -- "reasoning" blocks are
    opaque and not meant to be parsed)."""
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
    attempts = (
        lambda s: json.loads(s),
        # strict=False permits raw control characters (unescaped literal
        # newlines/tabs) inside string values -- happens occasionally
        # when a model emits code containing real newlines instead of
        # \n inside a JSON string.
        lambda s: json.loads(s, strict=False),
        lambda s: json.loads(_TRAILING_COMMA_RE.sub(r"\1", s)),
        lambda s: json.loads(_TRAILING_COMMA_RE.sub(r"\1", s), strict=False),
    )
    last_error = None
    for attempt in attempts:
        try:
            return attempt(stripped)
        except json.JSONDecodeError as e:
            last_error = e
    last_error.response_text = text  # noqa: B018 -- attached for callers/logging
    raise last_error


def _looks_truncated(error: json.JSONDecodeError, text_length: int) -> bool:
    """An unterminated string, or an error position right at the end of
    the text, means generation ran out of room mid-object -- distinct
    from a trailing-comma-style slip, which happens well before the end."""
    return "Unterminated string" in str(error) or error.pos >= text_length - 5


MAX_OUTPUT_TOKENS = 8192  # hard server-side cap for the endpoints this system uses


def query_model_json(
    system_prompt: str,
    user_content: str,
    model_endpoint: str,
    profile: str = "dev",
    max_tokens: int = MAX_OUTPUT_TOKENS,
    max_format_retries: int = 2,
) -> tuple[dict, ModelResponse]:
    """query_model + parse_json_response, but re-prompts if the model's
    output isn't valid JSON. Two distinct failure modes, handled
    differently:
    - Truncation (an unterminated string/object because max_tokens ran
      out mid-generation, likely for a large job spec): doubles the
      budget each retry, up to MAX_OUTPUT_TOKENS (the endpoint's hard
      cap) -- if already at the cap, there's no more room to give, so
      instead asks the model to be more concise in free-text fields
      (uncertainties/rationale) rather than hoping for more space.
    - A genuine formatting slip (trailing comma, stray prose): feed the
      exact parse error back and ask for a correction at the same
      max_tokens.
    Either way this is a format retry, separate from and not counted
    against any semantic retry budget (spec rounds, fix cycles) the
    caller enforces elsewhere.
    """
    content = user_content
    current_max_tokens = min(max_tokens, MAX_OUTPUT_TOKENS)
    last_response = None
    for attempt in range(max_format_retries + 1):
        last_response = query_model(
            system_prompt, content, model_endpoint, profile=profile, max_tokens=current_max_tokens,
        )
        try:
            return parse_json_response(last_response.text), last_response
        except json.JSONDecodeError as e:
            if attempt == max_format_retries:
                raise
            if _looks_truncated(e, len(last_response.text)):
                if current_max_tokens < MAX_OUTPUT_TOKENS:
                    current_max_tokens = min(current_max_tokens * 2, MAX_OUTPUT_TOKENS)
                else:
                    content = (
                        user_content
                        + "\n\nYour previous response was truncated (ran out of output "
                        "space) before the JSON finished. Produce the SAME complete JSON "
                        "again, but more concisely in free-text fields (uncertainties, "
                        "rationale, descriptions) so the whole thing fits. Do not drop any "
                        "components, connections, or java_blocks to save space -- shorten "
                        "prose, not structure."
                    )
            else:
                content = (
                    user_content
                    + f"\n\nYour previous response was not valid JSON: {e}. "
                    "Return ONLY the corrected JSON, nothing else."
                )
    raise RuntimeError("unreachable")  # pragma: no cover
