"""SPEC agent: reads raw Talend export files and produces a structured
job spec JSON. See prompts/spec_agent.md for the exact contract. All
understanding happens inside the model call -- this module only handles
chunking large jobs, calling the model, parsing its JSON, and merging
chunk results. It never inspects or interprets Talend semantics itself.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.tools.model_client import query_model_json
from src.tools.read_raw_files import chunk_files
from src.tools.tracing import trace

PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "spec_agent.md"
PROMPT_VERSION = "2"


def _load_prompt() -> str:
    return PROMPT_PATH.read_text()


def _format_chunk(pipeline_id: str, chunk: dict[str, str]) -> str:
    parts = [f"pipeline_id: {pipeline_id}", ""]
    for path, content in chunk.items():
        parts.append(f"--- FILE: {path} ---")
        parts.append(content)
        parts.append("")
    return "\n".join(parts)


@trace("spec_agent.run_chunk")
def _run_chunk(pipeline_id: str, chunk: dict[str, str], model_endpoint: str, profile: str) -> dict:
    system_prompt = _load_prompt()
    user_content = _format_chunk(pipeline_id, chunk) + (
        "\n\nThis is a PARTIAL set of this job's files (it may have been "
        "split across multiple calls). Extract everything you can from "
        "just what's shown here; a later merge pass will reconcile "
        "cross-chunk references. Still emit the full JSON schema."
    )
    spec, response = query_model_json(system_prompt, user_content, model_endpoint, profile=profile)
    return {
        "spec": spec,
        "prompt_tokens": response.prompt_tokens,
        "completion_tokens": response.completion_tokens,
    }


@trace("spec_agent.merge")
def _merge_chunks(pipeline_id: str, partial_specs: list[dict], model_endpoint: str, profile: str) -> dict:
    system_prompt = _load_prompt()
    user_content = (
        f"pipeline_id: {pipeline_id}\n\n"
        "You previously extracted these PARTIAL specs from different "
        "chunks of this same job's raw files (shown below as JSON). "
        "Merge them into the single final job spec: reconcile subjob "
        "boundaries, cross-subjob connections, globalMap puts/gets, and "
        "tRunJob/joblet parameter passing across chunks; de-duplicate any "
        "component that appeared in more than one chunk. Resolve "
        "conflicts by preferring the chunk with more detail, and note any "
        "unresolved conflict in uncertainties[]. Still emit the full JSON "
        "schema as the final answer.\n\n"
        + json.dumps(partial_specs, indent=2)
    )
    spec, response = query_model_json(system_prompt, user_content, model_endpoint, profile=profile)
    return {
        "spec": spec,
        "prompt_tokens": response.prompt_tokens,
        "completion_tokens": response.completion_tokens,
    }


@trace("spec_agent.run")
def run_spec_agent(
    pipeline_id: str,
    raw_files: dict[str, str],
    model_endpoint: str = "databricks-claude-sonnet-5",
    fallback_model_endpoint: str = "databricks-claude-opus-5",
    profile: str = "dev",
) -> dict:
    """Produce a job spec for one pipeline. Returns {"spec": ..., "cost": {...}}.

    Chunks by file group when the raw file set is large, processes each
    chunk independently, then does one final merge pass -- the merge
    pass's output is the returned spec, never a single chunk's partial
    output.
    """
    chunks = chunk_files(raw_files)
    total_prompt_tokens = 0
    total_completion_tokens = 0

    if len(chunks) == 1:
        result = _run_chunk(pipeline_id, chunks[0], model_endpoint, profile)
        spec = result["spec"]
        total_prompt_tokens += result["prompt_tokens"]
        total_completion_tokens += result["completion_tokens"]
    else:
        partials = []
        for chunk in chunks:
            result = _run_chunk(pipeline_id, chunk, model_endpoint, profile)
            partials.append(result["spec"])
            total_prompt_tokens += result["prompt_tokens"]
            total_completion_tokens += result["completion_tokens"]
        merged = _merge_chunks(pipeline_id, partials, model_endpoint, profile)
        spec = merged["spec"]
        total_prompt_tokens += merged["prompt_tokens"]
        total_completion_tokens += merged["completion_tokens"]

    return {
        "spec": spec,
        "prompt_version": PROMPT_VERSION,
        "model_endpoint": model_endpoint,
        "fallback_model_endpoint": fallback_model_endpoint,
        "cost": {
            "prompt_tokens": total_prompt_tokens,
            "completion_tokens": total_completion_tokens,
        },
    }
