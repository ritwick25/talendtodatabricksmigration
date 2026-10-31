"""Plain-code tools: deploy each migrated job as its own Databricks Job
and run it to a terminal state. No Talend/Java understanding here --
code and library paths are handed in fully formed.

Uses raw Jobs API 2.1 REST calls via WorkspaceClient.api_client.do()
(plain dict bodies) rather than the SDK's typed dataclasses
(jobs.create(tasks=[Task(...)]), compute.Environment, ...) or a
`databricks` CLI subprocess. Two real failures forced this, in order:
(1) the CLI binary isn't present inside a Databricks Jobs serverless
Python task (empty-stdout JSONDecodeError); (2) that same environment
ships an OLDER pre-installed databricks-sdk that doesn't even have
`compute.Environment`/`Library` as classes (ImportError), so typed SDK
objects aren't safe to construct from code that itself runs inside a
job. Raw REST dicts sidestep both: no CLI, no SDK version coupling --
the same fix already proven for model_client.py's thinking-disabled
payload earlier in this project.

Each pipeline_id gets its own job (named mig_<pipeline_id>) so parallel
deploys never collide; deploy_dev updates that job in place on every
call rather than creating a new one each time.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import base64

from databricks.sdk import WorkspaceClient

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_GENERATED_ROOT = "/Workspace/Shared/talend_migration/generated"


def generate_bundle(pipeline_id: str, code: str, libraries: list[str] | None = None) -> Path:
    """Writes generated/<pipeline_id>/src/job.py locally too (git
    history / local debugging) -- but this is no longer what gets
    deployed; deploy_dev uploads directly to the workspace. Returns the
    local directory path for backward compatibility with callers that
    log it.
    """
    bundle_dir = REPO_ROOT / "generated" / pipeline_id
    (bundle_dir / "src").mkdir(parents=True, exist_ok=True)
    (bundle_dir / "src" / "job.py").write_text(code)
    return bundle_dir


def _clean_libraries(libraries: list[str] | None) -> list[str]:
    # CODEGEN's bundle_resources.libraries is meant to be real jar paths
    # only, but has been observed putting prose explanations there
    # instead (e.g. "external library, jar not identified; reimplemented
    # natively") when it decided no jar was actually needed. Silently
    # dropping anything that isn't a plausible Volume jar path is safer
    # than trusting free text as a job resource field.
    return [lib for lib in (libraries or []) if lib.endswith(".jar") and " " not in lib]


@dataclass
class DeployResult:
    success: bool
    output: str
    job_id: int | None = None


def deploy_dev(pipeline_id: str, code: str | None = None, libraries: list[str] | None = None, profile: str = "dev") -> DeployResult:
    """Uploads code to the workspace and creates/updates the Databricks
    Job pointing at it. `code` is optional for backward compatibility
    with callers that already wrote it via generate_bundle() to the
    local path -- but a direct code upload is the real deploy path now,
    so prefer passing it explicitly.
    """
    try:
        client = WorkspaceClient(profile=profile)
        if code is None:
            code = (REPO_ROOT / "generated" / pipeline_id / "src" / "job.py").read_text()

        workspace_dir = f"{WORKSPACE_GENERATED_ROOT}/{pipeline_id}"
        client.api_client.do("POST", "/api/2.0/workspace/mkdirs", body={"path": workspace_dir})
        code_path = f"{workspace_dir}/job.py"
        client.api_client.do("POST", "/api/2.0/workspace/import", body={
            "path": code_path,
            "content": base64.b64encode(code.encode()).decode(),
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
        })

        job_name = f"mig_{pipeline_id}"
        task = {
            "task_key": "main",
            "spark_python_task": {"python_file": code_path},
            "environment_key": "serverless",
        }
        libs = [{"jar": lib} for lib in _clean_libraries(libraries)]
        if libs:
            task["libraries"] = libs
        job_settings = {
            "name": job_name,
            "tasks": [task],
            "environments": [{"environment_key": "serverless", "spec": {"client": "1", "dependencies": []}}],
        }

        existing = client.api_client.do("GET", "/api/2.1/jobs/list", query={"name": job_name})
        jobs_found = existing.get("jobs", [])
        if jobs_found:
            job_id = int(jobs_found[0]["job_id"])
            client.api_client.do("POST", "/api/2.1/jobs/reset", body={"job_id": job_id, "new_settings": job_settings})
        else:
            created = client.api_client.do("POST", "/api/2.1/jobs/create", body=job_settings)
            job_id = int(created["job_id"])

        return DeployResult(True, f"deployed {job_name} (job_id={job_id}) -> {code_path}", job_id)
    except Exception as e:
        return DeployResult(False, str(e), None)


@dataclass
class RunResult:
    success: bool
    is_infra_error: bool
    error: str | None
    logs: str | None
    run_id: str | None = None


INFRA_ERROR_MARKERS = (
    "QUOTA_EXCEEDED", "CLUSTER_CREATE_FAILURE", "NODE_TYPE_NOT_AVAILABLE",
    "NETWORK", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED",
    "cloud provider", "temporarily unavailable",
)


def _classify_error_text(text: str) -> bool:
    """Plain-code heuristic: infra vs code error. The orchestrator may
    later swap this for an LLM classify_error call (per spec, its output
    only routes -- counters always stay in plain code regardless). Note:
    deliberately does NOT match bare "INTERNAL_ERROR" -- that's the
    generic Databricks terminal run-result status for almost any task
    failure (missing secret, bad import, ...), not a reliable infra
    signal on its own; a real run already demonstrated this
    misclassifying a missing-secret error as infra.
    """
    return any(marker.lower() in text.lower() for marker in INFRA_ERROR_MARKERS)


def run_job(job_id: int, profile: str = "dev", timeout_min: int = 30) -> RunResult:
    """Runs the given job (by numeric job_id, as returned by deploy_dev)
    and polls (raw REST, same reasoning as deploy_dev) until it reaches
    a terminal state."""
    client = WorkspaceClient(profile=profile)
    try:
        run_now_resp = client.api_client.do("POST", "/api/2.1/jobs/run-now", body={"job_id": job_id})
        run_id = int(run_now_resp["run_id"])
    except Exception as e:
        return RunResult(False, _classify_error_text(str(e)), str(e), None)

    deadline = time.time() + timeout_min * 60
    run = None
    while time.time() < deadline:
        run = client.api_client.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id})
        life_cycle_state = run.get("state", {}).get("life_cycle_state")
        if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(10)
    else:
        return RunResult(False, True, f"run exceeded {timeout_min}min timeout", None, str(run_id))

    state = run.get("state", {})
    success = state.get("result_state") == "SUCCESS"
    if success:
        return RunResult(True, False, None, None, str(run_id))

    error_parts = [state.get("state_message", "no state message")]
    for task in run.get("tasks", []):
        try:
            output = client.api_client.do("GET", "/api/2.1/jobs/runs/get-output", query={"run_id": task["run_id"]})
            if output.get("error"):
                error_parts.append(f"{task.get('task_key')}: {output['error']}")
        except Exception:
            pass
    error_text = "\n".join(error_parts)
    return RunResult(False, _classify_error_text(error_text), error_text, error_text, str(run_id))
