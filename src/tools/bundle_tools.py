"""Plain-code tools: deploy each migrated job as its own Databricks Job
(via the SDK's Jobs API directly, not a `databricks bundle` CLI
subprocess -- that CLI binary isn't present inside a Databricks Jobs
serverless Python task, confirmed by an actual job run failing with an
empty-stdout JSONDecodeError when this module still shelled out to it),
and run it to a terminal state. No Talend/Java understanding here --
code and library paths are handed in fully formed.

Each pipeline_id gets its own job (named mig_<pipeline_id>) so parallel
deploys never collide; deploy_dev updates that job in place on every
call rather than creating a new one each time.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment, Library
from databricks.sdk.service.jobs import JobEnvironment, JobSettings, SparkPythonTask, Task
from databricks.sdk.service.workspace import ImportFormat, Language

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
        client.workspace.mkdirs(workspace_dir)
        code_path = f"{workspace_dir}/job.py"
        client.workspace.upload(
            code_path, code.encode(), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True,
        )

        job_name = f"mig_{pipeline_id}"
        libs = [Library(jar=lib) for lib in _clean_libraries(libraries)]
        task = Task(
            task_key="main",
            spark_python_task=SparkPythonTask(python_file=code_path),
            environment_key="serverless",
            libraries=libs or None,
        )
        env = JobEnvironment(environment_key="serverless", spec=Environment(client="1", dependencies=[]))

        existing = list(client.jobs.list(name=job_name))
        if existing:
            job_id = existing[0].job_id
            client.jobs.reset(job_id, JobSettings(name=job_name, tasks=[task], environments=[env]))
        else:
            created = client.jobs.create(name=job_name, tasks=[task], environments=[env])
            job_id = created.job_id

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
    and blocks until it reaches a terminal state via the SDK's Wait.result(),
    which polls internally -- no manual polling loop needed here."""
    client = WorkspaceClient(profile=profile)
    try:
        waiter = client.jobs.run_now(job_id=job_id)
        run = waiter.result(timeout=timedelta(minutes=timeout_min))
    except TimeoutError as e:
        return RunResult(False, True, f"run exceeded {timeout_min}min timeout: {e}", None)
    except Exception as e:
        return RunResult(False, _classify_error_text(str(e)), str(e), None)

    run_id = str(run.run_id)
    state = run.state
    success = state and state.result_state and state.result_state.value == "SUCCESS"
    if success:
        return RunResult(True, False, None, None, run_id)

    # Pull the real per-task error (state_message alone is often just
    # "Workload failed, see run output for details").
    error_parts = [state.state_message if state else "no state"]
    for task in run.tasks or []:
        try:
            output = client.jobs.get_run_output(task.run_id)
            if output.error:
                error_parts.append(f"{task.task_key}: {output.error}")
        except Exception:
            pass
    error_text = "\n".join(error_parts)
    return RunResult(False, _classify_error_text(error_text), error_text, error_text, run_id)
