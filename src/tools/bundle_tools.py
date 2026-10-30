"""Plain-code tools: write each migrated job as its own mini Databricks
Asset Bundle under generated/<pipeline_id>/ (so parallel deploys never
collide with each other or with the root bundle), deploy it to dev, and
run it to a terminal state. No Talend/Java understanding here -- code
and bundle resources are handed in fully formed.
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_HOST = "https://adb-7405617014325769.9.azuredatabricks.net"


def generate_bundle(pipeline_id: str, code: str, libraries: list[str] | None = None) -> Path:
    """Writes generated/<pipeline_id>/ as a standalone mini-bundle.
    Returns the bundle directory path.
    """
    bundle_dir = REPO_ROOT / "generated" / pipeline_id
    (bundle_dir / "src").mkdir(parents=True, exist_ok=True)
    (bundle_dir / "resources").mkdir(parents=True, exist_ok=True)

    (bundle_dir / "src" / "job.py").write_text(code)

    libraries = libraries or []
    lib_yaml = "\n".join(f"            - jar: {lib}" for lib in libraries)
    libraries_block = f"          libraries:\n{lib_yaml}\n" if libraries else ""

    job_resource = f"""resources:
  jobs:
    mig_{pipeline_id}:
      name: mig_{pipeline_id}
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ../src/job.py
{libraries_block}"""
    (bundle_dir / "resources" / "job.yml").write_text(job_resource)

    bundle_yaml = f"""bundle:
  name: mig_{pipeline_id}

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: {WORKSPACE_HOST}
"""
    (bundle_dir / "databricks.yml").write_text(bundle_yaml)
    return bundle_dir


@dataclass
class DeployResult:
    success: bool
    output: str


def deploy_dev(pipeline_id: str, profile: str = "dev") -> DeployResult:
    bundle_dir = REPO_ROOT / "generated" / pipeline_id
    result = subprocess.run(
        ["databricks", "bundle", "deploy", "-t", "dev", "--profile", profile],
        cwd=bundle_dir, capture_output=True, text=True,
    )
    return DeployResult(result.returncode == 0, result.stdout + result.stderr)


@dataclass
class RunResult:
    success: bool
    is_infra_error: bool
    error: str | None
    logs: str | None
    run_id: str | None = None


INFRA_ERROR_MARKERS = (
    "QUOTA_EXCEEDED", "CLUSTER_CREATE_FAILURE", "NODE_TYPE_NOT_AVAILABLE",
    "NETWORK", "INTERNAL_ERROR", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED",
    "cloud provider", "temporarily unavailable",
)


def _classify_error_text(text: str) -> bool:
    """Plain-code heuristic: infra vs code error. The orchestrator may
    later swap this for an LLM classify_error call (per spec, its output
    only routes -- counters always stay in plain code regardless)."""
    return any(marker.lower() in text.lower() for marker in INFRA_ERROR_MARKERS)


def run_job(pipeline_id: str, profile: str = "dev", timeout_min: int = 30) -> RunResult:
    """`databricks bundle run` blocks until the run reaches a terminal
    state and exits non-zero on any task failure, so no manual polling
    is needed here."""
    bundle_dir = REPO_ROOT / "generated" / pipeline_id
    try:
        result = subprocess.run(
            ["databricks", "bundle", "run", f"mig_{pipeline_id}", "-t", "dev", "--profile", profile],
            cwd=bundle_dir, capture_output=True, text=True, timeout=timeout_min * 60,
        )
    except subprocess.TimeoutExpired as e:
        text = (e.stdout or "") + (e.stderr or "")
        return RunResult(False, True, f"run exceeded {timeout_min}min timeout: {text}", text)

    text = result.stdout + result.stderr
    run_id = None
    for line in text.splitlines():
        if "/jobs/" in line and "/runs/" in line:
            run_id = line.strip().rstrip("/").rsplit("/", 1)[-1]

    success = result.returncode == 0
    return RunResult(success, (not success) and _classify_error_text(text), text if not success else None, text, run_id)
