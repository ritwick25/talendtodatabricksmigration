"""Plain-code checks on generated code: syntax validity and secret
scanning. No LLM involved -- these are the two automated gates every
generated module passes through before the CODE REVIEW agent even sees
it (a module that doesn't compile or contains a literal secret is
rejected immediately, cheaper than spending a review call on it).
"""
from __future__ import annotations

import py_compile
import subprocess
import tempfile
from pathlib import Path


def check_syntax(code: str) -> tuple[bool, str | None]:
    """Returns (ok, error). Writes to a temp file because py_compile
    needs a real path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        path = f.name
    try:
        py_compile.compile(path, doraise=True)
        return True, None
    except py_compile.PyCompileError as e:
        return False, str(e)
    finally:
        Path(path).unlink(missing_ok=True)


def scan_for_secrets(code: str) -> tuple[bool, list[str]]:
    """Runs gitleaks against the code (as a temp file). Returns
    (clean, findings). Requires `gitleaks` on PATH -- same tool used in
    pre-commit and CI, so a secret that slips past this check would also
    be caught there before merge.
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        path = f.name
    try:
        result = subprocess.run(
            ["gitleaks", "detect", "--no-git", "--source", path, "--report-format", "json",
             "--report-path", path + ".report.json", "--exit-code", "1"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            return True, []
        report_path = Path(path + ".report.json")
        if report_path.exists():
            import json
            findings = json.loads(report_path.read_text())
            report_path.unlink(missing_ok=True)
            return False, [f.get("Description", "secret detected") for f in findings]
        return False, ["gitleaks reported a finding but no report file was produced"]
    finally:
        Path(path).unlink(missing_ok=True)
