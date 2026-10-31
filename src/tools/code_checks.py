"""Plain-code checks on generated code: syntax validity and secret
scanning. No LLM involved -- these are the two automated gates every
generated module passes through before the CODE REVIEW agent even sees
it (a module that doesn't compile or contains a literal secret is
rejected immediately, cheaper than spending a review call on it).
"""
from __future__ import annotations

import py_compile
import re
import shutil
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


# Fallback patterns for when the gitleaks binary isn't on PATH (it isn't,
# inside a Databricks Jobs serverless Python task -- confirmed by an
# actual job run failing with FileNotFoundError: gitleaks). Covers the
# same high-confidence categories gitleaks' default ruleset flags most
# often: cloud provider keys, common vendor token prefixes, private key
# blocks, and a generic high-entropy "KEY = '...'" assignment. Not a
# full gitleaks replacement -- local dev and CI still run the real
# gitleaks (more rules, lower false-negative rate); this fallback only
# covers the in-job runtime check.
_SECRET_PATTERNS = [
    ("AWS access key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("AWS secret key assignment", re.compile(r"(?i)aws_secret_access_key\s*=\s*['\"][A-Za-z0-9/+=]{40}['\"]")),
    ("Private key block", re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")),
    ("GitHub token", re.compile(r"gh[pousr]_[A-Za-z0-9]{36,}")),
    ("Slack token", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
    ("OpenAI/Anthropic-style API key", re.compile(r"sk-[A-Za-z0-9]{20,}")),
    ("JWT", re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")),
    (
        "Generic high-entropy secret assignment",
        re.compile(r"(?i)\b(secret|password|passwd|api_key|apikey|token)\s*=\s*['\"][A-Za-z0-9+/_=-]{20,}['\"]"),
    ),
]


def _scan_for_secrets_fallback(code: str) -> tuple[bool, list[str]]:
    findings = []
    for description, pattern in _SECRET_PATTERNS:
        if pattern.search(code):
            findings.append(description)
    return (not findings), findings


def scan_for_secrets(code: str) -> tuple[bool, list[str]]:
    """Runs gitleaks against the code (as a temp file) if the binary is
    available; otherwise falls back to _scan_for_secrets_fallback's
    regex patterns. Returns (clean, findings)."""
    if shutil.which("gitleaks") is None:
        return _scan_for_secrets_fallback(code)

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
