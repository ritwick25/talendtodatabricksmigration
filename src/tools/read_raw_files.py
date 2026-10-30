"""Plain-code tool: read raw Talend export files from a Unity Catalog
Volume. Read-only -- this system never modifies anything under the Talend
export Volume. No parsing happens here; files are returned as raw text for
an LLM agent to read.
"""
from __future__ import annotations

from databricks.sdk import WorkspaceClient

# Component files commonly found in a Talend job export directory.
RAW_FILE_SUFFIXES = (".item", ".properties")


def read_raw_files(
    pipeline_dir: str,
    profile: str = "dev",
) -> dict[str, str]:
    """Read every raw file under a job's export directory in the Volume.

    pipeline_dir: full Volume path to one job's directory, e.g.
      /Volumes/talend_migration/samples/talend_export/samples/job_01_customer_enrichment
    Returns {relative_path: file_text}. Binary files (e.g. a .jar under
    jars/) are skipped -- this tool is for the text Talend exports an LLM
    agent reads, not for execution artifacts.
    """
    client = WorkspaceClient(profile=profile)
    out: dict[str, str] = {}
    _walk(client, pipeline_dir, pipeline_dir, out)
    return out


def _walk(client: WorkspaceClient, root: str, current: str, out: dict[str, str]) -> None:
    for entry in client.files.list_directory_contents(current):
        path = entry.path
        if entry.is_directory:
            _walk(client, root, path, out)
            continue
        if not path.endswith(RAW_FILE_SUFFIXES):
            continue
        resp = client.files.download(path)
        content = resp.contents.read().decode("utf-8", errors="replace")
        out[path[len(root):].lstrip("/")] = content


def chunk_files(
    raw_files: dict[str, str],
    max_chars_per_chunk: int = 60_000,
) -> list[dict[str, str]]:
    """Split a job's raw files into chunks small enough for one model call.

    Keeps whole files together (never splits a single file's content) so a
    component's XML is never cut mid-element; groups smaller files
    together up to the size budget. If a single file alone exceeds the
    budget, it gets its own chunk anyway -- the SPEC agent prompt is
    expected to handle that gracefully, just less precisely.
    """
    chunks: list[dict[str, str]] = []
    current: dict[str, str] = {}
    current_size = 0
    for path, content in raw_files.items():
        size = len(content)
        if current and current_size + size > max_chars_per_chunk:
            chunks.append(current)
            current, current_size = {}, 0
        current[path] = content
        current_size += size
    if current:
        chunks.append(current)
    return chunks or [{}]
