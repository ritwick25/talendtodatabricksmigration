"""Plain-code tool: read raw Talend export files from a Unity Catalog
Volume. Read-only -- this system never modifies anything under the Talend
export Volume. No parsing happens here; files are returned as raw text for
an LLM agent to read.
"""
from __future__ import annotations

import re

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


_CODE_PARAM_RE = re.compile(
    r'<elementParameter\s+name="CODE"[^>]*>\s*<!\[CDATA\[(.*?)\]\]>',
    re.DOTALL,
)


def extract_java_source(raw_files: dict[str, str], block_id: str) -> str:
    """Mechanical lookup, not semantic parsing: given a block_id the SPEC
    agent already identified (e.g. "tJavaRow_1"), pull that node's CODE
    CDATA text out of the raw XML so it can be handed to the JAVAROUTINE
    agent. This locates text by a name an LLM already gave us -- it does
    not understand components, connections, or Java, so it doesn't
    violate the no-deterministic-parser principle any more than
    read_raw_files() handing over a whole file's text does.
    """
    for path, content in raw_files.items():
        if not path.endswith(".item"):
            continue
        node_marker = f'uniqueName="{block_id}"'
        node_start = content.find(node_marker)
        if node_start == -1:
            continue
        # Search for the CODE CDATA block within a reasonable window after
        # the node tag starts (up to the next </node>).
        node_end = content.find("</node>", node_start)
        window = content[node_start:node_end if node_end != -1 else None]
        match = _CODE_PARAM_RE.search(window)
        if match:
            return match.group(1).strip()
    raise ValueError(f"Could not find CODE block for {block_id} in any raw file")
