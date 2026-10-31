"""Shared plain-code helper: execute SQL against a Databricks SQL
warehouse via the Statement Execution API, using the databricks-sdk
Python client directly (not a `databricks` CLI subprocess). The CLI
binary isn't present inside a Databricks Jobs serverless Python task --
confirmed by an actual job run failing with an empty-stdout
JSONDecodeError when this module still shelled out to it -- so every
workspace call in this system goes through the SDK, which works
identically on a local machine (with the CLI installed) and inside a
job (without it).
"""
from __future__ import annotations

from databricks.sdk import WorkspaceClient


def pick_warehouse(profile: str = "dev") -> str:
    client = WorkspaceClient(profile=profile)
    warehouses = list(client.warehouses.list())
    serverless = [w for w in warehouses if w.enable_serverless_compute]
    chosen = (serverless or warehouses)[0]
    return chosen.id


def exec_sql(statement: str, warehouse_id: str, profile: str = "dev") -> dict:
    """Returns a dict shaped like the old REST/CLI response
    ({"status": {"state": ...}, "result": {"data_array": [...]}}) so
    every caller (control.py, queue_ops.py, process_job.py,
    eval_gate.py, batch_loop.py, ...) keeps working unchanged. Raises
    RuntimeError on any failure (transport or SQL error) with the
    message so callers can classify it (infra vs. a real SQL bug).
    """
    client = WorkspaceClient(profile=profile)
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="30s",
    )
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        error_message = None
        if resp.status and resp.status.error:
            error_message = resp.status.error.message
        raise RuntimeError(error_message or f"statement did not succeed: state={state}")

    data_array = resp.result.data_array if resp.result and resp.result.data_array else []
    return {"status": {"state": state}, "result": {"data_array": data_array}}


def esc(value: str) -> str:
    """Escape a string for inlining into a SQL literal. These modules
    use direct string interpolation rather than parameterized queries
    because the Statement Execution API's named-parameter support is
    limited for DDL/complex DML -- every caller MUST pass values through
    this escape, never interpolate raw.
    """
    return value.replace("'", "''")


def rows(statement_response: dict) -> list[list]:
    return statement_response.get("result", {}).get("data_array", [])
