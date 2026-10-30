"""Shared plain-code helper: execute SQL against a Databricks SQL
warehouse via the Statement Execution API, through the `databricks` CLI
subprocess (keeps every workspace-mutating call in this system visible
to the same auditable path, rather than mixing in a separate SDK client
for just this one thing).
"""
from __future__ import annotations

import json
import subprocess


def pick_warehouse(profile: str = "dev") -> str:
    result = subprocess.run(
        ["databricks", "warehouses", "list", "--profile", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    warehouses = json.loads(result.stdout)
    serverless = [w for w in warehouses if w.get("enable_serverless_compute")]
    return (serverless or warehouses)[0]["id"]


def exec_sql(statement: str, warehouse_id: str, profile: str = "dev") -> dict:
    """Returns the parsed statement-execution response. Raises
    RuntimeError on any failure (transport or SQL error) with the
    message so callers can classify it (infra vs. a real SQL bug)."""
    payload = json.dumps({"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s"})
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", profile, "--json", payload],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr)
    out = json.loads(result.stdout)
    if out.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(out.get("status", {}).get("error", {}).get("message", json.dumps(out)))
    return out


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
