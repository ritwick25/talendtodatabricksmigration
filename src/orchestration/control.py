"""Plain-code access to the singleton migration.control row. The
orchestrator reads this before every batch, every agent call, and every
deploy/run (guardrail #6); nothing here is ever decided by an LLM.
"""
from __future__ import annotations

from dataclasses import dataclass

from src.tools.sql_exec import exec_sql, rows

CATALOG = "talend_migration"
SCHEMA = "migration"
TABLE = f"{CATALOG}.{SCHEMA}.control"


@dataclass
class ControlState:
    kill_switch: bool
    halted: bool
    halt_reason: str | None
    batch_size: int
    max_fix_cycles: int
    halt_threshold: int
    max_spec_rounds: int
    max_review_rounds: int
    per_job_token_budget: int
    per_job_dbu_budget: float
    per_job_run_timeout_min: int
    global_daily_token_cap: int
    global_daily_dbu_cap: float


def read_control(warehouse_id: str, profile: str = "dev") -> ControlState:
    resp = exec_sql(
        f"SELECT kill_switch, halted, halt_reason, batch_size, max_fix_cycles, "
        f"halt_threshold, max_spec_rounds, max_review_rounds, per_job_token_budget, "
        f"per_job_dbu_budget, per_job_run_timeout_min, global_daily_token_cap, "
        f"global_daily_dbu_cap FROM {TABLE} WHERE id = 1",
        warehouse_id, profile,
    )
    r = rows(resp)
    if not r:
        raise RuntimeError("migration.control has no row with id=1 -- run setup/bootstrap_workspace.py")
    row = r[0]
    return ControlState(
        kill_switch=row[0] == "true",
        halted=row[1] == "true",
        halt_reason=row[2],
        batch_size=int(row[3]),
        max_fix_cycles=int(row[4]),
        halt_threshold=int(row[5]),
        max_spec_rounds=int(row[6]),
        max_review_rounds=int(row[7]),
        per_job_token_budget=int(row[8]),
        per_job_dbu_budget=float(row[9]),
        per_job_run_timeout_min=int(row[10]),
        global_daily_token_cap=int(row[11]),
        global_daily_dbu_cap=float(row[12]),
    )


def set_halted(halted: bool, halt_reason: str | None, warehouse_id: str, profile: str = "dev") -> None:
    reason_sql = "NULL" if halt_reason is None else f"'{halt_reason.replace(chr(39), chr(39)*2)}'"
    exec_sql(
        f"UPDATE {TABLE} SET halted = {str(halted).lower()}, halt_reason = {reason_sql}, "
        f"updated_at = current_timestamp() WHERE id = 1",
        warehouse_id, profile,
    )


def is_kill_switch_set(warehouse_id: str, profile: str = "dev") -> bool:
    resp = exec_sql(f"SELECT kill_switch FROM {TABLE} WHERE id = 1", warehouse_id, profile)
    r = rows(resp)
    return bool(r) and r[0][0] == "true"
