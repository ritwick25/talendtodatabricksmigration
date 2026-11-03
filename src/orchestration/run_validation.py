"""Computes real destination-side stats for completed pipelines and writes
them to validation_log. Never fabricates a Talend-side baseline -- those
columns stay NULL until a real one exists (no Talend execution of these
jobs has ever happened). The destination is read from the control table's
destination_path, combined with the bare table name the SPEC agent read
off the Talend job's own tDBOutput/tFileOutput parameters (lineage.target_
table) -- not guessed. This works unchanged whether destination_path points
at a native Databricks schema or a Unity Catalog foreign catalog (e.g. a
Redshift connection via Lakehouse Federation): both are queried the same
way through a SQL warehouse.

Usage:
    python3 -m src.orchestration.run_validation --profile dev [--pipeline-ids job_01,job_02]
"""
from __future__ import annotations

import argparse
import inspect
import re
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(inspect.currentframe().f_code.co_filename).resolve().parents[2]))

from src.tools.sql_exec import esc, exec_sql, pick_warehouse, rows

CATALOG = "talend_migration"
SCHEMA = "migration"
BARE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REF_RE = re.compile(r"^[A-Za-z0-9_.]+$")


def _sql_str(v):
    return f"'{esc(v)}'" if v is not None else "NULL"


def read_destination_path(warehouse_id: str, profile: str) -> str | None:
    resp = exec_sql(f"SELECT destination_path FROM {CATALOG}.{SCHEMA}.control WHERE id = 1", warehouse_id, profile)
    r = rows(resp)
    return r[0][0] if r and r[0][0] else None


def latest_lineage_target(pipeline_id: str, warehouse_id: str, profile: str) -> dict | None:
    """Most recent lineage row for this pipeline with a non-empty target
    table. None if the SPEC agent found no output component at all."""
    resp = exec_sql(
        f"""
        SELECT lineage_id, target_type, target_system, target_table
        FROM {CATALOG}.{SCHEMA}.lineage
        WHERE pipeline_id = '{esc(pipeline_id)}' AND target_table IS NOT NULL AND target_table != ''
        ORDER BY created_at DESC
        LIMIT 1
        """,
        warehouse_id, profile,
    )
    r = rows(resp)
    if not r:
        return None
    lineage_id, target_type, target_system, target_table = r[0]
    return {"lineage_id": lineage_id, "type": target_type, "system": target_system, "table": target_table}


def write_result(
    pipeline_id, warehouse_id, profile, *, lineage_id=None, dest_type=None, dest_system=None,
    dest_table=None, row_count=None, checksum=None, status, notes,
):
    statement = f"""
    INSERT INTO {CATALOG}.{SCHEMA}.validation_log
    (validation_id, pipeline_id, lineage_id, created_at, destination_type, destination_system,
     destination_table, destination_row_count, destination_column_checksums,
     talend_row_count, talend_column_checksums, status, notes)
    VALUES ('{uuid.uuid4()}', '{esc(pipeline_id)}', {_sql_str(lineage_id)}, current_timestamp(),
            {_sql_str(dest_type)}, {_sql_str(dest_system)}, {_sql_str(dest_table)},
            {row_count if row_count is not None else "NULL"},
            {_sql_str(str(checksum)) if checksum is not None else "NULL"},
            NULL, NULL, {_sql_str(status)}, {_sql_str(notes[:4000])})
    """
    exec_sql(statement.strip(), warehouse_id, profile)


def validate_one(pipeline_id: str, destination_path: str | None, warehouse_id: str, profile: str) -> None:
    target = latest_lineage_target(pipeline_id, warehouse_id, profile)
    if not target:
        write_result(
            pipeline_id, warehouse_id, profile, status="NO_LINEAGE",
            notes="No lineage row with a resolvable target table for this pipeline -- "
            "the SPEC agent either found no output component or could not read one.",
        )
        return

    bare_table = target["table"].strip()
    if not BARE_NAME_RE.match(bare_table):
        write_result(
            pipeline_id, warehouse_id, profile, lineage_id=target["lineage_id"], dest_type=target["type"],
            dest_system=target["system"], dest_table=bare_table, status="UNRESOLVED_TARGET",
            notes=f"target_table '{bare_table}' isn't a plain table name -- likely an unresolved "
            "context variable or expression, so it can't be queried directly.",
        )
        return

    if not destination_path or not REF_RE.match(destination_path):
        write_result(
            pipeline_id, warehouse_id, profile, lineage_id=target["lineage_id"], dest_type=target["type"],
            dest_system=target["system"], dest_table=bare_table, status="NO_DESTINATION_CONFIGURED",
            notes="control.destination_path is empty or not a plain catalog.schema reference. "
            "Set it on the Sources page first.",
        )
        return

    full_table = f"{destination_path}.{bare_table}"
    try:
        row_count = int(rows(exec_sql(f"SELECT COUNT(*) FROM {full_table}", warehouse_id, profile))[0][0])
    except Exception as e:
        write_result(
            pipeline_id, warehouse_id, profile, lineage_id=target["lineage_id"], dest_type=target["type"],
            dest_system=target["system"], dest_table=full_table, status="DESTINATION_UNREADABLE",
            notes=f"Could not query {full_table}: {str(e)[:400]}",
        )
        return

    # Whole-row checksum (order-independent) rather than per-column: there's
    # no baseline yet to compare individual columns against either way.
    try:
        checksum = rows(exec_sql(f"SELECT SUM(hash(to_json(struct(*)))) FROM {full_table}", warehouse_id, profile))[0][0]
    except Exception:
        checksum = None

    write_result(
        pipeline_id, warehouse_id, profile, lineage_id=target["lineage_id"], dest_type=target["type"],
        dest_system=target["system"], dest_table=full_table, row_count=row_count, checksum=checksum,
        status="DESTINATION_ONLY",
        notes="Computed from the real destination table. No Talend baseline exists yet to compare against.",
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="dev")
    p.add_argument(
        "--pipeline-ids", default=None,
        help="Comma-separated explicit pipeline list. Empty runs every COMPLETED pipeline with no validation_log row yet.",
    )
    args = p.parse_args()

    warehouse_id = pick_warehouse(args.profile)
    destination_path = read_destination_path(warehouse_id, args.profile)

    if args.pipeline_ids:
        pipeline_ids = [x.strip() for x in args.pipeline_ids.split(",") if x.strip()]
    else:
        resp = exec_sql(
            f"""
            SELECT q.pipeline_id FROM {CATALOG}.{SCHEMA}.queue q
            WHERE q.status = 'COMPLETED'
              AND NOT EXISTS (SELECT 1 FROM {CATALOG}.{SCHEMA}.validation_log v WHERE v.pipeline_id = q.pipeline_id)
            """,
            warehouse_id, args.profile,
        )
        pipeline_ids = [r[0] for r in rows(resp)]

    print(f"validating {len(pipeline_ids)} pipeline(s) against destination '{destination_path}': {pipeline_ids}")
    for pid in pipeline_ids:
        validate_one(pid, destination_path, warehouse_id, args.profile)
        print(f"-- {pid}: done --")


if __name__ == "__main__":
    main()
