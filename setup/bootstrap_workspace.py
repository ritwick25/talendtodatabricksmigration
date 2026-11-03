#!/usr/bin/env python3
"""
Idempotent workspace bootstrap for the Talend-to-Databricks migration system.

Run this once per Databricks workspace (first time you point this repo at a
new workspace) to create everything the system depends on: the catalog,
schemas, sample-data volume, secret scope, and all control-plane Delta
tables, plus the seed control row.

Usage:
    databricks auth login --host https://<workspace>.azuredatabricks.net --profile dev
    # or hand-write a [dev] profile in ~/.databrickscfg with client_id/client_secret
    python3 setup/bootstrap_workspace.py --profile dev

    # To also grant another principal USE_CATALOG/USE_SCHEMA/SELECT on the
    # catalog (e.g. your own human account, separate from the service
    # principal that owns it), pass --grant-to:
    python3 setup/bootstrap_workspace.py --profile dev --grant-to someone@example.com

Nothing here is destructive: every step checks for existence first and
skips if already present. Safe to re-run.
"""
import argparse
import json
import subprocess
import sys

TABLES = {
    "queue": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.queue (
  pipeline_id STRING NOT NULL,
  source STRING,
  status STRING,
  fix_attempts INT,
  last_error STRING,
  error_signature STRING,
  complexity_score INT,
  complexity_rationale STRING,
  java_complexity STRING,
  java_uncertain BOOLEAN,
  spec_uncertain BOOLEAN,
  unresolved_dependencies BOOLEAN,
  notes STRING,
  batch_id STRING,
  deployed_job_id STRING,
  last_run_id STRING,
  code_hash STRING,
  commit_sha STRING,
  cost DOUBLE,
  validation_status STRING,
  updated_at TIMESTAMP
) USING DELTA
""",
    "job_spec": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.job_spec (
  pipeline_id STRING NOT NULL,
  spec_json STRING,
  spec_version INT,
  verifier_result STRING,
  uncertainties STRING,
  updated_at TIMESTAMP
) USING DELTA
""",
    "batch_log": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.batch_log (
  batch_id STRING NOT NULL,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  job_ids STRING,
  completed_count INT,
  failed_count INT,
  halted BOOLEAN,
  halt_reason STRING,
  cost DOUBLE
) USING DELTA
""",
    "java_block_log": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.java_block_log (
  pipeline_id STRING NOT NULL,
  block_id STRING NOT NULL,
  tags STRING,
  translation_tier STRING,
  unit_cases_passed INT,
  unit_cases_total INT,
  case_source STRING,
  updated_at TIMESTAMP
) USING DELTA
""",
    "attempt_log": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.attempt_log (
  attempt_id STRING NOT NULL,
  pipeline_id STRING,
  ts TIMESTAMP,
  agent STRING,
  model STRING,
  prompt_version STRING,
  knowledge_version STRING,
  git_sha STRING,
  cost DOUBLE,
  error_signature STRING,
  code_hash STRING,
  injection_suspected BOOLEAN,
  details STRING
) USING DELTA
""",
    "improvement_log": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.improvement_log (
  improvement_id STRING NOT NULL,
  created_at TIMESTAMP,
  topic STRING,
  what STRING,
  why STRING,
  before_metrics STRING,
  after_metrics STRING,
  assumptions STRING,
  pr_url STRING,
  merged BOOLEAN
) USING DELTA
""",
    "eval_results": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.eval_results (
  eval_id STRING NOT NULL,
  pr_url STRING,
  pipeline_id STRING,
  ran_at TIMESTAMP,
  reached_completed BOOLEAN,
  fix_cycles INT,
  verifier_approved BOOLEAN,
  java_unit_pass_rate DOUBLE,
  cost DOUBLE,
  details STRING
) USING DELTA
""",
    "control": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.control (
  id INT NOT NULL,
  kill_switch BOOLEAN,
  halted BOOLEAN,
  halt_reason STRING,
  batch_size INT,
  max_fix_cycles INT,
  halt_threshold INT,
  max_spec_rounds INT,
  max_review_rounds INT,
  per_job_token_budget BIGINT,
  per_job_dbu_budget DOUBLE,
  per_job_run_timeout_min INT,
  global_daily_token_cap BIGINT,
  global_daily_dbu_cap DOUBLE,
  updated_at TIMESTAMP
) USING DELTA
""",
    # Not in the original spec's table list -- added so the SPEC agent's
    # per-job source->target lineage has somewhere to land for the
    # separately-run data validation process to consume.
    "lineage": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.lineage (
  pipeline_id STRING NOT NULL,
  spec_version INT,
  lineage_id STRING NOT NULL,
  subjob_id STRING,
  component_name STRING,
  component_type STRING,
  source_type STRING,
  source_system STRING,
  source_table STRING,
  source_columns ARRAY<STRING>,
  target_type STRING,
  target_system STRING,
  target_table STRING,
  target_columns ARRAY<STRING>,
  column_lineage STRING,
  transformation_summary STRING,
  confidence STRING,
  commit_sha STRING,
  created_at TIMESTAMP
) USING DELTA
""",
    # Destination-side validation stats, computed by run_validation.py
    # against the real target table (native Databricks or a Unity Catalog
    # foreign catalog, e.g. a Redshift connection -- both are queryable the
    # same way through a SQL warehouse). The talend_* columns stay NULL
    # until a real Talend-side baseline exists to compare against; this
    # table exists now so the comparison can be wired in later without a
    # schema change.
    "validation_log": """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.validation_log (
  validation_id STRING NOT NULL,
  pipeline_id STRING NOT NULL,
  lineage_id STRING,
  created_at TIMESTAMP,
  destination_type STRING,
  destination_system STRING,
  destination_table STRING,
  destination_row_count BIGINT,
  destination_column_checksums STRING,
  talend_row_count BIGINT,
  talend_column_checksums STRING,
  status STRING,
  notes STRING
) USING DELTA
""",
}


def run_cli(args, profile, check=True):
    cmd = ["databricks"] + args + ["--profile", profile]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        return None, result.stderr
    return result.stdout, result.stderr


def pick_warehouse(profile):
    out, err = run_cli(["warehouses", "list", "-o", "json"], profile)
    if out is None:
        print(f"ERROR: could not list warehouses: {err}", file=sys.stderr)
        sys.exit(1)
    warehouses = json.loads(out)
    if not warehouses:
        print(
            "ERROR: no SQL warehouse found in this workspace. Create one "
            "(a small serverless warehouse is fine) and re-run.",
            file=sys.stderr,
        )
        sys.exit(1)
    serverless = [w for w in warehouses if w.get("enable_serverless_compute")]
    chosen = (serverless or warehouses)[0]
    print(f"Using warehouse: {chosen['name']} ({chosen['id']})")
    return chosen["id"]


def exec_sql(warehouse_id, statement, profile):
    payload = json.dumps(
        {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s"}
    )
    cmd = [
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", profile, "--json", payload,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return False, result.stderr
    out = json.loads(result.stdout)
    state = out.get("status", {}).get("state")
    if state != "SUCCEEDED":
        return False, out.get("status", {}).get("error", {}).get("message", out)
    return True, None


def ensure_catalog(name, profile):
    out, _ = run_cli(["catalogs", "get", name, "-o", "json"], profile, check=False)
    if out:
        print(f"catalog {name}: already exists")
        return
    out, err = run_cli(["catalogs", "create", name, "-o", "json"], profile)
    if out is None:
        print(f"ERROR creating catalog {name}: {err}", file=sys.stderr)
        sys.exit(1)
    print(f"catalog {name}: created")


def ensure_schema(catalog, schema, profile):
    full = f"{catalog}.{schema}"
    out, _ = run_cli(["schemas", "get", full, "-o", "json"], profile, check=False)
    if out:
        print(f"schema {full}: already exists")
        return
    out, err = run_cli(["schemas", "create", schema, catalog, "-o", "json"], profile)
    if out is None:
        print(f"ERROR creating schema {full}: {err}", file=sys.stderr)
        sys.exit(1)
    print(f"schema {full}: created")


def ensure_volume(catalog, schema, volume, profile):
    full = f"{catalog}.{schema}.{volume}"
    out, _ = run_cli(["volumes", "read", full, "-o", "json"], profile, check=False)
    if out:
        print(f"volume {full}: already exists")
    else:
        out, err = run_cli(
            ["volumes", "create", catalog, schema, volume, "MANAGED", "-o", "json"],
            profile,
        )
        if out is None:
            print(f"ERROR creating volume {full}: {err}", file=sys.stderr)
            sys.exit(1)
        print(f"volume {full}: created")

    for sub in ("samples", "real", "jars", "jars_src"):
        path = f"dbfs:/Volumes/{catalog}/{schema}/{volume}/{sub}"
        run_cli(["fs", "mkdir", path], profile, check=False)
    print(f"volume {full}: ensured samples/real/jars/jars_src subdirectories")


def ensure_secret_scope(scope, profile):
    out, _ = run_cli(["secrets", "list-scopes", "-o", "json"], profile, check=False)
    existing = {s["name"] for s in json.loads(out)} if out else set()
    if scope in existing:
        print(f"secret scope {scope}: already exists")
        return
    out, err = run_cli(["secrets", "create-scope", scope], profile)
    if out is None and err:
        print(f"ERROR creating secret scope {scope}: {err}", file=sys.stderr)
        sys.exit(1)
    print(f"secret scope {scope}: created")


def ensure_tables(catalog, schema, warehouse_id, profile):
    for name, sql_template in TABLES.items():
        sql = sql_template.format(catalog=catalog, schema=schema).strip()
        ok, err = exec_sql(warehouse_id, sql, profile)
        print(f"table {catalog}.{schema}.{name}: {'ok' if ok else f'FAILED ({err})'}")
        if not ok:
            sys.exit(1)


def ensure_control_row(catalog, schema, warehouse_id, profile):
    check_sql = f"SELECT count(*) FROM {catalog}.{schema}.control WHERE id = 1"
    payload = json.dumps({"warehouse_id": warehouse_id, "statement": check_sql, "wait_timeout": "30s"})
    cmd = ["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", profile, "--json", payload]
    result = subprocess.run(cmd, capture_output=True, text=True)
    out = json.loads(result.stdout)
    count = int(out.get("result", {}).get("data_array", [["0"]])[0][0])
    if count > 0:
        print("control row (id=1): already exists")
        return
    insert_sql = f"""
    INSERT INTO {catalog}.{schema}.control
    (id, kill_switch, halted, halt_reason, batch_size, max_fix_cycles, halt_threshold,
     max_spec_rounds, max_review_rounds, per_job_token_budget, per_job_dbu_budget,
     per_job_run_timeout_min, global_daily_token_cap, global_daily_dbu_cap, updated_at)
    VALUES
    (1, false, false, NULL, 5, 2, 3, 2, 3, 2000000, 5.0, 30, 20000000, 50.0, current_timestamp())
    """
    ok, err = exec_sql(warehouse_id, insert_sql.strip(), profile)
    print(f"control row (id=1): {'inserted' if ok else f'FAILED ({err})'}")
    if not ok:
        sys.exit(1)


def grant_access(catalog, principal, warehouse_id, profile):
    sql = f"GRANT USE_CATALOG, USE_SCHEMA, SELECT ON CATALOG {catalog} TO `{principal}`"
    ok, err = exec_sql(warehouse_id, sql, profile)
    print(f"grant USE_CATALOG/USE_SCHEMA/SELECT on {catalog} to {principal}: {'ok' if ok else f'FAILED ({err})'}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", default="dev", help="~/.databrickscfg profile to use")
    p.add_argument("--catalog", default="talend_migration")
    p.add_argument("--control-schema", default="migration")
    p.add_argument("--samples-schema", default="samples")
    p.add_argument("--volume", default="talend_export")
    p.add_argument("--secret-scope", default="talend-migration")
    p.add_argument("--warehouse-id", default=None, help="SQL warehouse id; auto-picked if omitted")
    p.add_argument("--grant-to", default=None, help="Principal (email) to grant USE_CATALOG/USE_SCHEMA/SELECT to")
    args = p.parse_args()

    print(f"--- bootstrapping catalog={args.catalog} profile={args.profile} ---")
    ensure_catalog(args.catalog, args.profile)
    ensure_schema(args.catalog, args.control_schema, args.profile)
    ensure_schema(args.catalog, args.samples_schema, args.profile)
    ensure_volume(args.catalog, args.samples_schema, args.volume, args.profile)
    ensure_secret_scope(args.secret_scope, args.profile)

    warehouse_id = args.warehouse_id or pick_warehouse(args.profile)
    ensure_tables(args.catalog, args.control_schema, warehouse_id, args.profile)
    ensure_control_row(args.catalog, args.control_schema, warehouse_id, args.profile)

    if args.grant_to:
        grant_access(args.catalog, args.grant_to, warehouse_id, args.profile)
    else:
        print(
            "No --grant-to given. If other users need to see this catalog in "
            "Catalog Explorer, grant it explicitly, e.g.:\n"
            f"  GRANT USE_CATALOG, USE_SCHEMA, SELECT ON CATALOG {args.catalog} "
            "TO `someone@example.com`;"
        )

    print("--- bootstrap complete ---")


if __name__ == "__main__":
    main()
