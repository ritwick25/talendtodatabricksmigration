# Databricks notebook / job task source
# job_06_parent_orchestrator
#
# This is a pure orchestrator job (no data flow / tmaps): tPrejob -> tJava (side-effect
# setup of batchRunId into a cross-task state) -> tRunJob (child job call with context
# propagation) -> tPostjob on success, or tDie on child failure.
#
# In this Lakeflow/Databricks Jobs translation:
#   - tPrejob_1 / tPostjob_1 are synthetic markers -> logged no-ops.
#   - tJava_1 is the already-translated Java block (block_id=tJava_1), inserted verbatim
#     at its original position in the pipeline. It is UNRESOLVED/best-effort for the exact
#     ISO timestamp format (see its rationale/TODO) -- preserved as-is, not re-translated.
#   - tRunJob_1 -> a call to the child job's notebook/task entrypoint, with explicit context
#     parameter propagation (PROPAGATE_CONTEXT=true -> we forward the full resolved context
#     dict, plus the explicit CONTEXT_PARAM_customer_batch_id override) and ON_SUBJOB_OK /
#     ON_SUBJOB_ERROR routed to tPostjob_1 / tDie_1 respectively via a try/except at the
#     driver level (this task is not split into separate Databricks Jobs tasks with run_if
#     because the orchestration here is a single notebook driving a dbutils.notebook.run
#     child call; if the surrounding bundle models this as a multi-task job instead, this
#     tRunJob call should become its own task with run_if: ALL_SUCCESS / AT_LEAST_ONE_FAILED
#     pointing back to this task).
#
# Secrets: none required directly by this job (db_user/db_url are context vars used by
# downstream child jobs, not referenced here). If this job were extended to open a DB
# connection directly, credentials would come from dbutils.secrets.get(scope="talend-migration", ...).
#
# FIX (cycle 1): previous version had a broken try/except block for the dbutils NameError
# check -- the `except NameError:` line was indented one space relative to `try:`,
# producing "IndentationError: unindent does not match any outer indentation level".
# Rewritten using a plain NameError-safe lookup via globals() instead of try/except,
# which avoids the indentation mismatch entirely and is simpler.

import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_06_parent_orchestrator")

# ---------------------------------------------------------------------------
# Context resolution (Migration_Context group). In a real Databricks Jobs
# deployment these come from job/task parameters per environment (dev/test/prod
# bundle target); dbutils.widgets provides a notebook-local equivalent default.
# ---------------------------------------------------------------------------
dbutils = globals().get("dbutils", None)


def _get_param(name: str, default: str) -> str:
    if dbutils is not None:
        try:
            return dbutils.widgets.get(name)
        except Exception:
            pass
    return default


context = {
    "source_dir": _get_param("source_dir", "samples/talend_export_src/fixtures"),
    "db_url": _get_param("db_url", "jdbc:postgresql://dev-db.internal:5432/migration"),
    "db_user": _get_param("db_user", "migration_svc_dev"),
    "target_schema": _get_param("target_schema", "dev_samples"),
    "max_retries": _get_param("max_retries", "3"),
    "watermark_ts": _get_param("watermark_ts", "2024-01-01 00:00:00"),
    "s3_bucket": _get_param("s3_bucket", "s3a://sample-lake-dev/"),
    "max_claim_amount": _get_param("max_claim_amount", "5000"),
    "batch_run_prefix": _get_param("batch_run_prefix", "DEV"),
    "customer_batch_id": _get_param("customer_batch_id", "BATCH001"),
}


# ---------------------------------------------------------------------------
# tPrejob_1 -- synthetic prejob marker (no real lock-file check in this spec).
# ---------------------------------------------------------------------------
def t_prejob_1():
    logger.info("tPrejob_1: prejob marker OK (synthetic -- no lock file check in source).")


# ---------------------------------------------------------------------------
# tJava_1 -- verbatim translated Java block (block_id=tJava_1), inserted at its
# original pipeline position. UNRESOLVED/best-effort for DateUtil.formatIsoInZone;
# left exactly as produced by the JAVAROUTINE agent.
# ---------------------------------------------------------------------------
def run(input: dict) -> dict:
    batch_run_prefix = input.get("batch_run_prefix", "")

    # Best-effort stand-in for com.sample.util.DateUtil.formatIsoInZone(new Date(), "UTC")
    now_utc = datetime.now(timezone.utc)
    iso_ts = now_utc.strftime("%Y-%m-%dT%H:%M:%S") + "Z"  # UNVERIFIED format

    batch_run_id = f"{batch_run_prefix}-{iso_ts}"

    print(f"job_06_parent_orchestrator: starting run {batch_run_id}")

    # globalMap.put("batchRunId", ...) -> explicit state dict returned for downstream
    # steps / tasks to consume (e.g. via dbutils.jobs.taskValues.set in the notebook
    # orchestration layer, since this crosses tRunJob/child-job boundaries).
    return {
        "batchRunId": batch_run_id
    }


def t_java_1(ctx: dict) -> dict:
    """Wraps the translated tJava_1 block, matching globalmap_usage: put('batchRunId')."""
    global_map = run({"batch_run_prefix": ctx["batch_run_prefix"]})
    if dbutils is not None:
        try:
            dbutils.jobs.taskValues.set(key="batchRunId", value=global_map["batchRunId"])
        except Exception as e:  # pragma: no cover - best effort propagation
            logger.warning("tJava_1: could not set taskValues.batchRunId: %s", e)
    return global_map


# ---------------------------------------------------------------------------
# tRunJob_1 -- call child job job_07_child_validation, propagating context.
# PROPAGATE_CONTEXT=true -> forward the whole resolved context dict as params;
# CONTEXT_PARAM_customer_batch_id -> explicit override of customer_batch_id
# (identical value here, but kept explicit to match the spec's elementParameter).
# ---------------------------------------------------------------------------
def t_run_job_1(ctx: dict, global_map: dict) -> None:
    child_job_name = "job_07_child_validation"
    child_params = dict(ctx)  # PROPAGATE_CONTEXT=true -> forward full context
    child_params["customer_batch_id"] = ctx["customer_batch_id"]  # CONTEXT_PARAM_customer_batch_id override
    child_params["batchRunId"] = global_map.get("batchRunId")

    logger.info(
        "tRunJob_1: invoking child job %s with customer_batch_id=%s, batchRunId=%s",
        child_job_name, child_params["customer_batch_id"], child_params["batchRunId"],
    )

    if dbutils is None:
        raise RuntimeError(
            "tRunJob_1: dbutils not available -- cannot invoke child job "
            f"'{child_job_name}' in this environment."
        )

    # USE_DYNAMIC_PARAM=false -> parameters passed as a fixed, explicitly-built dict,
    # not resolved dynamically at call time from a variable parameter table.
    dbutils.notebook.run(
        child_job_name,
        timeout_seconds=3600,
        arguments={k: str(v) for k, v in child_params.items()},
    )


# ---------------------------------------------------------------------------
# tPostjob_1 -- synthetic postjob marker (cleanup / notification point).
# ---------------------------------------------------------------------------
def t_postjob_1():
    logger.info("tPostjob_1: postjob marker OK (synthetic -- cleanup/notification point).")


# ---------------------------------------------------------------------------
# tDie_1 -- raised on ON_SUBJOB_ERROR from tRunJob_1, matching the original's
# aborting MESSAGE + ERROR_CODE semantics.
# ---------------------------------------------------------------------------
def t_die_1(ctx: dict, cause: Exception) -> None:
    message = f"job_06_parent_orchestrator: child job failed for batch {ctx['customer_batch_id']}"
    logger.error(message)
    raise RuntimeError(message) from cause


def main():
    # subjob_1 trigger order: tPrejob_1 -> tJava_1 -> tRunJob_1 -> tPostjob_1 / tDie_1
    t_prejob_1()

    global_map = t_java_1(context)

    try:
        t_run_job_1(context, global_map)
    except Exception as exc:
        # ON_SUBJOB_ERROR
        t_die_1(context, exc)
        sys.exit(2)  # ERROR_CODE=2, matches tDie_1.ERROR_CODE
    else:
        # ON_SUBJOB_OK
        t_postjob_1()


if __name__ == "__main__":
    main()
