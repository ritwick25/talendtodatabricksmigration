"""
Databricks Jobs task: job_06_parent_orchestrator

Translated from a Talend parent orchestrator job:
  tPrejob_1 -> tJava_1 -> tRunJob_1 -> tPostjob_1
                              \-> (on error) -> tDie_1

This module is a plain PySpark/Python batch task (spark_python_task), run as a
standard Databricks Jobs step -- NOT a Lakeflow Declarative Pipeline. The
ON_SUBJOB_OK / ON_SUBJOB_ERROR wiring from the spec is expressed natively via
Databricks Jobs task dependencies/run_if conditions at the job-resource level;
this script only implements the single task's own logic (tPrejob marker,
tJava_1 setup block, invoking the child job job_07_child_validation, and
raising on failure so the job-level run_if=ON_FAILURE path to a tDie-equivalent
task can fire). tPostjob_1 is likewise a separate downstream task triggered by
run_if=ALL_SUCCESS at the job-resource level, not inline code here.
"""

import datetime
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_06_parent_orchestrator")

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None

try:
    import dbutils  # noqa: F401  (only present inside real Databricks runtime)
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark) if spark is not None else None
except Exception:
    dbutils = None

# ---------------------------------------------------------------------------
# Context variables (Migration_Context group) -- literal dev defaults, per spec.
# If this task is later wired with real job parameters, read overrides from
# sys.argv instead of inventing a config-key lookup.
# ---------------------------------------------------------------------------
SOURCE_DIR = "samples/talend_export_src/fixtures"
DB_URL = "jdbc:postgresql://dev-db.internal:5432/migration"
DB_USER = "migration_svc_dev"
TARGET_SCHEMA = "dev_samples"
MAX_RETRIES = 3
WATERMARK_TS = "2024-01-01 00:00:00"
S3_BUCKET = "s3a://sample-lake-dev/"
MAX_CLAIM_AMOUNT = 5000
BATCH_RUN_PREFIX = "DEV"
CUSTOMER_BATCH_ID = "BATCH001"

CHILD_JOB_NAME = "job_07_child_validation"


def _read_arg_override(name, default):
    """Optional sys.argv override, e.g. --customer_batch_id=FOO."""
    prefix = f"--{name}="
    for arg in sys.argv[1:]:
        if arg.startswith(prefix):
            return arg[len(prefix):]
    return default


CUSTOMER_BATCH_ID = _read_arg_override("customer_batch_id", CUSTOMER_BATCH_ID)
BATCH_RUN_PREFIX = _read_arg_override("batch_run_prefix", BATCH_RUN_PREFIX)


# ---------------------------------------------------------------------------
# tJava_1 translation (verbatim, as produced by the JAVAROUTINE agent for
# block_id=tJava_1). Mirrors com.sample.util.DateUtil.formatIsoInZone(..., "UTC")
# with a best-effort ISO-8601 UTC formatter -- flagged translation_uncertain
# upstream; kept as-is here.
# ---------------------------------------------------------------------------
def _format_iso_in_zone_utc(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def tjava_1_run(input: dict) -> dict:
    # context.batch_run_prefix -> job parameter/widget, expected to be passed in input
    batch_run_prefix = input.get('batch_run_prefix', '')

    now = datetime.datetime.now(datetime.timezone.utc)
    # TODO: original Java: com.sample.util.DateUtil.formatIsoInZone(new java.util.Date(), "UTC")
    batch_run_id = f"{batch_run_prefix}-{_format_iso_in_zone_utc(now)}"

    # globalMap.put("batchRunId", batchRunId) -> explicit state dict entry, propagated via
    # dbutils.jobs.taskValues.set(key='batchRunId', value=batch_run_id) in the orchestrator job
    # so downstream tRunJob-equivalent child tasks/notebooks can read it back.
    output = {'batchRunId': batch_run_id}

    # System.out.println(...) -> explicit, visible log line (side effect), not silently dropped
    print(f"job_06_parent_orchestrator: starting run {batch_run_id}", file=sys.stdout)

    return output


def tprejob_marker():
    """tPrejob_1: synthetic marker, e.g. a lock-file check in the original job.
    No actual lock mechanism specified, so this is a no-op logged checkpoint."""
    logger.info("job_06_parent_orchestrator: tPrejob_1 marker -- pre-job checks OK")


def run_child_job(batch_run_id: str) -> None:
    """
    tRunJob_1 equivalent: invoke job_07_child_validation as a dependent Databricks
    Jobs task, propagating context (customer_batch_id) and the batchRunId computed
    in tJava_1. PROPAGATE_CONTEXT=true in the spec maps to passing the full set of
    context values along as job parameters / taskValues, rather than a literal
    in-process Python function call, since the child is itself a separate task in
    the job graph (see child_jobs in the spec). Here we both (a) publish taskValues
    for the next task in this same job run to pick up, and (b) log the parameters
    that the job-level task dependency wiring will pass to the child task.
    """
    params = {
        "customer_batch_id": CUSTOMER_BATCH_ID,
        "batchRunId": batch_run_id,
    }
    logger.info(
        "job_06_parent_orchestrator: invoking child job %s with params=%s "
        "(PROPAGATE_CONTEXT=true)",
        CHILD_JOB_NAME,
        params,
    )

    if dbutils is not None:
        try:
            dbutils.jobs.taskValues.set(key="customer_batch_id", value=CUSTOMER_BATCH_ID)
            dbutils.jobs.taskValues.set(key="batchRunId", value=batch_run_id)
        except Exception as exc:  # pragma: no cover - best effort in non-notebook contexts
            logger.warning("job_06_parent_orchestrator: could not set taskValues: %s", exc)
    else:
        logger.warning(
            "job_06_parent_orchestrator: dbutils unavailable in this execution context; "
            "taskValues not set. Child task %s must be wired as a separate job task "
            "with its own dependency on this task in the job resource.",
            CHILD_JOB_NAME,
        )

    # The actual execution of job_07_child_validation happens as a separate
    # Databricks Jobs task, declared as depending on this task in the job
    # resource (run_if=ALL_SUCCESS), per child_jobs[] in the spec. This task's
    # job is only to prepare/publish the parameters and signal success/failure
    # for that dependency's run_if gating (ON_SUBJOB_OK -> tPostjob_1 task,
    # ON_SUBJOB_ERROR -> tDie_1 task).


def main():
    tprejob_marker()

    tjava_output = tjava_1_run({"batch_run_prefix": BATCH_RUN_PREFIX})
    batch_run_id = tjava_output["batchRunId"]

    try:
        run_child_job(batch_run_id)
    except Exception as exc:
        # tDie_1 equivalent: fail loudly so the job-level run_if=ON_FAILURE
        # dependency to the tDie_1 task fires, matching ON_SUBJOB_ERROR semantics.
        message = f"job_06_parent_orchestrator: child job failed for batch {CUSTOMER_BATCH_ID}"
        logger.error(message)
        raise RuntimeError(message) from exc

    logger.info(
        "job_06_parent_orchestrator: run %s completed successfully; "
        "tPostjob_1 (cleanup/notification) task may now proceed.",
        batch_run_id,
    )


if __name__ == "__main__":
    main()
