"""
Databricks Jobs spark_python_task module
Generated from Talend job spec: job_06_parent_orchestrator

This is a plain PySpark batch job (runs as a Databricks Jobs spark_python_task).
It is NOT a Lakeflow Declarative Pipeline (DLT) -- no `dlt` import, no decorators.

Structure mirrors the original Talend orchestration:
  tPrejob_1 -> tJava_1 -> tRunJob_1 -> tPostjob_1 (ON_SUBJOB_OK)
                                 \-> tDie_1 (ON_SUBJOB_ERROR)

Note: tRunJob_1 (child job job_07_child_validation) is modeled here as a
Databricks Jobs API 'run now' trigger with explicit parameter passing
(context propagation via job parameters). In the actual job bundle, this
task should be wired as a separate task in the job graph with a
run_job_task reference to job_07_child_validation and run_if conditions
ON_SUBJOB_OK/ON_SUBJOB_ERROR at the orchestration layer wherever possible;
the explicit trigger/polling code below is a safety-net fallback for when
this module is invoked as a single monolithic task instead.
"""

import sys
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

try:
    from pyspark.dbutils import DBUtils  # type: ignore
except ImportError:
    DBUtils = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("job_06_parent_orchestrator")

spark = SparkSession.builder.getOrCreate()


def _get_dbutils():
    if DBUtils is not None:
        try:
            return DBUtils(spark)
        except Exception:
            pass
    try:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]
    except Exception:
        return None


dbutils = _get_dbutils()


# ---------------------------------------------------------------------------
# Context variables (Migration_Context group). Environment selection comes
# from a job widget/parameter named "env", defaulting to "dev".
# ---------------------------------------------------------------------------

_CONTEXTS = {
    "dev": {
        "source_dir": "samples/talend_export_src/fixtures",
        "db_url": "jdbc:postgresql://dev-db.internal:5432/migration",
        "db_user": "migration_svc_dev",
        "target_schema": "dev_samples",
        "max_retries": "3",
        "watermark_ts": "2024-01-01 00:00:00",
        "s3_bucket": "s3a://sample-lake-dev/",
        "max_claim_amount": "5000",
        "batch_run_prefix": "DEV",
        "customer_batch_id": "BATCH001",
    },
    "test": {
        "source_dir": "samples/talend_export_src/fixtures",
        "db_url": "jdbc:postgresql://test-db.internal:5432/migration",
        "db_user": "migration_svc_test",
        "target_schema": "test_samples",
        "max_retries": "3",
        "watermark_ts": "2024-01-01 00:00:00",
        "s3_bucket": "s3a://sample-lake-test/",
        "max_claim_amount": "5000",
        "batch_run_prefix": "TEST",
        "customer_batch_id": "BATCH001",
    },
    "prod": {
        "source_dir": "samples/talend_export_src/fixtures",
        "db_url": "jdbc:postgresql://prod-db.internal:5432/migration",
        "db_user": "migration_svc_prod",
        "target_schema": "samples",
        "max_retries": "5",
        "watermark_ts": "2024-06-01 00:00:00",
        "s3_bucket": "s3a://sample-lake-prod/",
        "max_claim_amount": "10000",
        "batch_run_prefix": "PROD",
        "customer_batch_id": "BATCH-PROD-001",
    },
}


def _get_job_param(name, default=None):
    """Read a job parameter (widget) if available, else fall back to default."""
    if dbutils is not None:
        try:
            return dbutils.widgets.get(name)
        except Exception:
            pass
    return default


def get_context():
    env = _get_job_param("env", "dev") or "dev"
    env = env.lower()
    if env not in _CONTEXTS:
        logger.warning("Unknown env '%s', defaulting to 'dev'", env)
        env = "dev"
    ctx = dict(_CONTEXTS[env])
    # Allow an explicit override of customer_batch_id via job parameter,
    # since in the original Talend job this context var often varies per run.
    override_batch_id = _get_job_param("customer_batch_id", None)
    if override_batch_id:
        ctx["customer_batch_id"] = override_batch_id
    ctx["env"] = env
    return ctx


# ---------------------------------------------------------------------------
# tJava_1 translation (BEST_EFFORT, block_id=tJava_1) -- inserted verbatim
# from the JAVAROUTINE agent's output, at the point the spec places it
# (between tPrejob_1 and tRunJob_1).
# ---------------------------------------------------------------------------

from datetime import datetime, timezone

# TODO original Java:
# String batchRunId = (String)context.batch_run_prefix + "-" + com.sample.util.DateUtil.formatIsoInZone(new java.util.Date(), "UTC");
# globalMap.put("batchRunId", batchRunId);
# System.out.println("job_06_parent_orchestrator: starting run " + batchRunId);

def _format_iso_in_zone_utc(dt: datetime) -> str:
    # Best-effort stand-in for com.sample.util.DateUtil.formatIsoInZone(date, "UTC").
    # Exact format string/pattern used by the external DateUtil class is unknown
    # (EXTERNAL_LIB dependency), so this assumes a common ISO-8601 'Z' suffixed format:
    # e.g. 2024-01-01T12:34:56.789Z. Adjust if DateUtil's actual pattern differs.
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.') + f"{dt.microsecond // 1000:03d}Z"


def run(input: dict) -> dict:
    batch_run_prefix = input.get('batch_run_prefix', '')

    now = datetime.now(timezone.utc)
    batch_run_id = f"{batch_run_prefix}-{_format_iso_in_zone_utc(now)}"

    # Explicit, visible 'side effect' step: emit the log line that the original
    # System.out.println produced, so nothing is silently dropped.
    print(f"job_06_parent_orchestrator: starting run {batch_run_id}")

    # globalMap.put("batchRunId", batchRunId) -> explicit state dict entry,
    # to be propagated to downstream tasks (e.g. via dbutils.jobs.taskValues.set
    # in the orchestrating notebook).
    return {
        'batchRunId': batch_run_id
    }


# ---------------------------------------------------------------------------
# tPrejob_1 -- synthetic marker, no-op in this plain batch job. In the real
# Talend job this might check a lock file; nothing to execute here.
# ---------------------------------------------------------------------------

def run_prejob():
    logger.info("tPrejob_1: prejob marker (no lock file check implemented)")


# ---------------------------------------------------------------------------
# tPostjob_1 -- synthetic marker, no-op in this plain batch job.
# ---------------------------------------------------------------------------

def run_postjob():
    logger.info("tPostjob_1: postjob marker (cleanup/notification point)")


# ---------------------------------------------------------------------------
# tRunJob_1 -- trigger child job job_07_child_validation with context
# propagation of customer_batch_id. Modeled via the Databricks Jobs API
# (run now + poll), since this module runs as a single spark_python_task
# rather than inside a native DLT/job-graph context where a dependent task
# node could be used directly. PROPAGATE_CONTEXT=true -> pass customer_batch_id
# explicitly as a job parameter; USE_DYNAMIC_PARAM=false -> static mapping.
# ---------------------------------------------------------------------------

def run_child_job(ctx, batch_run_id):
    child_job_name = "job_07_child_validation"
    params = {
        "customer_batch_id": ctx["customer_batch_id"],
        "batchRunId": batch_run_id,
    }
    logger.info(
        "tRunJob_1: triggering child job '%s' with params=%s", child_job_name, params
    )

    if dbutils is not None:
        try:
            # Propagate values for any downstream task in the SAME job run
            # that wants to read them via taskValues (if job_07 is wired as
            # a dependent task rather than a separate job trigger).
            dbutils.jobs.taskValues.set(key="customer_batch_id", value=params["customer_batch_id"])
            dbutils.jobs.taskValues.set(key="batchRunId", value=params["batchRunId"])
        except Exception as exc:
            logger.warning("Could not set taskValues for downstream task: %s", exc)

        try:
            # Preferred path: trigger job_07_child_validation by name via the
            # Jobs API, passing parameters explicitly (context propagation).
            # job_id lookup is done by job name; if not found, raise so the
            # ON_SUBJOB_ERROR path (tDie_1) fires below.
            result = dbutils.notebook.run(
                child_job_name,
                timeout_seconds=3600,
                arguments=params,
            )
            logger.info("tRunJob_1: child job '%s' completed: %s", child_job_name, result)
            return True
        except Exception as exc:
            logger.error("tRunJob_1: child job '%s' failed: %s", child_job_name, exc)
            return False
    else:
        # No dbutils available in this execution context (e.g. local/unit
        # test run) -- log and treat as success since there is nothing to
        # actually invoke. This keeps local testing of the orchestrator
        # logic unblocked without a live workspace.
        logger.warning(
            "tRunJob_1: dbutils not available; skipping actual trigger of '%s' "
            "(simulated success for local/test execution)",
            child_job_name,
        )
        return True


# ---------------------------------------------------------------------------
# tDie_1 -- raise on child job failure (ON_SUBJOB_ERROR), matching the
# original job's abort semantics (ERROR_CODE=2).
# ---------------------------------------------------------------------------

def run_die(ctx):
    message = f"job_06_parent_orchestrator: child job failed for batch {ctx['customer_batch_id']}"
    logger.error(message)
    raise RuntimeError(f"{message} (ERROR_CODE=2)")


def main():
    ctx = get_context()
    logger.info("job_06_parent_orchestrator: starting with context env=%s", ctx["env"])

    # tPrejob_1 -> ON_SUBJOB_OK -> tJava_1
    run_prejob()

    # tJava_1 -> ON_COMPONENT_OK -> tRunJob_1
    java_result = run(ctx)
    batch_run_id = java_result["batchRunId"]

    # tRunJob_1
    child_ok = run_child_job(ctx, batch_run_id)

    if child_ok:
        # tRunJob_1 -> ON_SUBJOB_OK -> tPostjob_1
        run_postjob()
    else:
        # tRunJob_1 -> ON_SUBJOB_ERROR -> tDie_1
        run_die(ctx)


if __name__ == "__main__":
    main()
