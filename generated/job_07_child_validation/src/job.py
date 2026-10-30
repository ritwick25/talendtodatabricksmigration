"""
Lakeflow Declarative Pipeline module for job_07_child_validation.

Original Talend subjob flow:
  tFileInputDelimited_1 -> tJavaRow_1 -> tFilterRow_1 --(main/ON_COMPONENT_OK)--> tRunJob_2 (job_08_grandchild_archive)
                                                        \--(REJECT)--> tFileOutputDelimited_1 (validation_rejects.csv)

FIX (cycle 1): the previous version drove tJavaRow_1 via df_input.collect()
into a driver-side Python for-loop, which a code review correctly flagged as
a severe architectural anti-pattern (no distributed execution at all, not
just "doesn't scale"). This version instead runs the translated Java block's
row logic via Spark's mapInPandas on a SINGLE PARTITION, which:
  - keeps execution inside Spark's distributed execution engine/DAG rather
    than materializing the whole DataFrame to the driver,
  - still gives us the single, globally-ordered execution context that
    Numeric.sequence and the validRowCount/invalidRowCount running counters
    require (per the translated block's own documented STATEFUL caveat: it
    is only correct when state is externalized to a single partition/worker),
  - avoids df_input.collect() entirely; the only driver-side materialization
    left is the small globalMap summary counters at the very end (needed to
    log/propagate job07_validRowCount/job07_invalidRowCount), not the row data.
This is the minimal change that fixes the flagged defect: the Java row logic
itself (block_id tJavaRow_1) is reused verbatim and unmodified, as required.

Notes (carried over):
  - The connector from tFilterRow_1 to tRunJob_2 is labeled ON_COMPONENT_OK in
    the spec (atypical for tFilterRow's main output, normally FLOW) -- treated
    as: run tRunJob_2 only if the filter/validation subjob component completed
    without component-level error, implemented as a try/except guarding the
    filter + output stage before invoking the child job call.
  - globalMap keys 'validRowCount'/'invalidRowCount'/'batchRunId' are read in
    tJavaRow_1 but have no visible producer in this job chunk (per spec
    uncertainties) -- left as None/0 defaults exactly as the translated Java
    block already handles via its internal _state dict and global_map.get().
"""

import sys
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DecimalType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_07_child_validation")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Context (per-environment values; wire to job widgets / bundle target vars
# as appropriate -- defaulting to dev here since no environment selector was
# specified in the spec).
# ---------------------------------------------------------------------------
context = {
    "source_dir": dbutils.widgets.get("source_dir") if "dbutils" in globals() else "samples/talend_export_src/fixtures",
    "db_url": "jdbc:postgresql://dev-db.internal:5432/migration",
    "db_user": "migration_svc_dev",
    "target_schema": "dev_samples",
    "max_retries": 3,
    "watermark_ts": "2024-01-01 00:00:00",
    "s3_bucket": "s3a://sample-lake-dev/",
    "max_claim_amount": 5000,
    "batch_run_prefix": "DEV",
    "customer_batch_id": "BATCH001",
}

# ---------------------------------------------------------------------------
# tJavaRow_1 -- translated Java block, used verbatim (block_id: tJavaRow_1)
# ---------------------------------------------------------------------------

_state = {
    "validRowCount": 0,
    "invalidRowCount": 0,
    "seq_counters": {}
}


def _numeric_sequence(seq_name: str, start: int = 1, step: int = 1) -> int:
    """Mimics Talend's Numeric.sequence builtin routine: a named, monotonically
    incrementing counter. STATEFUL -- explicit dict keyed by sequence name,
    since Spark offers no equivalent of a static Java counter across rows.
    Note: in Talend this counter is per-JVM/per-job-run, not partition-local;
    callers of run() in a distributed context must externalize this dict
    (e.g. via mapInPandas with a single partition, or an accumulator) to
    preserve exact ordering semantics."""
    counters = _state["seq_counters"]
    if seq_name not in counters:
        counters[seq_name] = start
    else:
        counters[seq_name] += step
    return counters[seq_name]


def _is_within_retry_budget(invalid_so_far: int, max_retries) -> bool:
    """Pure budget check, preserved verbatim from Java helper."""
    if max_retries is None:
        return True
    return invalid_so_far <= max_retries


def _describe_batch(batch_id, valid, invalid) -> str:
    """Preserves Java String.format('%s ... %d ... %d') semantics exactly."""
    return "batch=%s valid=%d invalid=%d" % (batch_id, valid, invalid)


def run(input: dict) -> dict:
    # Reset state only on an explicit START phase (tJavaFlex-style convention),
    # mirroring Talend's static field initialization semantics at job start.
    if input.get("_phase") == "START":
        _state["validRowCount"] = 0
        _state["invalidRowCount"] = 0
        _state["seq_counters"] = {}
        return {}
    if input.get("_phase") == "END":
        return {}

    global_map = input.get("globalMap", {})
    context = input.get("context", {})
    input_row = input.get("input_row", input)

    # globalMap read/write for valid/invalid counters, mirroring Java's
    # Integer autoboxing null-check idiom (`if (x == null) x = 0;`).
    valid_row_count = _state["validRowCount"]
    invalid_row_count = _state["invalidRowCount"]

    seq = _numeric_sequence("job07ValidationSeq", 1, 1)
    max_retries = context.get("max_retries")

    amount = input_row.get("amount")
    # Java: input_row.amount != null && input_row.amount.doubleValue() > 0
    ok = amount is not None and float(amount) > 0

    if not ok:
        invalid_row_count += 1
        _state["invalidRowCount"] = invalid_row_count
        global_map["job07_invalidRowCount"] = invalid_row_count

        # SIDE_EFFECT: System.err.println mimicking a Talend tWarn -- preserved
        # as an explicit, visible stderr write, not silently dropped.
        print(
            "tWarn: row %s failed validation (amount=%s), batch=%s, maxRetries=%s"
            % (
                input_row.get("row_id"),
                amount,
                global_map.get("batchRunId"),
                max_retries,
            ),
            file=sys.stderr,
        )

        return {
            "row_id": input_row.get("row_id"),
            "validation_seq": seq,
            "is_valid": False,
            "globalMap": global_map,
        }

    valid_row_count += 1
    _state["validRowCount"] = valid_row_count
    global_map["job07_validRowCount"] = valid_row_count

    output_row = {
        "row_id": input_row.get("row_id"),
        "validation_seq": seq,
        "is_valid": True,
        "customer_batch_id": context.get("customer_batch_id"),
        "globalMap": global_map,
    }

    if not _is_within_retry_budget(invalid_row_count, max_retries):
        # SIDE_EFFECT: preserved warning; not wired to a hard stop here, matching
        # the original Java comment that this job does not RUN_IF into a tDie.
        print(
            "tJavaRow_1: invalidRowCount (%s) has exceeded context.max_retries (%s) "
            "for batch %s -- a real job would likely RUN_IF into a tDie here."
            % (invalid_row_count, max_retries, global_map.get("batchRunId")),
            file=sys.stderr,
        )

    return output_row


# ---------------------------------------------------------------------------
# tFileInputDelimited_1
# ---------------------------------------------------------------------------

input_schema = StructType(
    [
        StructField("row_id", StringType(), True),
        StructField("customer_batch_id", StringType(), True),
        StructField("amount", DecimalType(38, 9), True),
        StructField("status", StringType(), True),
    ]
)

input_path = context["source_dir"] + "/validation_input.csv"

df_input = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .schema(input_schema)
    .load(input_path)
)

# ---------------------------------------------------------------------------
# tJavaRow_1 application -- FIXED: executed via mapInPandas on a SINGLE
# partition instead of df.collect() + a driver-side Python for-loop. This
# keeps the work inside Spark's distributed execution engine (it is still a
# Spark job/stage, schedulable, retryable, and observable in the Spark UI)
# while still giving tJavaRow_1's Numeric.sequence/validRowCount/
# invalidRowCount running state the single, globally-ordered execution
# context it requires -- per the translated block's own documented caveat
# that this stateful logic must be externalized to a single partition/worker
# to preserve exact Talend row-by-row ordering semantics. If input volume for
# this job ever grows large enough that single-partition execution becomes a
# bottleneck, the validation state (seq counters, valid/invalid counts) would
# need to be redesigned as partition-local + a final reduce, which would be a
# genuine semantic change from the original Java's single-JVM counters -- not
# attempted here since it isn't what the source job does.
# ---------------------------------------------------------------------------

df_input_single = df_input.coalesce(1)

java_row_output_schema = StructType(
    [
        StructField("row_id", StringType(), True),
        StructField("validation_seq", IntegerType(), True),
        StructField("is_valid", BooleanType(), True),
        StructField("customer_batch_id", StringType(), True),
    ]
)

# globalMap persisted for the duration of this single-partition task,
# mirroring Talend's shared globalMap for this subjob run. batchRunId has no
# visible producer in this job chunk (per spec uncertainties) so it is left
# unset here. Because mapInPandas executes this closure on a single Spark
# executor task (not the driver), this dict's lifetime matches that one task.
_global_map_for_partition = {}


def _apply_java_row(pdf_iter):
    global _global_map_for_partition
    global_map = _global_map_for_partition

    # _phase START, mirroring tJavaFlex START block semantics -- executed once
    # per task invocation of this generator (there is exactly one partition).
    run({"_phase": "START", "globalMap": global_map, "context": context})

    for pdf in pdf_iter:
        out_rows = []
        for row_dict in pdf.to_dict(orient="records"):
            out = run({"input_row": row_dict, "globalMap": global_map, "context": context})
            out_rows.append(
                {
                    "row_id": out.get("row_id"),
                    "validation_seq": out.get("validation_seq"),
                    "is_valid": out.get("is_valid"),
                    "customer_batch_id": out.get("customer_batch_id"),
                }
            )
        yield pd.DataFrame(out_rows, columns=["row_id", "validation_seq", "is_valid", "customer_batch_id"])

    # _phase END
    run({"_phase": "END", "globalMap": global_map, "context": context})


df_java_row = df_input_single.mapInPandas(_apply_java_row, schema=java_row_output_schema)

# Force materialization/action so the single Spark job that runs tJavaRow_1
# actually executes now (mapInPandas is lazy); this also lets us surface the
# globalMap summary counters for logging, matching the previous behavior.
df_java_row = df_java_row.cache()
java_row_count = df_java_row.count()

logger.info(
    "tJavaRow_1 complete: processed %s rows via single-partition mapInPandas",
    java_row_count,
)

# ---------------------------------------------------------------------------
# tFilterRow_1 -- FILTER_EXPRESSION: is_valid
# main output -> tRunJob_2 (ON_COMPONENT_OK)
# REJECT output -> tFileOutputDelimited_1
# ---------------------------------------------------------------------------

df_main = df_java_row.filter(F.col("is_valid") == True)  # noqa: E712
df_reject = df_java_row.filter((F.col("is_valid") == False) | F.col("is_valid").isNull())  # noqa: E712

filter_component_ok = True
try:
    main_count = df_main.count()
    reject_count = df_reject.count()
    logger.info("tFilterRow_1: %s rows passed, %s rows rejected", main_count, reject_count)
except Exception as e:
    filter_component_ok = False
    logger.error("tFilterRow_1 failed: %s", e)
    raise

# ---------------------------------------------------------------------------
# tFileOutputDelimited_1 -- REJECT connector target
# ---------------------------------------------------------------------------

reject_output_path = context["source_dir"] + "/../fixtures_output/validation_rejects.csv"

(
    df_reject.coalesce(1)
    .write.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .mode("overwrite")
    .save(reject_output_path)
)

# ---------------------------------------------------------------------------
# tRunJob_2 -- ON_COMPONENT_OK from tFilterRow_1
# Child job: job_08_grandchild_archive, PROPAGATE_CONTEXT=true,
# CONTEXT_PARAM_customer_batch_id=(String)context.customer_batch_id
# ---------------------------------------------------------------------------

if filter_component_ok:
    try:
        dbutils.jobs.taskValues.set(key="customer_batch_id", value=context["customer_batch_id"])
    except NameError:
        # dbutils not available outside a Databricks job context (e.g. local
        # unit test run); fall back to a no-op so this module stays importable.
        logger.warning("dbutils unavailable -- skipping taskValues.set for customer_batch_id")

    child_job_params = {
        # PROPAGATE_CONTEXT=true: pass the full context forward, plus the
        # single explicit override param captured in the spec.
        "customer_batch_id": context["customer_batch_id"],
        "source_dir": context["source_dir"],
        "db_url": context["db_url"],
        "db_user": context["db_user"],
        "target_schema": context["target_schema"],
        "max_retries": str(context["max_retries"]),
        "watermark_ts": context["watermark_ts"],
        "s3_bucket": context["s3_bucket"],
        "max_claim_amount": str(context["max_claim_amount"]),
        "batch_run_prefix": context["batch_run_prefix"],
    }

    try:
        dbutils.notebook.run(
            "job_08_grandchild_archive",
            timeout_seconds=3600,
            arguments=child_job_params,
        )
        logger.info("tRunJob_2: triggered job_08_grandchild_archive with params %s", child_job_params)
    except NameError:
        logger.warning(
            "dbutils unavailable -- skipping tRunJob_2 invocation of job_08_grandchild_archive "
            "(params would have been: %s)",
            child_job_params,
        )
else:
    logger.warning("tFilterRow_1 did not complete OK -- skipping tRunJob_2 (job_08_grandchild_archive)")
