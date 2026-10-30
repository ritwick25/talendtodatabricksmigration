import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_07_child_validation")

# ---------------------------------------------------------------------------
# Context variables (Talend context.* -> module-level constants).
# source_dir's 'dev' value is a synthetic-local path, NOT real here --
# fixtures for this workspace live flat under the shared fixtures volume.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
MAX_RETRIES = 3
CUSTOMER_BATCH_ID = "BATCH001"

INPUT_FILENAME = f"{SOURCE_DIR}/validation_input.csv"
# Original Talend path was context.source_dir + "/../fixtures_output/validation_rejects.csv";
# map this to a sibling output area in the same volume mount.
REJECTS_FILENAME = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output/validation_rejects.csv"

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Translated tJavaRow_1 block (verbatim logic, adapted to per-row dict calls).
# ---------------------------------------------------------------------------
_state = {
    "validRowCount": 0,
    "invalidRowCount": 0,
    "job07_validRowCount": None,
    "job07_invalidRowCount": None,
    "_seq_job07ValidationSeq": 0,
    "batchRunId": None,
}


def _numeric_sequence(name, start_value, step):
    key = f"_seq_{name}"
    if _state.get(key) is None or _state[key] == 0:
        _state[key] = start_value
    else:
        _state[key] += step
    return _state[key]


def _is_within_retry_budget(invalid_so_far, max_retries):
    if max_retries is None:
        return True
    return invalid_so_far <= max_retries


def _describe_batch(batch_id, valid, invalid):
    return "batch=%s valid=%d invalid=%d" % (batch_id, valid, invalid)


def tjavarow_1_run(input: dict) -> dict:
    global _state

    max_retries = input.get("context.max_retries", input.get("max_retries", MAX_RETRIES))
    customer_batch_id = input.get("context.customer_batch_id", input.get("customer_batch_id", CUSTOMER_BATCH_ID))
    batch_run_id = input.get("batchRunId", _state.get("batchRunId"))

    if batch_run_id is not None:
        _state["batchRunId"] = batch_run_id
    batch_run_id = _state.get("batchRunId")

    row_id = input.get("row_id")
    amount = input.get("amount")

    seq = _numeric_sequence("job07ValidationSeq", 1, 1)

    ok = amount is not None and float(amount) > 0

    if not ok:
        _state["invalidRowCount"] += 1
        _state["job07_invalidRowCount"] = _state["invalidRowCount"]
        print(
            f"tWarn: row {row_id} failed validation (amount={amount}), "
            f"batch={batch_run_id}, maxRetries={max_retries}",
            file=sys.stderr,
        )
        return {
            "row_id": row_id,
            "validation_seq": seq,
            "is_valid": False,
            "customer_batch_id": customer_batch_id,
        }

    _state["validRowCount"] += 1
    _state["job07_validRowCount"] = _state["validRowCount"]

    output = {
        "row_id": row_id,
        "validation_seq": seq,
        "is_valid": True,
        "customer_batch_id": customer_batch_id,
    }

    if not _is_within_retry_budget(_state["invalidRowCount"], max_retries):
        print(
            f"tJavaRow_1: invalidRowCount ({_state['invalidRowCount']}) has exceeded "
            f"context.max_retries ({max_retries}) for batch {batch_run_id} -- a real job "
            "would likely RUN_IF into a tDie here.",
            file=sys.stderr,
        )

    return output


def main():
    # -----------------------------------------------------------------
    # tFileInputDelimited_1
    # -----------------------------------------------------------------
    input_schema = StructType([
        StructField("row_id", StringType(), True),
        StructField("customer_batch_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
    ])

    df_in = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(input_schema)
        .load(INPUT_FILENAME)
    )

    # cast amount to decimal-friendly double for validation logic
    df_in = df_in.withColumn("amount", F.col("amount").cast("double"))

    # -----------------------------------------------------------------
    # tJavaRow_1: per-row validation via translated Java block.
    # Driver-side state (validRowCount/invalidRowCount/sequence) is
    # inherently sequential, so collect rows to the driver to preserve
    # exact Talend row-by-row semantics (small validation files expected).
    # -----------------------------------------------------------------
    rows = df_in.collect()
    out_records = []
    for r in rows:
        rec = tjavarow_1_run({
            "row_id": r["row_id"],
            "amount": r["amount"],
            "context.max_retries": MAX_RETRIES,
            "context.customer_batch_id": CUSTOMER_BATCH_ID,
        })
        out_records.append(rec)

    logger.info(
        "tJavaRow_1 complete: validRowCount=%s invalidRowCount=%s",
        _state["validRowCount"],
        _state["invalidRowCount"],
    )

    validated_schema = StructType([
        StructField("row_id", StringType(), True),
        StructField("validation_seq", IntegerType(), True),
        StructField("is_valid", BooleanType(), True),
        StructField("customer_batch_id", StringType(), True),
    ])
    df_validated = spark.createDataFrame(out_records, schema=validated_schema)

    # -----------------------------------------------------------------
    # tFilterRow_1: FILTER_EXPRESSION = "is_valid"
    # main (ON_COMPONENT_OK) -> valid rows; REJECT -> invalid rows.
    # -----------------------------------------------------------------
    df_valid = df_validated.filter(F.col("is_valid") == True)  # noqa: E712
    df_reject = df_validated.filter((F.col("is_valid") == False) | F.col("is_valid").isNull())  # noqa: E712

    # -----------------------------------------------------------------
    # REJECT branch -> tFileOutputDelimited_1
    # -----------------------------------------------------------------
    if df_reject.limit(1).count() > 0:
        (
            df_reject.write.format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .mode("overwrite")
            .save(REJECTS_FILENAME)
        )
        logger.info("Wrote %d rejected rows to %s", df_reject.count(), REJECTS_FILENAME)
    else:
        logger.info("No rejected rows to write.")

    # -----------------------------------------------------------------
    # ON_COMPONENT_OK branch -> tRunJob_2 (job_08_grandchild_archive),
    # only triggered if there is at least one valid row (component-ok
    # semantics), propagating customer_batch_id as a context param.
    # -----------------------------------------------------------------
    valid_count = df_valid.count()
    if valid_count > 0:
        logger.info(
            "tFilterRow_1 OK (%d valid rows) -- would invoke child job "
            "job_08_grandchild_archive with customer_batch_id=%s. "
            "Child job triggering is handled as a separate task dependency "
            "in the Databricks Job graph (run_if: ALL_SUCCESS), not inline here.",
            valid_count,
            CUSTOMER_BATCH_ID,
        )
        try:
            import dbutils  # type: ignore
            dbutils.jobs.taskValues.set(key="customer_batch_id", value=CUSTOMER_BATCH_ID)
        except Exception:
            # Not running in a context with dbutils.jobs.taskValues available
            # (e.g. local test run); task-value passing is wired at the job-
            # graph level between this task and job_08_grandchild_archive's task.
            pass
    else:
        logger.info("tFilterRow_1: no valid rows -- skipping downstream child job trigger.")


if __name__ == "__main__":
    main()
