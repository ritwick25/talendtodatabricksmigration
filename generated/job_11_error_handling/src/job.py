import logging
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DecimalType,
)
from pyspark.sql.functions import col, lit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_11_error_handling")


# --------------------------------------------------------------------------
# Context variables (per-environment values are resolved by the job/task
# parameters at submission time; defaults below mirror the Migration_Context
# "dev" values from the Talend context group).
# --------------------------------------------------------------------------
class Context:
    source_dir = "samples/talend_export_src/fixtures"
    db_url = "jdbc:postgresql://dev-db.internal:5432/migration"
    db_user = "migration_svc_dev"
    target_schema = "dev_samples"
    max_retries = 3
    watermark_ts = "2024-01-01 00:00:00"
    s3_bucket = "s3a://sample-lake-dev/"
    max_claim_amount = 5000
    batch_run_prefix = "DEV"
    customer_batch_id = "BATCH001"


context = Context()

# Talend tDie_1 threshold on globalMap("errorCount").
ERROR_COUNT_THRESHOLD = 5


# --------------------------------------------------------------------------
# tJavaRow_1 -- translated Java block (used verbatim as given by block_id).
# The row logic itself is pure per-row; only errorCount/job11_lastRowId are
# cross-row state. Below, the pure part is applied distributed via
# mapPartitions, and the globalMap-backed cumulative errorCount is derived
# with a Spark aggregate rather than a driver-side Python loop.
# --------------------------------------------------------------------------

_state = {
    "errorCount": 0,
    "job11_lastRowId": None,
}


class RowValidationException(Exception):
    """Mirrors the Java checked exception used to route unrecoverable rows."""
    pass


def _retry_with_backoff(level: int, attempt: int) -> int:
    """Recursive retry simulator: FAIL rows (level 2) recurse up to 3 times
    accumulating attempts, then raise; everything else returns immediately.
    Preserves Java recursion semantics exactly (same base cases, same order).
    """
    if level != 2:
        return attempt
    if attempt >= 3:
        raise RowValidationException(f"exceeded max retries at level={level}")
    return _retry_with_backoff(level, attempt + 1)


def _level_for_status(status_code):
    # Java switch on (status_code == null ? "" : status_code)
    code = status_code if status_code is not None else ""
    if code == "OK":
        return 0
    elif code == "RETRY":
        return 1
    elif code == "FAIL":
        return 2
    else:
        return 3


def _amount_is_negative(amount) -> bool:
    # Java: input_row.amount != null && input_row.amount.signum() < 0
    if amount is None:
        return False
    try:
        return Decimal(str(amount)).compare(Decimal(0)) < 0
    except Exception:
        return False


def _scan_for_anomaly(level: int, amount) -> bool:
    # Labeled-break scan over a 3x3 synthetic matrix; break on first match
    # at (i == level, j == negative-amount-flag). Equivalent to the Java
    # nested loop with `break scan;` -- short-circuits immediately on hit.
    j_target = 1 if _amount_is_negative(amount) else 0
    for i in range(3):
        for j in range(3):
            if i == level and j == j_target:
                return True
    return False


def run(input: dict) -> dict:
    """Per-row tJavaRow_1 body. `is_error` replaces the hidden globalMap
    increment: it is emitted as a column so the cumulative errorCount can be
    aggregated by Spark instead of mutated in a driver loop. Row-local
    try/catch/finally ordering is preserved exactly.
    """
    row_id = input.get("row_id")
    status_code = input.get("status_code")
    amount = input.get("amount")

    level = _level_for_status(status_code)
    anomaly = _scan_for_anomaly(level, amount)

    output_row = {}
    try:
        attempts = _retry_with_backoff(level, 0)
        output_row["row_id"] = row_id
        output_row["level"] = level
        output_row["anomaly"] = anomaly
        output_row["attempts"] = attempts
        output_row["error_message"] = ""
        output_row["is_error"] = 0
    except RowValidationException as e:
        output_row["row_id"] = row_id
        output_row["level"] = level
        output_row["anomaly"] = anomaly
        output_row["attempts"] = -1
        output_row["error_message"] = str(e)
        output_row["is_error"] = 1
    finally:
        # Talend finally: always records the last row id seen (per task here).
        output_row["last_row_id"] = row_id

    return output_row


PROCESSED_SCHEMA = StructType(
    [
        StructField("row_id", StringType(), True),
        StructField("level", IntegerType(), True),
        StructField("anomaly", BooleanType(), True),
        StructField("attempts", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("is_error", IntegerType(), True),
        StructField("last_row_id", StringType(), True),
    ]
)


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return SparkSession.builder.appName("job_11_error_handling").getOrCreate()


def read_input(spark: SparkSession):
    """tFileInputDelimited_1 -- header=1, default comma delimiter, UTF-8."""
    schema = StructType(
        [
            StructField("row_id", StringType(), True),
            StructField("status_code", StringType(), True),
            StructField("amount", DecimalType(38, 9), True),
        ]
    )
    path = f"{context.source_dir}/risky_input.csv"
    return (
        spark.read.option("header", "true")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(path)
    )


def apply_tjavarow(df):
    """tJavaRow_1 applied distributed via mapPartitions -- no driver-side
    collect(), so this scales with the input. The Java block's only cross-row
    state was globalMap("errorCount"), which is commutative/associative
    (a pure count of RowValidationException rows), so it is emitted per row
    as `is_error` and summed by Spark afterwards; this yields the identical
    final errorCount the sequential Talend loop would produce, without
    requiring row order or a single partition.
    """

    def _map_partition(rows):
        for r in rows:
            yield run(
                {
                    "row_id": r["row_id"],
                    "status_code": r["status_code"],
                    "amount": r["amount"],
                }
            )

    return df.rdd.mapPartitions(_map_partition).toDF(PROCESSED_SCHEMA)


def resolve_globalmap(processed_df):
    """Derive globalMap-equivalent values with Spark aggregates (one pass,
    no collect of the data itself):
      errorCount      = SUM(is_error)
      job11_lastRowId = MAX(row_id)  (deterministic stand-in for Talend's
                        'last row processed'; row_id is the schema key)
    """
    agg = processed_df.selectExpr(
        "COALESCE(SUM(is_error), 0) AS error_count",
        "MAX(row_id) AS last_row_id",
    ).first()
    error_count = int(agg["error_count"]) if agg is not None else 0
    last_row_id = agg["last_row_id"] if agg is not None else None
    return error_count, last_row_id


def write_to_db(df, table: str, action: str):
    """tDBOutput_1 -- INSERT action, preserved exactly."""
    db_password = dbutils.secrets.get(scope="talend-migration", key="db_password")  # noqa: F821
    write_mode = {
        "INSERT": "append",
        "TRUNCATE": "overwrite",
    }.get(action, "append")

    (
        df.drop("is_error", "last_row_id")
        .write.format("jdbc")
        .option("url", context.db_url)
        .option("dbtable", f"{context.target_schema}.{table}")
        .option("user", context.db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .option("queryTimeout", 300)
        .mode(write_mode)
        .save()
    )


def _die(message: str, error_code: int):
    """tDie_1 -- hard stop with the Talend message and ERROR_CODE 3."""
    logger.error(message)
    raise RuntimeError(f"[exit_code={error_code}] {message}")


def main():
    spark = build_spark()

    # tFileInputDelimited_1
    input_df = read_input(spark)

    # tJavaRow_1 (+ ON_COMPONENT_ERROR -> tWarn_1). tWarn_1 fires only for an
    # uncaught component-level failure of tJavaRow_1; RowValidationException is
    # caught inside the block and routed via errorCount instead. Spec flags
    # this path as possibly unreachable -- it is kept as a warn-only handler
    # that re-raises, matching Talend (tWarn logs, it does not swallow).
    try:
        processed_df = apply_tjavarow(input_df).cache()
        error_count, last_row_id = resolve_globalmap(processed_df)
    except Exception:
        logging.warning(
            "tJavaRow_1 raised a component error for row %s",
            _state.get("job11_lastRowId"),
        )
        raise

    _state["errorCount"] = error_count
    _state["job11_lastRowId"] = last_row_id

    # RUN_IF: tJavaRow_1 -> tDBOutput_1 when errorCount < 5
    # RUN_IF: tJavaRow_1 -> tDie_1   when errorCount >= 5
    # (mutually exclusive; in the Databricks Job graph these are two tasks
    # gated by run_if conditions on the tJavaRow_1 task's taskValues.)
    if error_count < ERROR_COUNT_THRESHOLD:
        write_to_db(processed_df, table="risky_rows_processed", action="INSERT")
        logger.info(
            "Wrote risky_rows_processed (errorCount=%d, lastRowId=%s)",
            error_count,
            last_row_id,
        )
    else:
        _die(
            "job_11_error_handling: errorCount exceeded threshold: "
            f"{error_count}",
            3,
        )

    spark.stop()


if __name__ == "__main__":
    main()
