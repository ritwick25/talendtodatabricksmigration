import sys
import logging
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_11_error_handling")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Context variables (dev values). source_dir is remapped to the actual
# fixtures volume in this workspace per migration convention; it is NOT the
# literal 'dev' context value from the original Talend job.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
TARGET_SCHEMA = "dev_samples"  # context.target_schema (dev) -- used only to build table name
MAX_RETRIES = 3  # context.max_retries (dev) -- informational, matches retry-with-backoff bound

# This dev workspace has no reachable instance of the external Postgres DB
# named in context.db_url, so tDBOutput writes to a Delta table instead of a
# live JDBC connection, per migration policy.
TARGET_TABLE = "talend_migration.samples.risky_rows_processed"

INPUT_FILE = f"{SOURCE_DIR}/risky_input.csv"


# ---------------------------------------------------------------------------
# Translated tJavaRow_1 block (verbatim from JAVAROUTINE agent), module-level
# state mirrors Talend's globalMap persistence across rows.
# ---------------------------------------------------------------------------
_state = {
    "errorCount": None,
    "job11_lastRowId": None,
}


class RowValidationException(Exception):
    """Custom checked exception used to route unrecoverable rows away from the main flow."""
    def __init__(self, msg):
        super().__init__(msg)


def _retry_with_backoff(level, attempt):
    """Recursive retry simulator: FAIL rows (level 2) recurse up to 3 times
    accumulating attempts, then throw; everything else returns immediately."""
    if level != 2:
        return attempt
    if attempt >= 3:
        raise RowValidationException(f"exceeded max retries at level={level}")
    return _retry_with_backoff(level, attempt + 1)


def run(input: dict) -> dict:
    phase = input.get("_phase") if isinstance(input, dict) else None
    if phase == "START":
        _state["errorCount"] = None
        _state["job11_lastRowId"] = None
        return {}
    if phase == "END":
        return {}

    if _state["errorCount"] is None:
        _state["errorCount"] = 0

    status_code = input.get("status_code") or ""
    if status_code == "OK":
        level = 0
    elif status_code == "RETRY":
        level = 1
    elif status_code == "FAIL":
        level = 2
    else:
        level = 3

    amount = input.get("amount")
    neg_col = 1 if (amount is not None and Decimal(str(amount)).compare(Decimal(0)) < 0) else 0

    anomaly = False
    for i in range(3):
        broke = False
        for j in range(3):
            if i == level and j == neg_col:
                anomaly = True
                broke = True
                break
        if broke:
            break

    row_id = input.get("row_id")

    try:
        attempts = _retry_with_backoff(level, 0)
        output_row = {
            "row_id": row_id,
            "level": level,
            "anomaly": anomaly,
            "attempts": attempts,
            "error_message": "",
        }
    except RowValidationException as e:
        _state["errorCount"] += 1
        output_row = {
            "row_id": row_id,
            "level": level,
            "anomaly": anomaly,
            "attempts": -1,
            "error_message": str(e),
        }
    finally:
        _state["job11_lastRowId"] = row_id

    output_row["errorCount"] = _state["errorCount"]
    output_row["job11_lastRowId"] = _state["job11_lastRowId"]
    return output_row


# ---------------------------------------------------------------------------
# tFileInputDelimited_1: read risky_input.csv
# ---------------------------------------------------------------------------
def read_input_file():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(
            StructType(
                [
                    StructField("row_id", StringType(), True),
                    StructField("status_code", StringType(), True),
                    StructField("amount", StringType(), True),
                ]
            )
        )
        .load(INPUT_FILE)
    )
    return df


# ---------------------------------------------------------------------------
# tJavaRow_1 application: driver-side sequential loop, since errorCount and
# job11_lastRowId are cross-row stateful accumulators (globalMap semantics)
# that must be updated in strict row order, matching the original Talend
# single-threaded row-by-row execution. Row volumes for this kind of
# control-flow/error-handling job are expected to be small, so collecting
# to the driver is appropriate and preserves exact stateful semantics that a
# distributed mapInPandas could not guarantee (partition/row ordering).
# ---------------------------------------------------------------------------
def apply_java_row(df):
    rows = df.collect()

    # Reset state at the start of the run, mirroring a START phase call.
    run({"_phase": "START"})

    output_rows = []
    last_row_id = None
    final_error_count = 0

    for r in rows:
        record = {
            "row_id": r["row_id"],
            "status_code": r["status_code"],
            "amount": r["amount"],
        }
        try:
            result = run(record)
        except Exception as e:
            # ON_COMPONENT_ERROR: route to tWarn_1 equivalent (logging.warning),
            # matching that the original error does not abort the job.
            last_row_id = _state.get("job11_lastRowId")
            logger.warning(
                "tJavaRow_1 raised a component error for row %s: %s",
                last_row_id,
                str(e),
            )
            continue

        output_rows.append(
            (
                result["row_id"],
                result["level"],
                result["anomaly"],
                result["attempts"],
                result["error_message"],
            )
        )
        final_error_count = result["errorCount"]
        last_row_id = result["job11_lastRowId"]

    run({"_phase": "END"})

    schema = StructType(
        [
            StructField("row_id", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("anomaly", BooleanType(), True),
            StructField("attempts", IntegerType(), True),
            StructField("error_message", StringType(), True),
        ]
    )
    out_df = spark.createDataFrame(output_rows, schema=schema)
    return out_df, final_error_count, last_row_id


# ---------------------------------------------------------------------------
# tDBOutput_1: write to Delta table (INSERT action -> append)
# ---------------------------------------------------------------------------
def write_output(out_df):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            row_id STRING,
            level INT,
            anomaly BOOLEAN,
            attempts INT,
            error_message STRING
        ) USING DELTA
        """
    )
    out_df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)


def main():
    df = read_input_file()
    out_df, error_count, last_row_id = apply_java_row(df)

    # tWarn_1 is wired via ON_COMPONENT_ERROR and handled inline above per row;
    # this is a final summary warning if any errors accumulated overall.
    if error_count and error_count > 0:
        logger.warning(
            "tJavaRow_1 raised a component error for row %s", last_row_id
        )

    # RUN_IF: tJavaRow_1 -> tDBOutput_1 when errorCount < 5
    if error_count < 5:
        write_output(out_df)
    else:
        # RUN_IF: tJavaRow_1 -> tDie_1 when errorCount >= 5
        raise RuntimeError(
            f"job_11_error_handling: errorCount exceeded threshold: {error_count}"
        )


if __name__ == "__main__":
    main()
