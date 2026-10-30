import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger("job_08_grandchild_archive")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Context variables (Talend context.* -> module-level constants, dev values)
# ---------------------------------------------------------------------------
# NOTE: context.source_dir's spec 'dev' value ("samples/talend_export_src/fixtures")
# is a path from the original local Talend job generation and is NOT a real
# path in this workspace. The actual input fixtures for this migration live,
# flat, at the UC Volume path below, using the original FILENAME's basename.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
CUSTOMER_BATCH_ID = "BATCH001"

# Output path: original FILENAME expression was
#   (String)context.source_dir + "/../fixtures_output/archive_" + (String)context.customer_batch_id + ".csv"
# i.e. a sibling "fixtures_output" directory next to source_dir. We mirror that
# relative structure under the same UC Volume samples root.
OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/samples/fixtures_output"
OUTPUT_FILENAME = f"archive_{CUSTOMER_BATCH_ID}.csv"
OUTPUT_PATH = f"{OUTPUT_DIR}/{OUTPUT_FILENAME}"

INPUT_FILENAME = f"{SOURCE_DIR}/archive_seed.csv"

# ---------------------------------------------------------------------------
# globalMap simulation: job07_validRowCount would have been published by
# job_07 via globalMap.put(...) in the original Talend chain. In this
# standalone batch job there is no live upstream job context to read it
# from, so we default to None (uncertainty noted in the spec itself).
# ---------------------------------------------------------------------------
JOB07_VALID_ROW_COUNT = None


# ---------------------------------------------------------------------------
# Translated tJava_1 block (verbatim, by block_id) -- SIDE_EFFECT/STATEFUL log
# ---------------------------------------------------------------------------
_state = {}


def run(input: dict) -> dict:
    """
    Translates tJava_1: archive-stamping block that reads a context variable
    (customer_batch_id, propagated from job_06) and a globalMap value
    (job07_validRowCount, published by job_07), then logs an archive message.

    Expected input keys (supplied by test harness / upstream orchestration):
      - "customer_batch_id": str  (Talend context.customer_batch_id)
      - "job07_validRowCount": Any (Talend globalMap.get("job07_validRowCount"))

    Output:
      - echoes batchId and validCount, plus the archive log line, so the
        side effect (println) is visible/assertable instead of silently lost.
    """
    # context.customer_batch_id -> explicit job parameter / input field
    batch_id = input.get("customer_batch_id")
    if batch_id is not None:
        batch_id = str(batch_id)

    # globalMap.get("job07_validRowCount") -> explicit state dict lookup
    valid_count = input.get("job07_validRowCount", _state.get("job07_validRowCount"))

    message = "job_08_grandchild_archive: archiving batch=" + (str(batch_id) if batch_id is not None else "None") + " validatedRows=" + (str(valid_count) if valid_count is not None else "None")

    # Java System.out.println -> explicit, visible logging side effect (never swallowed)
    logger.info(message)
    print(message)

    # Persist state in case downstream calls in the same run rely on it via globalMap semantics
    _state["customer_batch_id"] = batch_id
    _state["job07_validRowCount"] = valid_count

    return {
        "customer_batch_id": batch_id,
        "job07_validRowCount": valid_count,
        "archive_log_message": message,
    }


def main() -> None:
    # --- tJava_1 (ON_COMPONENT_OK -> tFileInputDelimited_1) ---
    result = run(
        {
            "customer_batch_id": CUSTOMER_BATCH_ID,
            "job07_validRowCount": JOB07_VALID_ROW_COUNT,
        }
    )
    logger.info("tJava_1 result: %s", result)

    # --- tFileInputDelimited_1 ---
    # HEADER=1 -> first row is the header; delimiter defaults to comma (spec
    # does not specify FIELDSEPARATOR, so Talend's default "," is used).
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", ",")
        .schema("archive_id STRING, payload STRING, created_at STRING")
        .load(INPUT_FILENAME)
    )

    # --- tFileOutputDelimited_1 (FLOW from tFileInputDelimited_1) ---
    # APPEND=true -> append mode. Spark's CSV writer writes to a directory of
    # part-files rather than a single literal file path, which is the native
    # equivalent of Talend's single-file append semantics on this platform.
    (
        df.write.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .mode("append")
        .save(OUTPUT_PATH)
    )

    logger.info("job_08_grandchild_archive: wrote archive output to %s", OUTPUT_PATH)


if __name__ == "__main__":
    main()
