"""
Lakeflow Declarative Pipeline / PySpark module generated from Talend job:
  job_10_file_iteration

Subjob 1 (subjob_1_file_iteration):
  tFileList_1 -> tFileInputDelimited_1 -> tJava_2 -> tFlowToIterate_1
    Iterates CSV files under <source_dir>/incoming, reads each with Spark,
    logs a running file counter (STATEFUL tJava block, restructured below),
    then converts the resulting flow into iteration context via
    tFlowToIterate_1 (driver-level loop over rows' region_code values).

Subjob 2 (subjob_2_region_foreach):
  tForeach_1 -> tJava_3
    Iterates a hardcoded list of region codes, accumulating them via a
    STATEFUL tJava block (restructured below).

Notes / uncertainties carried over from the SPEC:
  - tFlowToIterate_1 has no downstream consumer in the spec chunk given;
    we materialize its iteration values (region_code) and expose them as
    a driver-level list for any future downstream step to consume.
  - tForeach_1 has no incoming trigger shown; treated as its own
    independent subjob root per subjobs[] / trigger_order.
  - No terminal sink (tDBOutput/tLogRow) is present in the spec; this
    module logs results instead of writing anywhere, to avoid inventing
    an output target not present in the source job.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logger = logging.getLogger("job_10_file_iteration")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Context / parameters (per contexts[].Migration_Context, using 'dev' values
# as default; override via job/task parameters or widgets as needed).
# ---------------------------------------------------------------------------
def get_context_param(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


SOURCE_DIR = get_context_param("source_dir", "samples/talend_export_src/fixtures")

# NOTE: original Talend path was a local/relative filesystem path
# ("samples/talend_export_src/fixtures"). On Databricks this must resolve
# to a Unity Catalog Volume path. Adjust VOLUME_ROOT to the actual mount.
VOLUME_ROOT = "/Volumes/migration_catalog/migration_schema/migration_volume"
INCOMING_DIR = f"{VOLUME_ROOT}/{SOURCE_DIR}/incoming"
FILE_MASK = "*.csv"


# ---------------------------------------------------------------------------
# tJava_2 translation (block_id=tJava_2), inserted verbatim as given by the
# JAVAROUTINE agent. STATEFUL: module-level counter mirrors globalMap's
# "job10_totalFilesSeen" accumulator across tFileList_1 iterations.
# ---------------------------------------------------------------------------
_state = {"job10_totalFilesSeen": 0}


def tJava_2_run(input: dict) -> dict:
    # input is expected to carry the current file path for this tFileList iteration,
    # equivalent to globalMap.get("tFileList_1_CURRENT_FILEPATH").
    current_file = input.get("tFileList_1_CURRENT_FILEPATH")

    if input.get("_phase") == "START":
        # Reset running total at the start of a fresh iteration sequence,
        # analogous to a fresh globalMap in a new job run.
        _state["job10_totalFilesSeen"] = 0
        return {"job10_totalFilesSeen": _state["job10_totalFilesSeen"]}

    running_total = _state["job10_totalFilesSeen"]
    running_total = running_total + 1
    _state["job10_totalFilesSeen"] = running_total

    # Preserve the original System.out.println side effect as an explicit log statement
    # rather than silently dropping it.
    logger.info(
        "job_10_file_iteration: processing file #%s -> %s",
        running_total,
        current_file,
    )

    return {
        "job10_totalFilesSeen": running_total,
        "tFileList_1_CURRENT_FILEPATH": current_file,
    }


# ---------------------------------------------------------------------------
# tJava_3 translation (block_id=tJava_3), inserted verbatim as given by the
# JAVAROUTINE agent. STATEFUL: module-level list mirrors globalMap's
# "job10_regionsSeen" accumulator across tForeach_1 iterations.
# ---------------------------------------------------------------------------
_job10_regionsSeen = []


def tJava_3_run(input: dict) -> dict:
    global _job10_regionsSeen

    phase = input.get("_phase")
    if phase == "START":
        _job10_regionsSeen = []
        return {}

    if phase == "END":
        return {"job10_regionsSeen": list(_job10_regionsSeen)}

    # main phase: one call per tForeach_1 iteration
    region = input.get("tForeach_1_CURRENT_VALUE")
    _job10_regionsSeen.append(region)

    return {
        "region": region,
        "job10_regionsSeen": list(_job10_regionsSeen)
    }


# ---------------------------------------------------------------------------
# tFileInputDelimited_1 schema (region_code: String key, sales: Integer)
# ---------------------------------------------------------------------------
FILE_SCHEMA = StructType(
    [
        StructField("region_code", StringType(), True),
        StructField("sales", IntegerType(), True),
    ]
)


def list_incoming_files(directory: str, mask: str) -> list:
    """Equivalent of tFileList_1: list files under `directory` matching `mask`.

    Driver-level listing via dbutils.fs, since this is a small-cardinality
    directory listing (not a per-row Spark operation).
    """
    import fnmatch

    try:
        entries = dbutils.fs.ls(directory)  # type: ignore[name-defined]
        paths = [e.path for e in entries if not e.isDir()]
    except Exception as exc:
        logger.warning("Could not list directory %s: %s", directory, exc)
        return []

    matched = [p for p in paths if fnmatch.fnmatch(p.split("/")[-1], mask)]
    return sorted(matched)


def read_delimited_file(file_path: str):
    """Equivalent of tFileInputDelimited_1: HEADER=1, schema as above."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(FILE_SCHEMA)
        .load(file_path)
    )


def run_subjob_1_file_iteration() -> list:
    """
    subjob_1_file_iteration:
      tFileList_1 -> tFileInputDelimited_1 -> tJava_2 -> tFlowToIterate_1

    ON_SUBJOB_OK condition from tFileList_1 to tFileInputDelimited_1 is
    '(CURRENT_FILEPATH) != null', i.e. only proceed into the per-file branch
    when there IS a current file -- modeled here as simply iterating only
    over files actually found (an empty list means the condition is never
    satisfied, matching Talend's loop-exit behavior rather than an error).

    tFlowToIterate_1 (ITERATE_FROM=region_code) is modeled as materializing
    each file's region_code column values into a driver-level list per
    file, since no downstream consumer of the iterate output is declared
    in the spec.
    """
    tJava_2_run({"_phase": "START"})

    files = list_incoming_files(INCOMING_DIR, FILE_MASK)
    if not files:
        logger.info(
            "subjob_1_file_iteration: no files matched %s in %s; loop body skipped",
            FILE_MASK,
            INCOMING_DIR,
        )
        return []

    all_region_codes = []

    for current_filepath in files:
        # ON_SUBJOB_OK condition check: CURRENT_FILEPATH != null
        if current_filepath is None:
            continue

        df = read_delimited_file(current_filepath)

        # tJava_2: stateful per-file counter + log, triggered ON_COMPONENT_OK
        # after tFileInputDelimited_1 succeeds for this file.
        tJava_2_run({"tFileList_1_CURRENT_FILEPATH": current_filepath})

        # tFlowToIterate_1: iterate_from=region_code -> collect region_code
        # values for this file onto the driver (small-cardinality lookup
        # key column, safe to collect).
        region_codes = [row["region_code"] for row in df.select("region_code").collect()]
        all_region_codes.extend(region_codes)

    return all_region_codes


def run_subjob_2_region_foreach() -> list:
    """
    subjob_2_region_foreach:
      tForeach_1 (VALUES="US","EU","APAC") -> tJava_3

    tForeach_1 has no declared incoming trigger in the spec; it is run as
    its own independent subjob per subjobs[] / trigger_order.
    """
    tJava_3_run({"_phase": "START"})

    region_values = ["US", "EU", "APAC"]

    last_result = {}
    for current_value in region_values:
        last_result = tJava_3_run({"tForeach_1_CURRENT_VALUE": current_value})

    final = tJava_3_run({"_phase": "END"})
    seen = final.get("job10_regionsSeen", [])
    logger.info("subjob_2_region_foreach: regions seen = %s", seen)
    return seen


def main():
    logger.info("Starting job_10_file_iteration")

    region_codes_from_files = run_subjob_1_file_iteration()
    logger.info(
        "subjob_1_file_iteration complete: %d region_code values collected, total files seen=%s",
        len(region_codes_from_files),
        _state.get("job10_totalFilesSeen"),
    )

    regions_seen = run_subjob_2_region_foreach()
    logger.info(
        "subjob_2_region_foreach complete: %d regions accumulated",
        len(regions_seen),
    )

    logger.info("job_10_file_iteration finished successfully")


if __name__ == "__main__":
    main()
