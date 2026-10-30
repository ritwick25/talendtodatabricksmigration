import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_19_control_flow_recursive")

spark = SparkSession.builder.getOrCreate()

# --- Context variables ---
# source_dir is a path context var; per migration convention, real input fixtures
# live flat under this Volume path, not the original Talend-relative value.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
TARGET_SCHEMA = "dev_samples"  # context.target_schema (dev)
TABLE_NAME = "org_hierarchy_flat"
ERROR_OUTPUT_PATH = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output/org_hierarchy_errors.csv"

INPUT_FILE = f"{SOURCE_DIR}/org_hierarchy.csv"

REQUIRED_COLUMNS = ["employee_id", "manager_id", "name", "level_hint"]

# --- Translated tJavaRow_1 logic (verbatim from JAVAROUTINE agent) ---

_state = {
    "managerOf": None,
    "globalMap": {}
}


class CyclicHierarchyException(Exception):
    pass


def _load_manager_map(csv_path):
    """Loads employee_id -> manager_id map from a CSV with header, 2+ cols.
    Mirrors Java: split on ',' with -1 limit (keep trailing empties),
    swallow IOError and log to stderr, return whatever was loaded so far."""
    manager_map = {}
    try:
        with open(csv_path, "r") as f:
            header = f.readline()  # discarded, mirrors r.readLine() for header
            for line in f:
                line = line.rstrip("\n").rstrip("\r")
                parts = line.split(",")
                if len(parts) >= 2:
                    manager_map[parts[0]] = parts[1]
    except OSError as e:
        print(f"loadManagerMap: failed reading {csv_path}: {e}", file=sys.stderr)
    return manager_map


def _compute_depth(employee_id, manager_of, visiting):
    if employee_id in visiting:
        raise CyclicHierarchyException(f"cycle detected at {employee_id}")
    manager = manager_of.get(employee_id)
    if manager is None or manager == "" or manager == employee_id:
        return 0
    visiting.add(employee_id)
    try:
        return 1 + _compute_depth(manager, manager_of, visiting)
    finally:
        visiting.discard(employee_id)


def run(input: dict) -> dict:
    if _state["managerOf"] is None:
        source_dir = input.get("context_source_dir") or input.get("source_dir") or SOURCE_DIR
        csv_path = os.path.join(source_dir, "org_hierarchy.csv") if source_dir else "org_hierarchy.csv"
        _state["managerOf"] = _load_manager_map(csv_path)

    manager_of = _state["managerOf"]

    level_hint = input.get("level_hint")
    level_hint = "" if level_hint is None else level_hint

    if level_hint == "EXEC":
        level = 1
    elif level_hint == "DIR":
        level = 2
    elif level_hint == "MGR":
        level = 3
    elif level_hint == "IC":
        level = 4
    else:
        level = 5

    # Dead/constant conflictMatrix scan from original Java, preserved as inert no-op.
    conflict_matrix = [[False, False], [False, False]]
    anomaly = False
    for i in range(len(conflict_matrix)):
        for j in range(len(conflict_matrix[i])):
            if conflict_matrix[i][j]:
                anomaly = True
                break
        if anomaly:
            break

    employee_id = input.get("employee_id")

    try:
        depth = _compute_depth(employee_id, manager_of, set())
        output_row = {
            "employee_id": employee_id,
            "level_from_hint": level,
            "depth": depth,
            "error_message": "",
        }
    except CyclicHierarchyException as e:
        output_row = {
            "employee_id": employee_id,
            "level_from_hint": level,
            "depth": -1,
            "error_message": str(e),
        }
    finally:
        _state["globalMap"]["job19_lastEmployeeId"] = employee_id

    return output_row


def main():
    # --- tFileInputDelimited_1 ---
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load(INPUT_FILE)
    )

    # Fail fast if the source is missing any expected column, rather than
    # fabricating a placeholder column from an unrelated field (which would
    # silently corrupt the recursive manager-lookup keyed on employee_id/
    # manager_id). This replaces the previous buggy placeholder-column loop.
    missing_columns = [c for c in REQUIRED_COLUMNS if c not in raw_df.columns]
    if missing_columns:
        raise ValueError(
            f"Input file {INPUT_FILE} is missing required column(s): {missing_columns}. "
            f"Found columns: {raw_df.columns}"
        )

    rows = raw_df.select(*REQUIRED_COLUMNS).collect()

    # --- tJavaRow_1 applied row-by-row at driver level ---
    # Recursion + cross-row lazy state (managerOf map, globalMap) make this
    # fundamentally stateful/sequential; driver-side iteration preserves the
    # exact semantics of the original Java row processing.
    output_rows = []
    for r in rows:
        input_dict = {
            "employee_id": r["employee_id"],
            "manager_id": r["manager_id"],
            "name": r["name"],
            "level_hint": r["level_hint"],
            "source_dir": SOURCE_DIR,
        }
        output_rows.append(run(input_dict))

    logger.info("job19_lastEmployeeId (globalMap) = %s", _state["globalMap"].get("job19_lastEmployeeId"))

    out_schema = StructType([
        StructField("employee_id", StringType(), True),
        StructField("level_from_hint", IntegerType(), True),
        StructField("depth", IntegerType(), True),
        StructField("error_message", StringType(), True),
    ])

    processed_df = spark.createDataFrame(output_rows, schema=out_schema)

    # --- tFilterRow_1: FILTER_EXPRESSION = error_message.isEmpty() ---
    main_df = processed_df.filter(processed_df.error_message == "")
    reject_df = processed_df.filter(processed_df.error_message != "")

    # --- tDBOutput_1: INSERT into Delta table (no live JDBC connection) ---
    full_table_name = f"talend_migration.samples.{TABLE_NAME.lower()}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            employee_id STRING,
            level_from_hint INT,
            depth INT,
            error_message STRING
        ) USING DELTA
    """)

    main_df.write.format("delta").mode("append").saveAsTable(full_table_name)
    logger.info("Wrote %d rows to %s", main_df.count(), full_table_name)

    # --- tFileOutputDelimited_1 (REJECT branch) ---
    reject_count = reject_df.count()
    if reject_count > 0:
        (
            reject_df.coalesce(1)
            .write.format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .mode("overwrite")
            .save(os.path.dirname(ERROR_OUTPUT_PATH))
        )
        logger.info("Wrote %d rejected rows to %s", reject_count, os.path.dirname(ERROR_OUTPUT_PATH))
    else:
        logger.info("No rejected rows to write.")


if __name__ == "__main__":
    main()
