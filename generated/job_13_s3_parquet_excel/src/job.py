import os
import re
import zlib
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
)
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_13_s3_parquet_excel")

spark = SparkSession.builder.getOrCreate()

# --- Context variables -----------------------------------------------------
# Per migration rules: the spec's 'dev' context value for source_dir is a
# local-generation artifact, not a real path in this workspace. Real input
# fixtures for this job live flat under the fixtures Volume below, using the
# original Talend FILENAME basenames.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"

# Non-path context values can be used literally from the spec's dev group.
S3_BUCKET = "s3a://sample-lake-dev/"

# Output locations: redirect legacy relative/local paths and s3a:// paths to a
# Unity Catalog Volume, since there's no real S3 connection in this workspace.
OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output"
RECONCILED_CSV_PATH = f"{OUTPUT_DIR}/employee_roster_reconciled.csv"
PARQUET_OUTPUT_PATH = f"{OUTPUT_DIR}/bronze/returns/part-00000.parquet"

# The original tJavaRow_1 used java.io.tmpdir for its per-row write/read/delete
# round-trip. On a distributed cluster each worker has its own local disk, so a
# node-local temp dir would make the round-trip invisible/non-durable outside
# the single task that performed it. Pointing the scratch directory at a Unity
# Catalog Volume keeps the write/read-back/delete a genuine, shared, durable
# filesystem side effect on every node, matching the original single-JVM intent.
RECON_SCRATCH_DIR = f"{OUTPUT_DIR}/job13_recon"


# --- tJavaRow_1 translated block (adapted I/O target, same logic) ----------
def _crc32_hex(s: str) -> str:
    data = s.encode("utf-8")
    crc = zlib.crc32(data) & 0xFFFFFFFF
    return format(crc, "08x")


def _looks_like_valid_emp_id(emp_id) -> bool:
    if emp_id is None:
        return False
    return re.fullmatch(r"E\d+", str(emp_id)) is not None


def _read_whole_file(path: str):
    try:
        with open(path, "rb") as f:
            return f.read().decode("utf-8")
    except OSError:
        return None


def _java_row_1_run(input_row: dict) -> dict:
    emp_id = input_row.get("emp_id")
    dept = input_row.get("dept")
    salary = input_row.get("salary")

    os.makedirs(RECON_SCRATCH_DIR, exist_ok=True)
    # Per-task-unique suffix so concurrent partitions never collide on the
    # same scratch file path.
    unique_suffix = f"{os.getpid()}_{id(input_row)}"
    tmp_file = os.path.join(RECON_SCRATCH_DIR, f"{emp_id}_{unique_suffix}.txt")

    line = f"{emp_id}|{dept}|{salary}"
    try:
        with open(tmp_file, "w", encoding="utf-8") as fw:
            fw.write(line)
    except OSError as e:
        # Original Java logged to System.err and continued; preserve that.
        print(f"tJavaRow_1: failed writing recon file for {emp_id}: {e}")

    read_back = _read_whole_file(tmp_file)
    checksum = _crc32_hex(read_back if read_back is not None else "")

    try:
        os.remove(tmp_file)
    except OSError:
        # Java's File.delete() silently returns false on failure; mirror that.
        pass

    if not _looks_like_valid_emp_id(emp_id):
        print(
            f"tJavaRow_1: emp_id '{emp_id}' does not match the expected E### shape -- "
            "proceeding anyway since this is a reconciliation step, not a validation gate."
        )

    return {
        "emp_id": emp_id,
        "dept": dept,
        "salary": salary,
        "recon_checksum": checksum,
        "recon_round_trip_ok": line == read_back,
    }


_JAVA_ROW_1_OUTPUT_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), True),
        StructField("dept", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("recon_checksum", StringType(), True),
        StructField("recon_round_trip_ok", BooleanType(), True),
    ]
)


@pandas_udf(_JAVA_ROW_1_OUTPUT_SCHEMA)
def _java_row_1_udf(emp_id: pd.Series, dept: pd.Series, salary: pd.Series) -> pd.DataFrame:
    results = []
    for e, d, s in zip(emp_id, dept, salary):
        s_val = None if pd.isna(s) else int(s)
        row = _java_row_1_run({"emp_id": e, "dept": d, "salary": s_val})
        results.append(row)
    return pd.DataFrame(results, columns=[f.name for f in _JAVA_ROW_1_OUTPUT_SCHEMA.fields])


def run_subjob_1_roster_reconciliation():
    excel_path = f"{SOURCE_DIR}/employee_roster.xlsx"
    # com.crealytics.spark.excel is a Scala/JAR-based Spark data source --
    # not attachable on serverless compute (confirmed: DATA_SOURCE_NOT_FOUND
    # on an actual run). Reading via pandas/openpyxl and handing Spark a
    # DataFrame it already understands avoids needing any JAR at all; the
    # 'Roster' sheet and header row match the original dataAddress option.
    pdf = pd.read_excel(excel_path, sheet_name="Roster", header=0, engine="openpyxl")
    df = spark.createDataFrame(pdf)

    # Normalize columns/types to match the spec's tFileInputExcel_1 schema.
    df = df.select(
        col("emp_id").cast(StringType()).alias("emp_id"),
        col("name").cast(StringType()).alias("name"),
        col("dept").cast(StringType()).alias("dept"),
        col("hire_date").cast(StringType()).alias("hire_date"),
        col("salary").cast(IntegerType()).alias("salary"),
    )

    reconciled = df.select(
        _java_row_1_udf(col("emp_id"), col("dept"), col("salary")).alias("recon")
    ).select("recon.*")

    (
        reconciled.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .save(RECONCILED_CSV_PATH)
    )
    logger.info("subjob_1_roster_reconciliation: wrote reconciled CSV to %s", RECONCILED_CSV_PATH)


def run_subjob_2_returns_parquet():
    # tFileInputJSON_2 stands in for a tFileInputParquet source per the spec's NOTE.
    json_path = f"{SOURCE_DIR}/returns_sample.json"
    df = spark.read.option("multiLine", "true").json(json_path)

    df = df.select(
        col("return_id").cast(StringType()).alias("return_id"),
        col("order_id").cast(StringType()).alias("order_id"),
        col("reason").cast(StringType()).alias("reason"),
        col("amount").cast("decimal(18,2)").alias("amount"),
        col("returned_at").cast(StringType()).alias("returned_at"),
    )

    # tFileOutputParquet_1 is explicitly noted as config-text-only (no real S3
    # write performed by the original synthetic export); we honor that by writing
    # to the Volume-backed output path rather than attempting a live s3a://
    # connection, since no S3 credentials/connection component exists here.
    df.write.format("parquet").mode("overwrite").save(PARQUET_OUTPUT_PATH)
    logger.info("subjob_2_returns_parquet: wrote parquet output to %s", PARQUET_OUTPUT_PATH)


def main():
    run_subjob_1_roster_reconciliation()
    run_subjob_2_returns_parquet()


if __name__ == "__main__":
    main()
