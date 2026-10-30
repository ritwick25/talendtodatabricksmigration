import os
import csv
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_14_http_stub_integration")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Context variables (dev values). source_dir is overridden per the fixtures
# path rule -- the spec's literal context value is not a real workspace path.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
TARGET_SCHEMA = "dev_samples"
TABLE_NAME = "webhook_log"
FULL_TABLE_NAME = f"talend_migration.samples.{TABLE_NAME}"

INPUT_FILE = f"{SOURCE_DIR}/vendor_webhook_queue.csv"

# ---------------------------------------------------------------------------
# Translated tJavaRow_1 block (verbatim, from JAVAROUTINE agent), adapted
# only in how it's invoked (mapInPandas instead of raw RDD ops).
# ---------------------------------------------------------------------------
_SOURCE_DIR = None
_VENDOR_CACHE = None


def _load_vendor_directory(source_dir):
    """Mimic FixtureJdbcHelper: read a local CSV fixture (vendor_id, name, active) and
    build a lookup. On any IO error, behave like the Java version: treat as not found
    (i.e. isVendorActive returns False for everything)."""
    global _SOURCE_DIR, _VENDOR_CACHE
    if _VENDOR_CACHE is not None and _SOURCE_DIR == source_dir:
        return _VENDOR_CACHE

    path = os.path.join(source_dir, "vendor_directory.csv") if not source_dir.endswith("vendor_directory.csv") else source_dir
    lookup = {}
    try:
        with open(path, "r", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header, mirrors reader.readLine() for header
            for parts in reader:
                # Java: line.split(",", -1) keeps trailing empty strings; csv.reader does too
                if len(parts) >= 3 and parts[0] not in lookup:
                    # first match wins, mirroring the Java loop's early return on first match
                    lookup[parts[0]] = parts[2].strip().lower() == "true"
    except IOError:
        # mirrors the Java catch-all: on IOException, isVendorActive always returns False
        lookup = {}
        _SOURCE_DIR = source_dir
        _VENDOR_CACHE = (lookup, True)  # True = force-false mode (IO error occurred)
        return _VENDOR_CACHE

    _SOURCE_DIR = source_dir
    _VENDOR_CACHE = (lookup, False)
    return _VENDOR_CACHE


def _is_vendor_active(source_dir, vendor_id):
    lookup, force_false = _load_vendor_directory(source_dir)
    if force_false:
        return False
    return lookup.get(vendor_id, False)


def _post_json(path, json_body, timeout=5):
    """TODO: original Java used an HttpClientStub with no real network I/O -- it
    deterministically derived a status code from the payload shape instead of calling out.
    We preserve that EXACT stub behavior here (no real HTTP call) rather than silently
    switching to a live request, since there is no real endpoint/base URL supplied by
    the job context. If a real HTTP integration is required, replace this function body
    with a `requests.post(..., timeout=timeout)` call using a base URL from
    dbutils.secrets / job widgets, and remove the stub logic below."""
    if path is None or path.startswith("/events/unknown"):
        return 404, '{"error":"unknown endpoint"}'
    if json_body is None or json_body.strip() == "{}":
        return 422, '{"error":"empty payload"}'
    return 200, '{"status":"accepted"}'


def run(input: dict) -> dict:
    source_dir = input.get("context_source_dir") or input.get("source_dir") or "."

    vendor_id = input.get("vendor_id")
    vendor_active = _is_vendor_active(source_dir, vendor_id)

    endpoint_path = input.get("endpoint_path")
    payload_json = input.get("payload_json")
    status_code, _body = _post_json(endpoint_path, payload_json)

    return {
        "webhook_id": input.get("webhook_id"),
        "vendor_id": vendor_id,
        "vendor_active": vendor_active,
        "http_status": status_code,
        "http_ok": 200 <= status_code < 300,
    }


# ---------------------------------------------------------------------------
# mapInPandas wrapper: applies the tJavaRow_1 `run()` function row-by-row.
# ---------------------------------------------------------------------------
_OUTPUT_SCHEMA = StructType([
    StructField("webhook_id", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("vendor_active", BooleanType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("http_ok", BooleanType(), True),
])


def _apply_java_row(pdf_iter):
    for pdf in pdf_iter:
        records = pdf.to_dict(orient="records")
        out_records = []
        for rec in records:
            rec["context_source_dir"] = SOURCE_DIR
            out_records.append(run(rec))
        import pandas as pd
        yield pd.DataFrame(out_records, columns=[f.name for f in _OUTPUT_SCHEMA.fields])


def main():
    # --- tFileInputDelimited_1 ---
    df_input = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(
            StructType([
                StructField("webhook_id", StringType(), True),
                StructField("vendor_id", StringType(), True),
                StructField("endpoint_path", StringType(), True),
                StructField("payload_json", StringType(), True),
            ])
        )
        .load(INPUT_FILE)
    )

    # --- tJavaRow_1 (per-row, via mapInPandas; no raw RDD APIs) ---
    df_java = df_input.mapInPandas(_apply_java_row, schema=_OUTPUT_SCHEMA)

    # --- tDBOutput_1: insert into Delta table ---
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            webhook_id STRING,
            vendor_id STRING,
            vendor_active BOOLEAN,
            http_status INT,
            http_ok BOOLEAN
        ) USING DELTA
    """)

    df_java.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME)

    logger.info("Wrote %d rows to %s", df_java.count(), FULL_TABLE_NAME)


if __name__ == "__main__":
    main()
