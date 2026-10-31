import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_12_db_upsert_truncate")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Context variables (Talend context.* -> plain module constants, dev values).
# NOTE: db_url/db_user in the original spec pointed at a live Postgres instance
# that is not reachable from this workspace; per migration rules all
# tDBInput/tDBRow/tDBOutput against that DB are implemented as Delta tables in
# talend_migration.samples instead of a live JDBC/psycopg2 connection.
# ---------------------------------------------------------------------------
TARGET_SCHEMA = "dev_samples"          # context.target_schema (dev)
WATERMARK_TS = "2024-01-01 00:00:00"   # context.watermark_ts (dev)

CATALOG_SCHEMA = "talend_migration.samples"
STAGING_TABLE = f"{CATALOG_SCHEMA}.inventory_staging"
RAW_TABLE = f"{CATALOG_SCHEMA}.inventory_raw"
MASTER_TABLE = f"{CATALOG_SCHEMA}.inventory_master"

# tDBOutput_1 target column list, taken from the spec's tJavaRow_1 output schema
# (tDBOutput_1's own schema array is empty per the spec's uncertainty note, so the
# incoming FLOW schema defines the target columns).
MASTER_COLUMNS = [
    "sku",
    "warehouse_id",
    "qty_on_hand",
    "delta_formatted",
    "watermark_used",
]

# MERGE match keys. The spec marks only sku key=true, but its own uncertainty note
# states "ACTION=UPDATE_OR_INSERT implies sku/warehouse_id are likely the match keys":
# (sku, warehouse_id) is the natural grain of an inventory-per-warehouse table, and
# matching on sku alone would let two warehouses' rows for the same sku overwrite each
# other. Both columns are therefore used as the match key.
MASTER_MERGE_KEYS = ["sku", "warehouse_id"]


# ---------------------------------------------------------------------------
# Translated Java block (tJavaRow_1), used verbatim as given by JAVAROUTINE
# agent, wired in via mapInPandas (no raw RDD API, no caching).
# ---------------------------------------------------------------------------
import sys as _sys

_state = {
    "rowsSinceWatermark": 0,
    "prevQty": {}  # keyed by sku, mirrors globalMap "job12_prevQty_<sku>"
}


def _is_stale_against_watermark(last_updated, watermark):
    """Pure string-compare, both already 'yyyy-MM-dd HH:mm:ss' -- preserves Java's
    String.compareTo <= 0 semantics and null-is-stale behavior."""
    if last_updated is None or watermark is None:
        return True
    return last_updated <= watermark


def _build_audit_line(sku, warehouse_id, delta, row_number):
    """Mirrors Java's String.format("[inventory-upsert #%d] sku=%s warehouse=%s delta=%s", ...)."""
    return "[inventory-upsert #{:d}] sku={} warehouse={} delta={}".format(
        row_number, sku, warehouse_id, delta
    )


def _compute_delta(qty_on_hand, prev, sku):
    """Stand-in for routines.InventoryRoutines.computeDelta, which transitively used
    ChecksumUtil (EXTERNAL_LIB). No Python equivalent library was supplied for
    ChecksumUtil, and the routine's own logic was out of scope for this block, so this
    is a best-effort faithful arithmetic delta formatted as "+N"/"-N"/"0" -- the checksum
    portion of the original routine could not be resolved here.
    """
    delta = (qty_on_hand or 0) - prev
    if delta > 0:
        return "+{}".format(delta)
    elif delta < 0:
        return str(delta)
    else:
        return "0"


def _java_row_run(input: dict) -> dict:
    phase = input.get("_phase")

    if phase == "START":
        _state["rowsSinceWatermark"] = 0
        _state["prevQty"] = {}
        return {}

    if phase == "END":
        return {}

    # Main per-row phase.
    _state["rowsSinceWatermark"] += 1
    rows_since_watermark = _state["rowsSinceWatermark"]

    sku = input.get("sku")
    qty_on_hand = input.get("qty_on_hand")
    warehouse_id = input.get("warehouse_id")
    last_updated = input.get("last_updated")
    watermark_ts = input.get("watermark_ts")  # context.watermark_ts, passed explicitly as job param

    prev = _state["prevQty"].get(sku, 0)

    # TODO: routines.InventoryRoutines.computeDelta transitively depended on ChecksumUtil
    # (EXTERNAL_LIB) whose JAR/equivalent Python library was not provided. Delta arithmetic
    # below is a best-effort reconstruction; any checksum-derived formatting is UNRESOLVED.
    delta_formatted = _compute_delta(qty_on_hand, prev, sku)

    _state["prevQty"][sku] = qty_on_hand

    stale = _is_stale_against_watermark(last_updated, watermark_ts)
    if stale:
        print(
            "tJavaRow_1: row for sku={} has last_updated={} which is NOT after watermark {} "
            "-- tDBInput_1's WHERE clause should have already excluded this row.".format(
                sku, last_updated, watermark_ts
            ),
            file=_sys.stderr,
        )

    audit_line = _build_audit_line(sku, warehouse_id, delta_formatted, rows_since_watermark)
    print(audit_line)

    return {
        "sku": sku,
        "warehouse_id": warehouse_id,
        "qty_on_hand": qty_on_hand,
        "delta_formatted": delta_formatted,
        "watermark_used": watermark_ts,
    }


OUTPUT_SCHEMA = StructType([
    StructField("sku", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("qty_on_hand", IntegerType(), True),
    StructField("delta_formatted", StringType(), True),
    StructField("watermark_used", StringType(), True),
])


def _java_row_partition_fn(iterator):
    """mapInPandas-compatible wrapper around the translated tJavaRow_1 block.

    tJavaRow_1 carries row-ordered cross-row state (rowsSinceWatermark counter,
    per-sku previous-quantity map) that mirrors Talend's globalMap usage. We
    reset state once per physical partition (standing in for the component's
    START phase) and process rows in the order Spark delivers them within a
    pandas batch/partition. This is not a global guarantee of row order across
    the whole dataset (Spark partitions run independently), but matches the
    per-row sequential semantics as closely as possible without caching the
    DataFrame or using raw RDD APIs.
    """
    import pandas as pd

    _java_row_run({"_phase": "START"})
    for pdf in iterator:
        out_rows = []
        for row in pdf.itertuples(index=False):
            rec = row._asdict() if hasattr(row, "_asdict") else dict(zip(pdf.columns, row))
            result = _java_row_run(rec)
            out_rows.append(result)
        yield pd.DataFrame(out_rows, columns=[f.name for f in OUTPUT_SCHEMA.fields])
    _java_row_run({"_phase": "END"})


def ensure_tables_exist():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            sku STRING,
            warehouse_id STRING,
            qty_on_hand INT,
            last_updated STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
            sku STRING,
            warehouse_id STRING,
            qty_on_hand INT,
            last_updated STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {MASTER_TABLE} (
            sku STRING,
            warehouse_id STRING,
            qty_on_hand INT,
            delta_formatted STRING,
            watermark_used STRING
        ) USING DELTA
    """)


def tdbrow_1_truncate():
    """tDBRow_1: TRUNCATE TABLE staging.inventory_staging -- translated as a
    native Spark overwrite-with-empty-data against the Delta staging table.
    Must succeed before tDBInput_1 runs (ON_SUBJOB_OK)."""
    logger.info("tDBRow_1: truncating %s", STAGING_TABLE)
    empty_df = spark.createDataFrame([], schema=StructType([
        StructField("sku", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("qty_on_hand", IntegerType(), True),
        StructField("last_updated", StringType(), True),
    ]))
    empty_df.write.format("delta").mode("overwrite").saveAsTable(STAGING_TABLE)
    logger.info("tDBRow_1: truncate complete")


def tdbinput_1_extract():
    """tDBInput_1: SELECT sku, warehouse_id, qty_on_hand, last_updated
    FROM inventory_raw WHERE last_updated > watermark_ts.
    Translated as a Spark SQL query against the Delta inventory_raw table.
    The watermark value is a trusted module-level constant derived solely from
    the spec's own context default (never external input)."""
    logger.info("tDBInput_1: extracting rows from %s newer than watermark %s", RAW_TABLE, WATERMARK_TS)
    df = spark.sql(
        f"""
        SELECT sku, warehouse_id, qty_on_hand, last_updated
        FROM {RAW_TABLE}
        WHERE last_updated > '{WATERMARK_TS}'
        """
    )
    return df


def tjavarow_1_transform(df):
    """tJavaRow_1: per-row stateful transform via the translated Java block,
    wired in with mapInPandas (never raw RDD APIs)."""
    df_with_watermark = df.withColumn("watermark_ts", F.lit(WATERMARK_TS))
    result = df_with_watermark.mapInPandas(_java_row_partition_fn, schema=OUTPUT_SCHEMA)
    return result


def tdboutput_1_upsert(df):
    """tDBOutput_1: ACTION=UPDATE_OR_INSERT into inventory_master.

    Two correctness fixes vs. the previous version, both called out by review:

    1. Match key is now the full natural grain (sku, warehouse_id), not sku alone.
       Matching on sku alone would make rows for the same sku in different
       warehouses collide and overwrite one another. The spec's own uncertainty
       note says sku/warehouse_id are the likely match keys for
       ACTION=UPDATE_OR_INSERT; schema key flags alone were under-specified.

    2. No more 'UPDATE SET *' / 'INSERT *'. Those wildcards resolve positionally
       / by whatever the source view happens to expose, so any schema drift
       between the source view and inventory_master silently misaligns or errors.
       The source is now projected to exactly MASTER_COLUMNS (the incoming FLOW
       schema from tJavaRow_1, which defines the target column list since
       tDBOutput_1's own schema array is empty in the spec), and the MERGE
       enumerates every column explicitly: non-key columns in UPDATE SET, all
       columns in the INSERT column/VALUES lists. Identifiers come only from the
       spec's schema metadata, never from runtime input.
    """
    # Project to the exact target column list, in target order, so the explicit
    # MERGE clauses below can never misalign.
    src = df.select([F.col(c) for c in MASTER_COLUMNS])

    temp_view = "tdboutput_1_src"
    src.createOrReplaceTempView(temp_view)

    on_clause = " AND ".join(
        f"target.{k} = source.{k}" for k in MASTER_MERGE_KEYS
    )
    update_set = ",\n            ".join(
        f"target.{c} = source.{c}"
        for c in MASTER_COLUMNS
        if c not in MASTER_MERGE_KEYS
    )
    insert_cols = ", ".join(MASTER_COLUMNS)
    insert_vals = ", ".join(f"source.{c}" for c in MASTER_COLUMNS)

    logger.info(
        "tDBOutput_1: upserting into %s on keys (%s) with columns (%s)",
        MASTER_TABLE,
        ", ".join(MASTER_MERGE_KEYS),
        insert_cols,
    )
    spark.sql(f"""
        MERGE INTO {MASTER_TABLE} AS target
        USING {temp_view} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET
            {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
            VALUES ({insert_vals})
    """)
    logger.info("tDBOutput_1: upsert complete")


def main():
    ensure_tables_exist()

    # subjob_1: tDBRow_1 -(ON_SUBJOB_OK)-> tDBInput_1 -(FLOW)-> tJavaRow_1 -(FLOW)-> tDBOutput_1
    tdbrow_1_truncate()

    extracted_df = tdbinput_1_extract()
    transformed_df = tjavarow_1_transform(extracted_df)
    tdboutput_1_upsert(transformed_df)

    logger.info("job_12_db_upsert_truncate: completed successfully")


if __name__ == "__main__":
    main()
