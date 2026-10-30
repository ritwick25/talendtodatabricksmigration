import re
import zlib
import logging
from decimal import Decimal, ROUND_HALF_UP

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_01_customer_enrichment")

# -----------------------------------------------------------------------------
# Context variables (Migration_Context group). In the Lakeflow pipeline these
# are surfaced as pipeline configuration / widget-style parameters rather than
# Talend context vars. Defaults below mirror the "dev" context values.
# -----------------------------------------------------------------------------
try:
    source_dir = spark.conf.get("pipeline.source_dir")
except Exception:
    source_dir = "samples/talend_export_src/fixtures"

try:
    target_schema = spark.conf.get("pipeline.target_schema")
except Exception:
    target_schema = "dev_samples"

# NOTE: tDBOutput_1/tDBInput targets are materialized as Delta tables in
# talend_migration.samples per migration policy -- there is no reachable
# instance of the original PostgreSQL database in this workspace, and the
# db_url/db_user context variables (and any JDBC credential) are therefore
# intentionally NOT used to open a live JDBC connection.
TARGET_CATALOG_SCHEMA = "talend_migration.samples"
TABLE_CUSTOMERS_ENRICHED = f"{TARGET_CATALOG_SCHEMA}.customers_enriched"

REJECTS_OUTPUT_PATH = f"{source_dir}/../fixtures_output/customers_rejected.csv"


# -----------------------------------------------------------------------------
# tJavaRow_1 -- translated Java block (verbatim, inserted at its pipeline
# point). Kept as-is; wrapped below for use as a Spark UDF per-row.
# -----------------------------------------------------------------------------

# Module-level state mirroring Talend's static fields across row calls.
_processedCustomerIds = None
_rowsProcessedCounter = None


def _safe(s):
    return "" if s is None else str(s)


def _normalize_whitespace(s):
    # com.sample.util.StringUtils.normalizeWhitespace -- EXTERNAL_LIB, no Python equivalent
    # found; best-effort reimplementation: collapse runs of whitespace to a single space
    # and strip leading/trailing whitespace (typical semantics for such helpers).
    return re.sub(r"\s+", " ", s).strip()


def _title_case(s):
    # com.sample.util.StringUtils.titleCase -- EXTERNAL_LIB, best-effort: capitalize each
    # whitespace-separated word's first letter, lowercase the rest (locale-naive, unlike
    # the original which may use a specific Locale -- cannot verify without the jar).
    return " ".join(w[:1].upper() + w[1:].lower() if w else w for w in s.split(" "))


def _mask_email(email):
    # com.sample.util.StringUtils.maskEmail -- EXTERNAL_LIB, best-effort: mask local part
    # keeping first character, e.g. j***@domain.com. Exact masking rule is unverified.
    if email is None:
        return None
    m = re.match(r"^(.)([^@]*)(@.*)$", email)
    if not m:
        return email
    return m.group(1) + "*" * max(len(m.group(2)), 1) + m.group(3)


def _crc32_hex(s):
    # com.sample.util.ChecksumUtil.crc32Hex -- EXTERNAL_LIB. CRC32 algorithm itself is
    # standard (zlib.crc32 matches java.util.zip.CRC32), but the exact hex formatting
    # (width/padding/case) of ChecksumUtil is unverified; assuming lowercase 8-char hex,
    # zero-padded, matching Java's common Long.toHexString(crc.getValue()) pattern is NOT
    # guaranteed -- using unpadded lowercase hex here as best effort.
    val = zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF
    return format(val, "x")


def _compute_loyalty_value(points, tier_name):
    # PURE helper -- faithfully translated BigDecimal logic using Decimal.
    if points is None:
        return Decimal("0").quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    base = Decimal(points) * Decimal("0.01")
    if tier_name is not None and tier_name.lower() == "platinum":
        multiplier = Decimal("1.50")
    elif tier_name is not None and tier_name.lower() == "gold":
        multiplier = Decimal("1.25")
    elif tier_name is None:
        multiplier = Decimal("0")
    else:
        multiplier = Decimal("1")
    return (base * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def run(input: dict) -> dict:
    global _processedCustomerIds, _rowsProcessedCounter

    if input.get("_phase") == "START":
        _processedCustomerIds = set()
        _rowsProcessedCounter = 0
        return {}

    if _processedCustomerIds is None:
        _processedCustomerIds = set()
    if _rowsProcessedCounter is None:
        _rowsProcessedCounter = 0

    if input.get("_phase") == "END":
        return {"job01_rowsProcessed": _rowsProcessedCounter}

    raw_customer_id = input.get("customer_id")
    customer_id = None if raw_customer_id is None else str(raw_customer_id)

    if customer_id is None or customer_id in _processedCustomerIds:
        # Side effect preserved explicitly: original used System.err.println + `return`
        # (skip row, i.e. filter it out of output). We return None to signal "no output
        # row produced"; caller/test harness should treat None as a dropped/reject row.
        print(f"tJavaRow_1: skipping duplicate/null customer_id={customer_id}")
        return None

    _processedCustomerIds.add(customer_id)
    _rowsProcessedCounter += 1

    first_name = _safe(input.get("first_name"))
    last_name = _safe(input.get("last_name"))
    full_name = _title_case(_normalize_whitespace(first_name + " " + last_name))

    loyalty_value = _compute_loyalty_value(input.get("loyalty_points"), input.get("tier_name"))

    row_checksum_input = f"{customer_id}|{_safe(input.get('email'))}|{input.get('loyalty_points')}"
    row_checksum = _crc32_hex(row_checksum_input)

    return {
        "customer_id": input.get("customer_id"),
        "full_name": full_name,
        "email_masked": _mask_email(input.get("email")),
        "loyalty_value": loyalty_value,
        "row_checksum": row_checksum,
        "region_name": input.get("region_name"),
        "tier_name": input.get("tier_name"),
        # Publish running counter to globalMap-equivalent on every row, matching Java's
        # globalMap.put on each successful row (not just at END).
        "job01_rowsProcessed": _rowsProcessedCounter,
    }


# This block is STATEFUL (dedupe set + counter) and has a SIDE_EFFECT (stderr
# log + globalMap-equivalent publish), so it is applied driver-side via
# toLocalIterator rather than as a distributed UDF, preserving the original's
# single-threaded, ordered semantics (Talend tJavaRow runs row-by-row on one
# thread per subjob by default).
def _apply_tjavarow_1(spark_df):
    rows_out = []
    run({"_phase": "START"})
    for row in spark_df.toLocalIterator():
        result = run(row.asDict())
        if result is not None:
            rows_out.append(result)
    end_result = run({"_phase": "END"})
    rows_processed = end_result.get("job01_rowsProcessed", 0)
    logger.info("tJavaRow_1: job01_rowsProcessed=%s", rows_processed)

    out_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("full_name", StringType(), True),
        StructField("email_masked", StringType(), True),
        StructField("loyalty_value", DecimalType(12, 2), True),
        StructField("row_checksum", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("tier_name", StringType(), True),
    ])

    cleaned = [
        {
            "customer_id": r.get("customer_id"),
            "full_name": r.get("full_name"),
            "email_masked": r.get("email_masked"),
            "loyalty_value": r.get("loyalty_value"),
            "row_checksum": r.get("row_checksum"),
            "region_name": r.get("region_name"),
            "tier_name": r.get("tier_name"),
        }
        for r in rows_out
    ]
    return spark.createDataFrame(cleaned, schema=out_schema)


# -----------------------------------------------------------------------------
# tFileInputDelimited_1 / _2 / _3 -- source reads
# -----------------------------------------------------------------------------

@dlt.view(name="customers_raw")
def customers_raw():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .schema(
            "customer_id INT, first_name STRING, last_name STRING, email STRING, "
            "signup_date STRING, country_code STRING, loyalty_points INT"
        )
        .load(f"{source_dir}/customers.csv")
    )


@dlt.view(name="regions_raw")
def regions_raw():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema("country_code STRING, region_name STRING")
        .load(f"{source_dir}/regions.csv")
    )


@dlt.view(name="loyalty_tiers_raw")
def loyalty_tiers_raw():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema("tier_code STRING, min_points INT, max_points INT, tier_name STRING")
        .load(f"{source_dir}/loyalty_tiers.csv")
    )


# -----------------------------------------------------------------------------
# tMap_1 -- joins (row2 inner on country_code, row3 left_outer on points range
# against loyalty_tiers' min_points/max_points, the only coherent join given
# tier_code is not present anywhere on the main flow -- flagged in spec
# uncertainties as inferred) + expression columns + main/reject filter split.
# -----------------------------------------------------------------------------

@dlt.view(name="tmap_1_joined")
def tmap_1_joined():
    row1 = dlt.read("customers_raw")
    row2 = dlt.read("regions_raw")
    row3 = dlt.read("loyalty_tiers_raw")

    # row2: Inner Join, unique match, LOAD_ONCE -- static lookup, no per-row
    # reload concern since regions.csv does not change mid-run.
    joined = row1.join(row2, on="country_code", how="inner")

    # row3: Left Outer Join, first match, RELOAD_AT_EACH_ROW. No explicit join
    # key parameter was present in the spec for this lookup (flagged as an
    # uncertainty); joining on the points range (min_points <= loyalty_points
    # <= max_points) is the only interpretation consistent with tier_name
    # being derived from loyalty tier bands rather than a shared key column.
    # RELOAD_AT_EACH_ROW has no distinct effect here since loyalty_tiers.csv
    # is static for the duration of this batch job.
    joined = joined.join(
        row3,
        on=(
            (joined["loyalty_points"] >= row3["min_points"])
            & (joined["loyalty_points"] <= row3["max_points"])
        ),
        how="left_outer",
    )

    is_valid_expr = (
        F.col("customer_id").isNotNull()
        & F.col("email").isNotNull()
        & F.col("email").contains("@")
    )

    return joined.withColumn("is_valid", is_valid_expr).select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "region_name",
        "tier_name",
        "loyalty_points",
        "is_valid",
    )


@dlt.view(name="tmap_1_out_main")
def tmap_1_out_main():
    return dlt.read("tmap_1_joined").filter(F.col("is_valid")).drop("is_valid")


@dlt.view(name="tmap_1_out_reject")
def tmap_1_out_reject():
    return dlt.read("tmap_1_joined").filter(~F.col("is_valid")).drop("is_valid")


# -----------------------------------------------------------------------------
# tFileOutputDelimited_1 -- reject sink
# -----------------------------------------------------------------------------

def write_rejects():
    df = dlt.read("tmap_1_out_reject") if False else None  # placeholder, replaced below


# Lakeflow views can't be read outside the pipeline graph for plain file
# writes, so the reject sink is materialized as a streaming table and then
# exported; here we do it as a direct batch write driven from the same
# module, executed after the DLT graph resolves tmap_1_out_reject logically
# via dlt.read within an explicit batch step (APPEND, matching original).
@dlt.table(name="customers_rejected_sink")
def customers_rejected_sink():
    return dlt.read("tmap_1_out_reject")


def export_rejects_to_csv():
    rejects_df = spark.read.table("LIVE.customers_rejected_sink")
    (
        rejects_df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .mode("overwrite")  # APPEND=false in spec -> overwrite, not append
        .save(REJECTS_OUTPUT_PATH)
    )


# -----------------------------------------------------------------------------
# tJavaRow_1 -- applied to the valid branch before the DB sink
# -----------------------------------------------------------------------------

@dlt.table(name="customers_enriched_staging")
def customers_enriched_staging():
    main_df = dlt.read("tmap_1_out_main")
    return _apply_tjavarow_1(main_df)


# -----------------------------------------------------------------------------
# tDBOutput_1 -- Delta table upsert (UPDATE_OR_INSERT), key = customer_id per
# upstream schema key=true flag (spec flags the explicit key param as absent,
# so this is the best-supported inference).
# -----------------------------------------------------------------------------

def upsert_customers_enriched():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CUSTOMERS_ENRICHED} (
            customer_id INT,
            full_name STRING,
            email_masked STRING,
            loyalty_value DECIMAL(12,2),
            row_checksum STRING,
            region_name STRING,
            tier_name STRING
        ) USING DELTA
    """)

    staging_df = spark.read.table("LIVE.customers_enriched_staging")
    staging_df.createOrReplaceTempView("tmp_customers_enriched_staging")

    spark.sql(f"""
        MERGE INTO {TABLE_CUSTOMERS_ENRICHED} AS target
        USING tmp_customers_enriched_staging AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# -----------------------------------------------------------------------------
# Orchestration entrypoint for the imperative (non-view/table-decorator) steps
# that must run after the declarative graph materializes: reject export and
# the final MERGE upsert. In a Lakeflow pipeline these run as a Python task
# downstream of the pipeline update (ON_SUBJOB_OK equivalent is the task's
# run_if = ALL_SUCCESS dependency on the pipeline-update task in the job
# graph, not inline branching here).
# -----------------------------------------------------------------------------

def main():
    export_rejects_to_csv()
    upsert_customers_enriched()


if __name__ == "__main__":
    main()
