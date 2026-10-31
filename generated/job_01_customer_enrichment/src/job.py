import sys
import re
import zlib
import logging
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_01_customer_enrichment")

# ---------------------------------------------------------------------------
# Context variables (Talend context.* -> module constants).
# Per migration rules: file-path context vars point at the fixtures volume,
# not the literal 'dev' value from the spec (which is a local path, not real
# in this workspace). Non-path values keep their spec 'dev' literal.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
TARGET_SCHEMA = "dev_samples"  # context.target_schema dev value (non-path, literal ok)
MAX_RETRIES = 3  # context.max_retries dev value

# Delta table target (per migration rules: tDBOutput -> Delta table in
# talend_migration.samples, not a live JDBC connection).
DELTA_CATALOG_SCHEMA = "talend_migration.samples"
CUSTOMERS_ENRICHED_TABLE = f"{DELTA_CATALOG_SCHEMA}.customers_enriched"

CUSTOMERS_CSV = f"{SOURCE_DIR}/customers.csv"
REGIONS_CSV = f"{SOURCE_DIR}/regions.csv"
LOYALTY_TIERS_CSV = f"{SOURCE_DIR}/loyalty_tiers.csv"

# Rejected rows output -- kept inside the same fixtures volume tree since the
# original path was source_dir/../fixtures_output; we write under a sibling
# folder inside the UC Volume mount instead of a local/DBFS path.
REJECTED_OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output/customers_rejected"


def build_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# tJavaRow_1 translated block (verbatim logic from JAVAROUTINE agent),
# adapted here to run per-partition via mapInPandas (no raw RDD API).
# ---------------------------------------------------------------------------

def _safe(s):
    return "" if s is None else str(s)


def _normalize_whitespace(s):
    return re.sub(r"\s+", " ", s).strip()


def _title_case(s):
    return " ".join(w[:1].upper() + w[1:].lower() if w else w for w in s.split(" "))


def _mask_email(email):
    if email is None:
        return None
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if len(local) <= 1:
        masked_local = local
    else:
        masked_local = local[0] + "*" * (len(local) - 1)
    return masked_local + "@" + domain


def _crc32_hex(s):
    val = zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF
    return format(val, "08x")


def _compute_loyalty_value(points, tier_name):
    if points is None:
        return Decimal("0").quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    base = Decimal(points) * Decimal("0.01")
    tier = tier_name.lower() if tier_name is not None else None
    if tier == "platinum":
        multiplier = Decimal("1.50")
    elif tier == "gold":
        multiplier = Decimal("1.25")
    elif tier_name is None:
        multiplier = Decimal("0")
    else:
        multiplier = Decimal("1")
    return (base * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _tjavarow_1_partition(iterator):
    """
    Per-partition application of tJavaRow_1 semantics via mapInPandas.
    Dedupe state (processedCustomerIds) and the row counter are scoped to
    the partition being processed (mirrors the original static-state
    Java behavior as closely as possible in a distributed engine --
    true single-JVM global ordering cannot be reproduced on Spark, this
    is a documented limitation of the STATEFUL translation).
    """
    import pandas as pd

    processed_customer_ids = set()
    rows_processed_counter = 0

    for pdf in iterator:
        out_customer_id = []
        out_full_name = []
        out_email_masked = []
        out_loyalty_value = []
        out_row_checksum = []
        out_region_name = []
        out_tier_name = []

        for row in pdf.itertuples(index=False):
            raw_customer_id = row.customer_id
            customer_id = None if raw_customer_id is None else str(raw_customer_id)

            if customer_id is None or customer_id in processed_customer_ids:
                print(f"tJavaRow_1: skipping duplicate/null customer_id={customer_id}", file=sys.stderr)
                continue

            processed_customer_ids.add(customer_id)
            rows_processed_counter += 1

            first_name = _safe(row.first_name)
            last_name = _safe(row.last_name)
            full_name = _title_case(_normalize_whitespace(first_name + " " + last_name))

            loyalty_points = row.loyalty_points
            tier_name = row.tier_name
            loyalty_value = _compute_loyalty_value(loyalty_points, tier_name)

            email = row.email
            row_checksum_input = f"{customer_id}|{_safe(email)}|{loyalty_points if loyalty_points is not None else ''}"
            row_checksum = _crc32_hex(row_checksum_input)

            out_customer_id.append(raw_customer_id)
            out_full_name.append(full_name)
            out_email_masked.append(_mask_email(email))
            out_loyalty_value.append(loyalty_value)
            out_row_checksum.append(row_checksum)
            out_region_name.append(row.region_name)
            out_tier_name.append(tier_name)

        yield pd.DataFrame({
            "customer_id": out_customer_id,
            "full_name": out_full_name,
            "email_masked": out_email_masked,
            "loyalty_value": out_loyalty_value,
            "row_checksum": out_row_checksum,
            "region_name": out_region_name,
            "tier_name": out_tier_name,
        })


TJAVAROW_1_OUTPUT_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("email_masked", StringType(), True),
    StructField("loyalty_value", DecimalType(12, 2), True),
    StructField("row_checksum", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("tier_name", StringType(), True),
])


def run_subjob_1(spark: SparkSession) -> None:
    # --- tFileInputDelimited_1/2/3 ---
    customers = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .csv(CUSTOMERS_CSV)
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("loyalty_points", F.col("loyalty_points").cast("int"))
    )

    regions = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .csv(REGIONS_CSV)
    )

    loyalty_tiers = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .csv(LOYALTY_TIERS_CSV)
        .withColumn("min_points", F.col("min_points").cast("int"))
        .withColumn("max_points", F.col("max_points").cast("int"))
    )

    # --- tMap_1 ---
    # row2 (regions) lookup: inner join, load-once. Join key inferred as
    # country_code (shared column name); spec flags this as UNCERTAIN since
    # no explicit LOOKUP_row2_JOIN_COLUMN was present in the original XML.
    joined = customers.join(regions, on="country_code", how="inner")

    # row3 (loyalty_tiers) lookup: left outer join, reload-each-row in Talend
    # terms (doesn't change join semantics for a static batch source --
    # reload_mode only matters if the lookup source itself changes mid-run,
    # which it doesn't here). Spec flags the actual join key as UNCERTAIN
    # (looks like a range match on loyalty_points between min_points/max_points
    # rather than an equi-join on any shared column name). We implement the
    # range-based match since that's the only interpretation that makes sense
    # given loyalty_tiers' min_points/max_points columns.
    joined = joined.join(
        loyalty_tiers,
        on=(
            (joined["loyalty_points"] >= loyalty_tiers["min_points"])
            & (joined["loyalty_points"] <= loyalty_tiers["max_points"])
        ),
        how="left_outer",
    )

    mapped = joined.select(
        F.col("customer_id").alias("customer_id"),
        F.col("first_name").alias("first_name"),
        F.col("last_name").alias("last_name"),
        F.col("email").alias("email"),
        F.col("region_name").alias("region_name"),
        F.col("tier_name").alias("tier_name"),
        F.col("loyalty_points").alias("loyalty_points"),
    )

    mapped = mapped.withColumn(
        "is_valid",
        (F.col("customer_id").isNotNull())
        & (F.col("email").isNotNull())
        & (F.col("email").contains("@")),
    )

    mapped.createOrReplaceTempView("tmap_1_output")

    out_main = mapped.filter(F.col("is_valid")).drop("is_valid")
    out_reject = mapped.filter(~F.col("is_valid")).drop("is_valid")

    # --- tFileOutputDelimited_1 (REJECT branch) ---
    (
        out_reject.write.mode("overwrite")
        .option("header", "true")
        .option("delimiter", ",")
        .csv(REJECTED_OUTPUT_DIR)
    )

    # --- tJavaRow_1 (FLOW branch from out_main) ---
    enriched = out_main.mapInPandas(_tjavarow_1_partition, schema=TJAVAROW_1_OUTPUT_SCHEMA)
    enriched.createOrReplaceTempView("tjavarow_1_output")

    # Publish counter equivalent to globalMap.put("job01_rowsProcessed", ...).
    rows_processed = enriched.count()
    logger.info("job01_rowsProcessed=%s", rows_processed)

    # --- tDBOutput_1: UPDATE_OR_INSERT -> MERGE INTO Delta table ---
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CUSTOMERS_ENRICHED_TABLE} (
            customer_id INT,
            full_name STRING,
            email_masked STRING,
            loyalty_value DECIMAL(12,2),
            row_checksum STRING,
            region_name STRING,
            tier_name STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        MERGE INTO {CUSTOMERS_ENRICHED_TABLE} AS target
        USING tjavarow_1_output AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main():
    spark = build_spark()
    try:
        run_subjob_1(spark)
    except Exception:
        logger.exception("job_01_customer_enrichment failed")
        raise


if __name__ == "__main__":
    main()
