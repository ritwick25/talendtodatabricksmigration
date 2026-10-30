import logging
import os
import posixpath
import re
import sys
import zlib
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType
)

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_01_customer_enrichment")

# --------------------------------------------------------------------------
# Context variables (per-environment). Resolved via job widgets/params,
# falling back to the 'dev' values from the spec's context group.
# --------------------------------------------------------------------------
dbutils_available = True
try:
    dbutils  # noqa: F821
except NameError:
    dbutils_available = False


def _get_param(name, default):
    if dbutils_available:
        try:
            return dbutils.widgets.get(name)  # noqa: F821
        except Exception:
            return default
    return default


source_dir = _get_param("source_dir", "samples/talend_export_src/fixtures")
db_url = _get_param("db_url", "jdbc:postgresql://dev-db.internal:5432/migration")
db_user = _get_param("db_user", "migration_svc_dev")
target_schema = _get_param("target_schema", "dev_samples")

# --------------------------------------------------------------------------
# Unity Catalog Volume root for all job-managed file I/O. The original Talend
# job wrote rejects to "<source_dir>/../fixtures_output/customers_rejected.csv";
# that relative '..' traversal out of an input parameter is not permitted here.
# Instead, all writes are rooted at an explicit, validated UC Volume path and
# the leaf directory name ('fixtures_output') is preserved.
# --------------------------------------------------------------------------
VOLUME_ROOT = _get_param(
    "output_volume_root", "/Volumes/main/talend_migration/job_01_customer_enrichment"
)


def _resolve_volume_path(*parts):
    """Join parts under VOLUME_ROOT and assert the result stays inside it.

    Rejects absolute components, '..' segments, and anything that normalizes to
    a location outside the configured Unity Catalog Volume.
    """
    if not VOLUME_ROOT.startswith("/Volumes/"):
        raise ValueError(
            f"Output root must be a Unity Catalog Volume path (/Volumes/...), got: {VOLUME_ROOT!r}"
        )
    root = posixpath.normpath(VOLUME_ROOT)
    for p in parts:
        if p is None or p == "":
            raise ValueError("Empty path component is not allowed")
        if p.startswith("/") or ".." in p.split("/"):
            raise ValueError(f"Unsafe path component rejected: {p!r}")
    candidate = posixpath.normpath(posixpath.join(root, *parts))
    if candidate != root and not candidate.startswith(root + "/"):
        raise ValueError(
            f"Resolved path {candidate!r} escapes the configured volume root {root!r}"
        )
    return candidate


db_password = dbutils.secrets.get(scope="talend-migration", key="db_password") if dbutils_available else None

NETWORK_TIMEOUT_MS = 30000

# --------------------------------------------------------------------------
# tJavaRow_1 translation (verbatim from JAVAROUTINE agent, by block_id)
# --------------------------------------------------------------------------
_processed_customer_ids = None
_rows_processed_counter = None
_global_map = {}


def _safe(s):
    return "" if s is None else str(s)


def _normalize_whitespace(s):
    # com.sample.util.StringUtils.normalizeWhitespace: collapse runs of whitespace to a single space, trim.
    return re.sub(r"\s+", " ", s).strip()


def _title_case(s):
    # com.sample.util.StringUtils.titleCase: capitalize first letter of each word, lowercase rest.
    return " ".join(w[:1].upper() + w[1:].lower() if w else w for w in s.split(" "))


def _mask_email(email):
    # com.sample.util.StringUtils.maskEmail: EXTERNAL_LIB with no public spec available.
    # Best-effort reimplementation: keep first char of local part, mask the rest, keep domain.
    if email is None:
        return None
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if len(local) <= 1:
        masked_local = "*" * len(local)
    else:
        masked_local = local[0] + "*" * (len(local) - 1)
    return masked_local + "@" + domain


def _crc32_hex(s):
    # com.sample.util.ChecksumUtil.crc32Hex: Java CRC32 produces an unsigned 32-bit value;
    # zlib.crc32 in Python 3 also returns an unsigned 32-bit int, so hex formatting matches.
    data = s.encode("utf-8")
    return format(zlib.crc32(data) & 0xFFFFFFFF, "08x")


def _compute_loyalty_value(points, tier_name):
    # Pure BigDecimal valuation translated to Decimal, HALF_UP, scale 2 -- matches
    # java.math.RoundingMode.HALF_UP semantics (Python's ROUND_HALF_UP is equivalent for this case).
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
    global _processed_customer_ids, _rows_processed_counter, _global_map

    phase = input.get("_phase") if isinstance(input, dict) else None

    if phase == "START":
        _processed_customer_ids = set()
        _rows_processed_counter = 0
        _global_map = {}
        return {}

    if phase == "END":
        return {"job01_rowsProcessed": _rows_processed_counter}

    # Lazily init state in case run() is called without an explicit START phase
    # (e.g. direct unit-test invocation) -- mirrors the Java's own lazy-init guard.
    if _processed_customer_ids is None:
        _processed_customer_ids = set()
    if _rows_processed_counter is None:
        _rows_processed_counter = 0

    raw_customer_id = input.get("customer_id")
    customer_id = None if raw_customer_id is None else str(raw_customer_id)

    if customer_id is None or customer_id in _processed_customer_ids:
        # Side effect preserved explicitly: original logged to stderr and dropped the row
        # (no reject output in source -- Talend tJavaRow 'return;' with no reject flow wired).
        print(f"tJavaRow_1: skipping duplicate/null customer_id={customer_id}", file=sys.stderr)
        return None

    _processed_customer_ids.add(customer_id)
    _rows_processed_counter += 1
    _global_map["job01_rowsProcessed"] = _rows_processed_counter

    first_name = _safe(input.get("first_name"))
    last_name = _safe(input.get("last_name"))
    full_name = _title_case(_normalize_whitespace(first_name + " " + last_name))

    loyalty_points = input.get("loyalty_points")
    tier_name = input.get("tier_name")
    loyalty_value = _compute_loyalty_value(loyalty_points, tier_name)

    row_checksum = _crc32_hex(
        customer_id + "|" + _safe(input.get("email")) + "|" + _safe(loyalty_points)
    )

    return {
        "customer_id": input.get("customer_id"),
        "full_name": full_name,
        "email_masked": _mask_email(input.get("email")),
        "loyalty_value": loyalty_value,
        "row_checksum": row_checksum,
        "region_name": input.get("region_name"),
        "tier_name": tier_name,
        "job01_rowsProcessed": _rows_processed_counter,
    }


# tJavaRow_1 is STATEFUL (dedupe set + row counter) and must run as a single-partition,
# ordered, driver-side pass to exactly preserve Talend's row-by-row semantics -- it is
# therefore applied via a collected row list rather than a distributed UDF.
def _apply_tjavarow(rows):
    run({"_phase": "START"})
    out_rows = []
    for r in rows:
        result = run(r.asDict())
        if result is not None:
            result.pop("job01_rowsProcessed", None)
            out_rows.append(result)
    end_result = run({"_phase": "END"})
    logger.info("tJavaRow_1: rowsProcessed=%s", end_result.get("job01_rowsProcessed"))
    return out_rows


# --------------------------------------------------------------------------
# subjob_1
# --------------------------------------------------------------------------
def run_subjob_1():
    # tFileInputDelimited_1 : customers.csv
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("loyalty_points", IntegerType(), True),
    ])
    df_customers = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .schema(customers_schema)
        .load(f"{source_dir}/customers.csv")
    )

    # tFileInputDelimited_2 : regions.csv (lookup, inner join, load-once, key=country_code)
    regions_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("region_name", StringType(), True),
    ])
    df_regions = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema(regions_schema)
        .load(f"{source_dir}/regions.csv")
    )

    # tFileInputDelimited_3 : loyalty_tiers.csv (lookup, left outer join, first match, key=tier_code)
    # NOTE: reload_mode RELOAD_AT_EACH_ROW only matters for correctness if the lookup
    # source changes mid-run; in this batch job it is loaded once per run, which is
    # functionally equivalent since loyalty_tiers.csv is static for the duration.
    tiers_schema = StructType([
        StructField("tier_code", StringType(), True),
        StructField("min_points", IntegerType(), True),
        StructField("max_points", IntegerType(), True),
        StructField("tier_name", StringType(), True),
    ])
    df_tiers = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema(tiers_schema)
        .load(f"{source_dir}/loyalty_tiers.csv")
    )

    # ---- tMap_1 ----
    # Per spec's tmaps[] entry, row2 (regions) joins on country_code (inner), row3 (tiers)
    # joins on tier_code (left outer, first match). df_customers carries no tier_code column,
    # so tier_code is derived deterministically from loyalty_points by assigning each customer
    # to the tier whose [min_points, max_points] range contains their loyalty_points, picking
    # the lowest min_points as the single deterministic "first match" tie-breaker (stable, not
    # dependent on monotonically_increasing_id or partition layout).
    df_joined_regions = df_customers.join(df_regions, on="country_code", how="inner")

    from pyspark.sql.window import Window

    df_candidate_tiers = df_joined_regions.alias("c").join(
        df_tiers.alias("t"),
        on=(
            (F.col("c.loyalty_points") >= F.col("t.min_points"))
            & (F.col("c.loyalty_points") <= F.col("t.max_points"))
        ),
        how="left_outer",
    )

    window_first_match = Window.partitionBy("c.customer_id").orderBy(
        F.col("t.min_points").asc_nulls_last(), F.col("t.tier_code").asc_nulls_last()
    )
    df_candidate_tiers = df_candidate_tiers.withColumn(
        "_rn", F.row_number().over(window_first_match)
    )
    df_joined = df_candidate_tiers.filter(F.col("_rn") == 1).drop("_rn")

    df_mapped = df_joined.select(
        F.col("c.customer_id").alias("customer_id"),
        F.col("c.first_name").alias("first_name"),
        F.col("c.last_name").alias("last_name"),
        F.col("c.email").alias("email"),
        F.col("c.region_name").alias("region_name"),
        F.col("t.tier_name").alias("tier_name"),
        F.col("c.loyalty_points").alias("loyalty_points"),
    )

    df_mapped = df_mapped.withColumn(
        "is_valid",
        F.col("customer_id").isNotNull()
        & F.col("email").isNotNull()
        & F.col("email").contains("@"),
    )

    df_main = df_mapped.filter(F.col("is_valid")).drop("is_valid")
    df_reject = df_mapped.filter(~F.col("is_valid")).drop("is_valid")

    # tFileOutputDelimited_1 : rejects.
    # Written to a validated Unity Catalog Volume path -- no '..' traversal, no path
    # derived by escaping the input source_dir. APPEND=false -> mode("overwrite").
    reject_path = _resolve_volume_path("fixtures_output", "customers_rejected.csv")
    logger.info("tFileOutputDelimited_1: writing rejects to %s", reject_path)
    (
        df_reject.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .mode("overwrite")
        .save(reject_path)
    )

    # tJavaRow_1 : stateful dedupe + enrichment, applied driver-side in customer_id order
    # to preserve exact Talend row-by-row semantics (static dedupe set, running counter).
    ordered_rows = df_main.orderBy("customer_id").collect()
    enriched_rows = _apply_tjavarow(ordered_rows)

    enriched_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("full_name", StringType(), True),
        StructField("email_masked", StringType(), True),
        StructField("loyalty_value", DecimalType(12, 2), True),
        StructField("row_checksum", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("tier_name", StringType(), True),
    ])
    df_enriched = spark.createDataFrame(enriched_rows, schema=enriched_schema)

    # tDBOutput_1 : UPDATE_OR_INSERT -> MERGE INTO customers_enriched
    jdbc_props = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
        "connectTimeout": str(NETWORK_TIMEOUT_MS),
        "socketTimeout": str(NETWORK_TIMEOUT_MS),
    }
    staging_table = f"{target_schema}._stg_customers_enriched_job01"
    target_table = f"{target_schema}.customers_enriched"

    (
        df_enriched.write.format("jdbc")
        .option("url", db_url)
        .option("dbtable", staging_table)
        .options(**jdbc_props)
        .mode("overwrite")
        .save()
    )

    import psycopg2

    # Explicit try/except around the connection + MERGE execution: on failure, log the
    # error with context and re-raise so the Databricks job task is marked failed -- this
    # is a hard DB write with no reject-flow routing defined in the spec for tDBOutput_1,
    # so failure must abort the job rather than being swallowed.
    conn = None
    try:
        conn = psycopg2.connect(
            dsn=db_url.replace("jdbc:", ""),
            user=db_user,
            password=db_password,
            connect_timeout=30,
        )
        with conn.cursor() as cur:
            merge_sql = f"""
                MERGE INTO {target_table} AS tgt
                USING {staging_table} AS src
                ON tgt.customer_id = src.customer_id
                WHEN MATCHED THEN UPDATE SET
                    full_name = src.full_name,
                    email_masked = src.email_masked,
                    loyalty_value = src.loyalty_value,
                    row_checksum = src.row_checksum,
                    region_name = src.region_name,
                    tier_name = src.tier_name
                WHEN NOT MATCHED THEN INSERT (
                    customer_id, full_name, email_masked, loyalty_value,
                    row_checksum, region_name, tier_name
                ) VALUES (
                    src.customer_id, src.full_name, src.email_masked, src.loyalty_value,
                    src.row_checksum, src.region_name, src.tier_name
                );
            """
            cur.execute(merge_sql)
        conn.commit()
    except Exception:
        logger.exception("tDBOutput_1: MERGE INTO %s failed", target_table)
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.exception("tDBOutput_1: rollback failed")
        raise
    finally:
        if conn is not None:
            conn.close()

    logger.info("job_01_customer_enrichment: subjob_1 complete")


if __name__ == "__main__":
    run_subjob_1()
