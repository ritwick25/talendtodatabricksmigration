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

spark = SparkSession.builder.appName("job_01_customer_enrichment").getOrCreate()

# -----------------------------------------------------------------------------
# Context variables (Migration_Context) -- resolved per-environment at run time
# via job widgets/params. Defaults below mirror the 'dev' context values.
# -----------------------------------------------------------------------------
dbutils_available = False
try:
    dbutils  # type: ignore # noqa: F821
    dbutils_available = True
except NameError:
    pass


def _get_param(name, default):
    if dbutils_available:
        try:
            return dbutils.widgets.get(name)  # type: ignore # noqa: F821
        except Exception:
            return default
    return default


source_dir = _get_param("source_dir", "samples/talend_export_src/fixtures")
db_url = _get_param("db_url", "jdbc:postgresql://dev-db.internal:5432/migration")
db_user = _get_param("db_user", "migration_svc_dev")
target_schema = _get_param("target_schema", "dev_samples")

# Secrets: never embed literal credentials.
db_password = dbutils.secrets.get(scope="talend-migration", key="db_password") if dbutils_available else None

# Unity Catalog Volume base path for rejects output (adjust catalog/schema/volume as provisioned).
REJECTS_VOLUME_PATH = "/Volumes/talend_migration/job_01_customer_enrichment/fixtures_output/customers_rejected.csv"

JDBC_PROPERTIES = {
    "user": db_user,
    "password": db_password if db_password is not None else "",
    "driver": "org.postgresql.Driver",
}


# -----------------------------------------------------------------------------
# tFileInputDelimited_1 / _2 / _3
# -----------------------------------------------------------------------------
def read_customers():
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("loyalty_points", IntegerType(), True),
    ])
    path = f"{source_dir}/customers.csv"
    return (
        spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(path)
    )


def read_regions():
    schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("region_name", StringType(), True),
    ])
    path = f"{source_dir}/regions.csv"
    return (
        spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .schema(schema)
        .csv(path)
    )


def read_loyalty_tiers():
    schema = StructType([
        StructField("tier_code", StringType(), True),
        StructField("min_points", IntegerType(), True),
        StructField("max_points", IntegerType(), True),
        StructField("tier_name", StringType(), True),
    ])
    path = f"{source_dir}/loyalty_tiers.csv"
    return (
        spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .schema(schema)
        .csv(path)
    )


# -----------------------------------------------------------------------------
# tMap_1
#
# row2 (regions) join key: country_code, inner join, load-once semantics are the
# Spark default since the lookup DataFrame is read once and reused -- no action
# needed to replicate LOAD_ONCE.
#
# row3 (loyalty_tiers) join key: UNCERTAIN per spec. MAP_EXPRESSIONS never
# references a join predicate between row1/row3; the spec's uncertainties note
# this is likely a range lookup (loyalty_points BETWEEN min_points AND
# max_points) rather than an equality join on tier_code, which has no matching
# column on row1. We implement the range-lookup interpretation since it is the
# only one that produces a deterministic single match per row (RELOAD_AT_EACH_ROW
# -> left outer, first match), and left_outer is preserved per the spec's
# join_model so non-matching rows still flow through with tier_name = null.
# -----------------------------------------------------------------------------
def build_tmap(customers_df, regions_df, loyalty_tiers_df):
    joined = customers_df.join(regions_df, on="country_code", how="inner")

    joined = joined.join(
        loyalty_tiers_df,
        on=(
            (joined["loyalty_points"] >= loyalty_tiers_df["min_points"])
            & (joined["loyalty_points"] <= loyalty_tiers_df["max_points"])
        ),
        how="left_outer",
    )

    mapped = joined.select(
        joined["customer_id"].alias("customer_id"),
        joined["first_name"].alias("first_name"),
        joined["last_name"].alias("last_name"),
        joined["email"].alias("email"),
        joined["region_name"].alias("region_name"),
        loyalty_tiers_df["tier_name"].alias("tier_name"),
        joined["loyalty_points"].alias("loyalty_points"),
    )

    mapped = mapped.withColumn(
        "is_valid",
        (F.col("customer_id").isNotNull())
        & (F.col("email").isNotNull())
        & (F.col("email").contains("@")),
    )

    out_main = mapped.filter(F.col("is_valid")).drop("is_valid")
    out_reject = mapped.filter(~F.col("is_valid")).drop("is_valid")

    return out_main, out_reject


# -----------------------------------------------------------------------------
# tJavaRow_1 (translated Java block, verbatim helper logic, adapted to a Spark
# mapPartitions UDF so the STATEFUL dedupe set / counter are scoped per
# partition -- true global-state dedupe across the whole dataset would require
# a prior distinct/dropDuplicates stage, called out explicitly below).
# -----------------------------------------------------------------------------
def _safe(s):
    return "" if s is None else str(s)


def _normalize_whitespace(s):
    return re.sub(r"\s+", " ", s).strip()


def _title_case(s):
    # com.sample.util.StringUtils.titleCase: EXTERNAL_LIB, exact rules unknown.
    # Best-effort equivalent using Python's str.title(); flagged uncertain.
    return s.title()


def _mask_email(email):
    # com.sample.util.StringUtils.maskEmail: EXTERNAL_LIB, exact masking rule unknown.
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
    crc = zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF
    return format(crc, "08x")


def _compute_loyalty_value(points, tier_name):
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


def _tjavarow_1_partition(rows):
    # NOTE: dedupe/counter state is scoped to this partition's iterator, matching
    # the original static-field semantics only when the job runs single-threaded /
    # single-partition. Documented limitation: Spark's parallelism means a true
    # global dedupe requires an upstream dropDuplicates("customer_id") stage.
    processed_customer_ids = set()
    rows_processed_counter = 0

    for row in rows:
        raw_customer_id = row["customer_id"]
        customer_id = None if raw_customer_id is None else str(raw_customer_id)

        if customer_id is None or customer_id in processed_customer_ids:
            logger.warning(
                "tJavaRow_1: skipping duplicate/null customer_id=%s", customer_id
            )
            continue

        processed_customer_ids.add(customer_id)
        rows_processed_counter += 1

        first_name = _safe(row["first_name"])
        last_name = _safe(row["last_name"])
        full_name = _title_case(_normalize_whitespace(first_name + " " + last_name))

        loyalty_value = _compute_loyalty_value(row["loyalty_points"], row["tier_name"])

        row_checksum = _crc32_hex(
            f"{customer_id}|{_safe(row['email'])}|{row['loyalty_points']}"
        )

        yield (
            row["customer_id"],
            full_name,
            _mask_email(row["email"]),
            loyalty_value,
            row_checksum,
            row["region_name"],
            row["tier_name"],
        )

    # job01_rowsProcessed globalMap equivalent: publish via taskValues rather than
    # a shared mutable globalMap, since globalMap has no direct Spark analogue.
    if dbutils_available:
        try:
            dbutils.jobs.taskValues.set(  # type: ignore # noqa: F821
                key="job01_rowsProcessed", value=rows_processed_counter
            )
        except Exception:
            logger.warning("Could not publish job01_rowsProcessed task value")


TJAVAROW_1_OUTPUT_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("email_masked", StringType(), True),
    StructField("loyalty_value", DecimalType(12, 2), True),
    StructField("row_checksum", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("tier_name", StringType(), True),
])


def apply_tjavarow_1(out_main_df):
    rdd = out_main_df.rdd.mapPartitions(_tjavarow_1_partition)
    return spark.createDataFrame(rdd, schema=TJAVAROW_1_OUTPUT_SCHEMA)


# -----------------------------------------------------------------------------
# tDBOutput_1 -- UPDATE_OR_INSERT (upsert) via MERGE INTO. Assumes customer_id
# is the natural key per the upstream schema's key=true flag (unconfirmed on
# the tDBOutput node itself, per spec uncertainties).
# -----------------------------------------------------------------------------
def write_customers_enriched(enriched_df):
    full_table = f"{target_schema}.customers_enriched"
    staging_table = f"{target_schema}.customers_enriched_staging_tmp"

    (
        enriched_df.write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", staging_table)
        .option("user", db_user)
        .option("password", db_password if db_password is not None else "")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING {staging_table} AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
            target.full_name = source.full_name,
            target.email_masked = source.email_masked,
            target.loyalty_value = source.loyalty_value,
            target.row_checksum = source.row_checksum,
            target.region_name = source.region_name,
            target.tier_name = source.tier_name
        WHEN NOT MATCHED THEN INSERT (
            customer_id, full_name, email_masked, loyalty_value, row_checksum, region_name, tier_name
        ) VALUES (
            source.customer_id, source.full_name, source.email_masked, source.loyalty_value,
            source.row_checksum, source.region_name, source.tier_name
        )
    """

    import psycopg2  # driver-side merge executed via a direct connection

    conn = psycopg2.connect(
        dsn=db_url.replace("jdbc:", ""), user=db_user, password=db_password,
        connect_timeout=30,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(merge_sql)
        conn.commit()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# tFileOutputDelimited_1 -- rejects
# -----------------------------------------------------------------------------
def write_rejects(out_reject_df):
    (
        out_reject_df.write
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .csv(REJECTS_VOLUME_PATH)
    )


# -----------------------------------------------------------------------------
# subjob_1 orchestration
# -----------------------------------------------------------------------------
def main():
    customers_df = read_customers()
    regions_df = read_regions()
    loyalty_tiers_df = read_loyalty_tiers()

    out_main_df, out_reject_df = build_tmap(customers_df, regions_df, loyalty_tiers_df)

    enriched_df = apply_tjavarow_1(out_main_df)

    write_customers_enriched(enriched_df)
    write_rejects(out_reject_df)

    logger.info("job_01_customer_enrichment completed successfully")


if __name__ == "__main__":
    main()
