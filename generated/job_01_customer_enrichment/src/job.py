"""
Databricks Jobs spark_python_task module.
Migrated from Talend job: job_01_customer_enrichment

Pipeline:
  tFileInputDelimited_1 (customers.csv)     --FLOW--> tMap_1
  tFileInputDelimited_2 (regions.csv)       --LOOKUP (inner, load-once)--> tMap_1
  tFileInputDelimited_3 (loyalty_tiers.csv) --LOOKUP (left_outer, reload-each-row)--> tMap_1
  tMap_1 --FLOW--> tJavaRow_1 --FLOW--> tDBOutput_1 (Delta MERGE upsert)
  tMap_1 --REJECT--> tFileOutputDelimited_1 (customers_rejected.csv)

NOTE on row3 lookup join key: no explicit join key parameter was present for
row3 in the source job. Only tier_name/points are referenced in expressions.
Implemented here as a range join (loyalty_points BETWEEN min_points AND
max_points) -- an inference, flagged for human/merge-pass review.

Fix pass 2 -- single targeted change:
The reviewer's one surviving high-severity finding is that tJavaRow_1's
dedupe used monotonically_increasing_id() to stand in for "first row seen",
which is not a stable row order after an upstream join/filter and so picks a
different surviving row (and a different reject set) run to run. Root cause:
the ordering key was synthesized AFTER the join instead of being captured
from the actual source read.

Fix: capture a genuine, stable input ordinal at the CSV read itself with
input_file_name() + a per-file row index, carry it through tMap_1 as
_src_ordinal, and order the dedupe window by (_src_ordinal, customer_id) so
"first occurrence" is derived from the real file position and is identical on
every re-run. Nothing else about the pipeline is changed.
"""

import re
import zlib
import logging
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_01_customer_enrichment")

spark = SparkSession.builder.getOrCreate()

dbutils = None
try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except Exception:
    pass


# Talend context.* variables -> plain module-level constants using the
# Migration_Context 'dev' values from the spec. Not read via
# spark.conf.get / dbutils.widgets.get (neither is available in a plain
# serverless spark_python_task).
SOURCE_DIR = "samples/talend_export_src/fixtures"
TARGET_SCHEMA = "dev_samples"

# Writes only to Delta tables in talend_migration.samples (no live JDBC
# connection to the external Postgres instance named in the spec -- not
# reachable from this workspace).
CATALOG_SCHEMA = "talend_migration.samples"
TABLE_CUSTOMERS_ENRICHED = f"{CATALOG_SCHEMA}.customers_enriched"

# Reject output targets a fixed Unity Catalog Volume path, never a path built
# by concatenating SOURCE_DIR with '../' segments.
REJECT_OUTPUT_PATH = "/Volumes/talend_migration/samples/fixtures_output/customers_rejected"


# ---------------------------------------------------------------------------
# tFileInputDelimited_1 / _2 / _3
# ---------------------------------------------------------------------------
def read_customers(spark):
    schema = T.StructType([
        T.StructField("customer_id", T.IntegerType(), True),
        T.StructField("first_name", T.StringType(), True),
        T.StructField("last_name", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("signup_date", T.StringType(), True),
        T.StructField("country_code", T.StringType(), True),
        T.StructField("loyalty_points", T.IntegerType(), True),
    ])
    path = SOURCE_DIR.rstrip("/") + "/customers.csv"
    df = (
        spark.read.format("csv")
        .option("header", "true").option("sep", ",").option("encoding", "UTF-8")
        .schema(schema).load(path)
    )
    # Stable input ordinal captured at the read, before any join/filter can
    # reshuffle rows. row_number() over (file, position-within-file) is
    # reproducible across runs for the same input files, unlike
    # monotonically_increasing_id() applied later in the plan.
    file_order = Window.partitionBy("_src_file").orderBy("_src_pos")
    return (
        df.withColumn("_src_file", F.input_file_name())
          .withColumn("_src_pos", F.monotonically_increasing_id())
          .withColumn("_src_ordinal", F.row_number().over(file_order))
          .drop("_src_file", "_src_pos")
    )


def read_regions(spark):
    schema = T.StructType([
        T.StructField("country_code", T.StringType(), True),
        T.StructField("region_name", T.StringType(), True),
    ])
    path = SOURCE_DIR.rstrip("/") + "/regions.csv"
    return (
        spark.read.format("csv")
        .option("header", "true").option("sep", ",")
        .schema(schema).load(path)
    )


def read_loyalty_tiers(spark):
    schema = T.StructType([
        T.StructField("tier_code", T.StringType(), True),
        T.StructField("min_points", T.IntegerType(), True),
        T.StructField("max_points", T.IntegerType(), True),
        T.StructField("tier_name", T.StringType(), True),
    ])
    path = SOURCE_DIR.rstrip("/") + "/loyalty_tiers.csv"
    return (
        spark.read.format("csv")
        .option("header", "true").option("sep", ",")
        .schema(schema).load(path)
    )


# ---------------------------------------------------------------------------
# tMap_1 : lookups + expression columns + main/reject filter split
# ---------------------------------------------------------------------------
OUT_COLUMNS = [
    "customer_id", "first_name", "last_name", "email",
    "region_name", "tier_name", "loyalty_points",
]


def apply_tmap(df_customers, df_regions, df_tiers):
    df_r2 = df_regions.select(
        F.col("country_code").alias("_r2_country_code"),
        F.col("region_name").alias("region_name"),
    )
    joined = df_customers.join(
        df_r2, df_customers["country_code"] == df_r2["_r2_country_code"], "inner"
    ).drop("_r2_country_code")

    # row3 lookup join key inferred as range match (see module docstring).
    df_r3 = df_tiers.select(
        F.col("min_points").alias("_r3_min_points"),
        F.col("max_points").alias("_r3_max_points"),
        F.col("tier_name").alias("tier_name"),
    )
    joined = joined.join(
        df_r3,
        (joined["loyalty_points"] >= df_r3["_r3_min_points"])
        & (joined["loyalty_points"] <= df_r3["_r3_max_points"]),
        "left_outer",
    ).drop("_r3_min_points", "_r3_max_points")

    mapped = joined.withColumn(
        "is_valid",
        F.col("customer_id").isNotNull()
        & F.col("email").isNotNull()
        & F.col("email").contains("@"),
    )

    # _src_ordinal is carried into out_main so tJavaRow_1's dedupe can order
    # by true source position; out_reject keeps only the declared schema.
    out_main = mapped.filter(F.col("is_valid")).select(*(OUT_COLUMNS + ["_src_ordinal"]))
    out_reject = mapped.filter(~F.col("is_valid")).select(*OUT_COLUMNS)
    return out_main, out_reject


# ---------------------------------------------------------------------------
# tJavaRow_1 (translated Java block) -- distributed version.
#
# The dedupe-by-customer_id (static HashSet) and counter (AtomicLong) were
# process-wide static state in the original JVM. On Spark that cannot be
# shared mutable state, so dedupe is computed with a window function keyed on
# customer_id and ordered by _src_ordinal -- the real per-file input position
# captured at read time -- so "keep the first occurrence" matches the Talend
# HashSet-first-seen semantics deterministically on every run. The pure
# per-row logic (valuation / masking / checksum) runs distributed via
# mapInPandas. Duplicate rows are routed to the reject output rather than
# silently dropped.
# ---------------------------------------------------------------------------
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
    masked_local = (local + "***") if len(local) <= 1 else (local[0] + "***")
    return masked_local + "@" + domain


def _crc32_hex(s):
    return format(zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF, "x")


def _compute_loyalty_value(points, tier_name):
    if points is None:
        return Decimal("0").quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    base = Decimal(int(points)) * Decimal("0.01")
    if tier_name is not None and str(tier_name).lower() == "platinum":
        multiplier = Decimal("1.50")
    elif tier_name is not None and str(tier_name).lower() == "gold":
        multiplier = Decimal("1.25")
    elif tier_name is None:
        multiplier = Decimal("0")
    else:
        multiplier = Decimal("1")
    return (base * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _enrich_partition(pdf_iter):
    """mapInPandas worker: pure per-row logic from tJavaRow_1 (stateless,
    no shared mutable state across partitions)."""
    for pdf in pdf_iter:
        full_names, masked_emails, loyalty_values, checksums = [], [], [], []
        for _, row in pdf.iterrows():
            cid = row["customer_id"]
            first_name = row["first_name"]
            last_name = row["last_name"]
            email = row["email"]
            points = row["loyalty_points"]
            tier_name = row["tier_name"]

            safe_first = "" if first_name is None else str(first_name)
            safe_last = "" if last_name is None else str(last_name)
            full_names.append(_title_case(_normalize_whitespace(safe_first + " " + safe_last)))
            masked_emails.append(_mask_email(email))
            loyalty_values.append(_compute_loyalty_value(points, tier_name))
            safe_points = "" if points is None else str(points)
            safe_email = "" if email is None else str(email)
            checksums.append(_crc32_hex(str(cid) + "|" + safe_email + "|" + safe_points))

        out = pdf[["customer_id", "region_name", "tier_name"]].copy()
        out["full_name"] = full_names
        out["email_masked"] = masked_emails
        out["loyalty_value"] = [str(v) for v in loyalty_values]
        out["row_checksum"] = checksums
        yield out


OUT_SCHEMA = T.StructType([
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("region_name", T.StringType(), True),
    T.StructField("tier_name", T.StringType(), True),
    T.StructField("full_name", T.StringType(), True),
    T.StructField("email_masked", T.StringType(), True),
    T.StructField("loyalty_value", T.StringType(), True),
    T.StructField("row_checksum", T.StringType(), True),
])


def apply_tjavarow(df_main):
    """Distributed equivalent of tJavaRow_1. Returns (df_enriched, df_dupes).

    Dedupe ordering uses _src_ordinal (true per-file input position captured
    at the CSV read), with customer_id as a tie-breaker, so the surviving row
    and the duplicate set are identical across re-runs.
    """
    w = Window.partitionBy("customer_id").orderBy(
        F.col("_src_ordinal").asc(), F.col("customer_id").asc()
    )
    ranked = df_main.withColumn("_dup_rank", F.row_number().over(w))

    deduped = ranked.filter(F.col("_dup_rank") == 1).drop("_dup_rank", "_src_ordinal")
    dupes = ranked.filter(F.col("_dup_rank") > 1).drop("_dup_rank", "_src_ordinal")

    rows_processed = deduped.count()
    logger.info("tJavaRow_1: job01_rowsProcessed=%s", rows_processed)
    if dbutils is not None:
        try:
            dbutils.jobs.taskValues.set(key="job01_rowsProcessed", value=rows_processed)
        except Exception:
            logger.warning("Could not publish job01_rowsProcessed via taskValues", exc_info=True)

    enriched = deduped.select(
        "customer_id", "region_name", "tier_name", "first_name", "last_name",
        "email", "loyalty_points",
    ).mapInPandas(_enrich_partition, schema=OUT_SCHEMA)

    enriched = enriched.withColumn(
        "loyalty_value", F.col("loyalty_value").cast(T.DecimalType(12, 2))
    )

    return enriched, dupes


# ---------------------------------------------------------------------------
# tDBOutput_1 : UPDATE_OR_INSERT -> Delta MERGE on customer_id (the key column)
# ---------------------------------------------------------------------------
def write_customers_enriched(df_enriched):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CUSTOMERS_ENRICHED} (
            customer_id INT,
            full_name STRING,
            email_masked STRING,
            loyalty_value DECIMAL(12,2),
            row_checksum STRING,
            region_name STRING,
            tier_name STRING
        ) USING DELTA
        """
    )

    temp_view = "tmp_customers_enriched_src"
    df_out = df_enriched.select(
        "customer_id", "full_name", "email_masked", "loyalty_value",
        "row_checksum", "region_name", "tier_name",
    )
    df_out.createOrReplaceTempView(temp_view)

    spark.sql(
        f"""
        MERGE INTO {TABLE_CUSTOMERS_ENRICHED} AS target
        USING {temp_view} AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# ---------------------------------------------------------------------------
# tFileOutputDelimited_1 : rejects (tMap_1 reject filter + tJavaRow_1 dupes)
# ---------------------------------------------------------------------------
def write_rejects(df_reject, df_dupes):
    dupes_aligned = df_dupes.select(*OUT_COLUMNS)
    combined = df_reject.select(*OUT_COLUMNS).unionByName(dupes_aligned)
    (
        combined.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .mode("overwrite")  # APPEND=false in the spec
        .save(REJECT_OUTPUT_PATH)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("Starting job_01_customer_enrichment")

    df_customers = read_customers(spark)
    df_regions = read_regions(spark)
    df_tiers = read_loyalty_tiers(spark)

    df_main, df_reject = apply_tmap(df_customers, df_regions, df_tiers)
    df_enriched, df_dupes = apply_tjavarow(df_main)

    write_customers_enriched(df_enriched)
    write_rejects(df_reject, df_dupes)

    logger.info("Completed job_01_customer_enrichment")


if __name__ == "__main__":
    main()
