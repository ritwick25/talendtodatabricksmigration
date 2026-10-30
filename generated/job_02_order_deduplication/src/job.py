import logging
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_02_order_deduplication")

# Talend context variables (dev defaults) -- rebased onto a Unity Catalog Volume
# path since this workspace has no reachable local/DBFS filesystem for these
# relative paths; adjust the volume/catalog/schema segments to match the real
# provisioned volume if different.
SOURCE_DIR = "/Volumes/talend_migration/samples/fixtures/talend_export_src/fixtures"
TARGET_SCHEMA = "samples"

REJECT_OUTPUT_PATH = "/Volumes/talend_migration/samples/fixtures_output/orders_duplicate_rejects.csv"

CATALOG = "talend_migration"
SCHEMA = "samples"
ORDER_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.order_summary"

# Cap on how many ranked rows are echoed to the driver log, standing in for the
# original tJavaFlex_1 System.out.println per row. Talend printed every row from a
# single JVM; in Spark, pulling the full result set to the driver is a driver-OOM
# hazard, so the side effect is preserved as a bounded sample instead.
RANKED_LOG_SAMPLE_LIMIT = 50


def tier_expr(total_amount_col):
    """Pure translation of tJavaFlex_1's tierFor() helper as a native Spark expression.

    Order of the branches matches the Java original exactly: >=1000 PLATINUM,
    >=500 GOLD, negative REVIEW, otherwise STANDARD (null -> STANDARD).
    """
    return (
        F.when(total_amount_col.isNull(), F.lit("STANDARD"))
        .when(total_amount_col >= F.lit(1000), F.lit("PLATINUM"))
        .when(total_amount_col >= F.lit(500), F.lit("GOLD"))
        .when(total_amount_col < F.lit(0), F.lit("REVIEW"))
        .otherwise(F.lit("STANDARD"))
    )


def main():
    spark = SparkSession.builder.getOrCreate()

    # --- tFileInputDelimited_1 : orders.csv ---
    orders_path = SOURCE_DIR + "/orders.csv"
    orders_df = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .csv(orders_path)
        .select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.col("order_date").cast("string").alias("order_date"),
            F.col("amount").cast("decimal(12,2)").alias("amount"),
            F.col("status").cast("string").alias("status"),
        )
    )

    # --- tFileInputDelimited_2 : customers.csv ---
    customers_path = SOURCE_DIR + "/customers.csv"
    customers_df = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .csv(customers_path)
        .select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.col("country_code").cast("string").alias("country_code"),
        )
    )

    # --- tJoin_1 : inner join on customer_id (unique match) ---
    joined_df = orders_df.join(customers_df, on="customer_id", how="inner").select(
        "order_id", "customer_id", "order_date", "amount", "status", "country_code"
    )

    # --- tUniqRow_1 : dedupe on order_id, with reject output for duplicates ---
    window_spec = Window.partitionBy("order_id").orderBy(F.lit(1))
    ranked_df = joined_df.withColumn("_dup_rank", F.row_number().over(window_spec))

    unique_df = ranked_df.filter(F.col("_dup_rank") == 1).drop("_dup_rank")
    reject_df = ranked_df.filter(F.col("_dup_rank") > 1).drop("_dup_rank")

    # --- tFileOutputDelimited_1 : write duplicate rejects (Unity Catalog Volume path) ---
    (
        reject_df.write.mode("overwrite")
        .option("header", "true")
        .option("delimiter", ",")
        .csv(REJECT_OUTPUT_PATH)
    )

    # --- tSortRow_1 : sort by order_date ASC, amount DESC ---
    sorted_df = unique_df.orderBy(F.col("order_date").asc(), F.col("amount").desc())

    # --- tAggregateRow_1 : group by customer_id, SUM(amount), COUNT(order_id) ---
    agg_df = sorted_df.groupBy("customer_id").agg(
        F.sum("amount").cast("decimal(12,2)").alias("total_amount"),
        F.count("order_id").cast("int").alias("order_count"),
    )

    # --- tJavaFlex_1 : ranking + tiering pass (translated from Java) ---
    # Original Java sorts descending by total_amount, ties broken ascending by
    # customer_id (as string), assigns 1-based rank, then a tier label. Both the
    # ranking and the tiering are expressed natively so nothing is pulled to the
    # driver for the computation itself.
    rank_window = Window.orderBy(
        F.col("total_amount").desc(), F.col("customer_id").cast("string").asc()
    )

    ranked_customers_df = (
        agg_df.withColumn("rank", F.row_number().over(rank_window).cast("int"))
        .withColumn("tier", tier_expr(F.col("total_amount")))
        .select(
            F.col("customer_id").cast("string").alias("customer_id"),
            "rank",
            "tier",
            "total_amount",
            "order_count",
        )
    )

    # --- tDBOutput_1 : INSERT into order_summary (Delta table) ---
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ORDER_SUMMARY_TABLE} (
            customer_id STRING,
            rank INT,
            tier STRING,
            total_amount DECIMAL(12,2),
            order_count INT
        ) USING DELTA
        """
    )

    ranked_customers_df.write.format("delta").mode("append").saveAsTable(ORDER_SUMMARY_TABLE)

    # SIDE_EFFECT replication, done AFTER the write and read back from the
    # persisted Delta table so no unbounded collect() is performed on the
    # in-flight DataFrame:
    #   1. globalMap.put("job02_tierCounts", tiers) -> small aggregate, at most
    #      one row per distinct tier label (4 possible values), so collecting it
    #      is bounded by construction.
    #   2. System.out.println per ranked row -> a bounded top-N sample via
    #      limit(), which Spark pushes down instead of materialising every row
    #      on the driver.
    summary_df = spark.table(ORDER_SUMMARY_TABLE)

    tier_counts = {
        row["tier"]: row["tier_count"]
        for row in summary_df.groupBy("tier")
        .agg(F.count(F.lit(1)).alias("tier_count"))
        .collect()
    }
    logger.info("job02_tierCounts=%s", tier_counts)

    total_ranked = summary_df.count()
    for row in (
        summary_df.orderBy(F.col("rank").asc()).limit(RANKED_LOG_SAMPLE_LIMIT).collect()
    ):
        logger.info(
            "ranked customer=%s rank=%s tier=%s",
            row["customer_id"],
            row["rank"],
            row["tier"],
        )
    if total_ranked > RANKED_LOG_SAMPLE_LIMIT:
        logger.info(
            "ranked row logging truncated: showed %s of %s rows (full result set is in %s)",
            RANKED_LOG_SAMPLE_LIMIT,
            total_ranked,
            ORDER_SUMMARY_TABLE,
        )

    logger.info("job_02_order_deduplication completed successfully")


if __name__ == "__main__":
    main()
