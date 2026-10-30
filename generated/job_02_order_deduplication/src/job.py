import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType
)

# ---------------------------------------------------------------------------
# Context-derived constants
# ---------------------------------------------------------------------------
# NOTE: context.source_dir's 'dev' value from the original Talend job is a
# local synthetic path and not valid in this workspace. Input fixtures for
# sample jobs live flat in the UC Volume below, so SOURCE_DIR points there.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"

# Output location for the reject flow (orders that failed the uniqueness
# check on order_id). Kept under a UC Volume path analogous to the original
# relative '../fixtures_output' location.
REJECT_OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output"
REJECT_OUTPUT_PATH = f"{REJECT_OUTPUT_DIR}/orders_duplicate_rejects.csv"

# tDBOutput_1 target: this dev workspace has no reachable PostgreSQL
# instance, so we materialize as a managed Delta table instead of a live
# JDBC connection, per migration policy.
TARGET_CATALOG_SCHEMA = "talend_migration.samples"
TARGET_TABLE = "order_summary"  # from tDBOutput_1 TABLE parameter, lowercased

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_02_order_deduplication")


def main():
    spark = SparkSession.builder.getOrCreate()

    # -----------------------------------------------------------------
    # tFileInputDelimited_1: orders.csv
    # -----------------------------------------------------------------
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("status", StringType(), True),
    ])

    orders_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema(orders_schema)
        .load(f"{SOURCE_DIR}/orders.csv")
    )

    # -----------------------------------------------------------------
    # tFileInputDelimited_2: customers.csv (lookup side)
    # -----------------------------------------------------------------
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("country_code", StringType(), True),
    ])

    customers_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema(customers_schema)
        .load(f"{SOURCE_DIR}/customers.csv")
    )

    # -----------------------------------------------------------------
    # tJoin_1: Inner Join on customer_id, unique match
    # -----------------------------------------------------------------
    joined_df = orders_df.join(
        customers_df,
        on="customer_id",
        how="inner",
    ).select(
        "order_id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        "country_code",
    )

    # -----------------------------------------------------------------
    # tUniqRow_1: dedupe on order_id, with reject flow for duplicates
    # -----------------------------------------------------------------
    # Keep the first row (by insertion/arbitrary tiebreak using monotonic id)
    # per order_id as unique; all other rows with the same order_id are
    # rejected (duplicates).
    w = Window.partitionBy("order_id").orderBy(F.monotonically_increasing_id())
    ranked_df = joined_df.withColumn("_rn", F.row_number().over(w))

    unique_df = ranked_df.filter(F.col("_rn") == 1).drop("_rn")
    reject_df = ranked_df.filter(F.col("_rn") > 1).drop("_rn")

    # Write reject flow (duplicates) to CSV, matching tFileOutputDelimited_1
    (
        reject_df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(REJECT_OUTPUT_PATH)
    )
    logger.info("Wrote duplicate order rejects to %s", REJECT_OUTPUT_PATH)

    # -----------------------------------------------------------------
    # tSortRow_1: sort by order_date ASC, amount DESC
    # -----------------------------------------------------------------
    sorted_df = unique_df.orderBy(
        F.col("order_date").asc(),
        F.col("amount").desc(),
    )

    # -----------------------------------------------------------------
    # tAggregateRow_1: group by customer_id, SUM(amount), COUNT(order_id)
    # -----------------------------------------------------------------
    agg_df = (
        sorted_df.groupBy("customer_id")
        .agg(
            F.sum("amount").cast(DecimalType(12, 2)).alias("total_amount"),
            F.count("order_id").alias("order_count"),
        )
    )

    # -----------------------------------------------------------------
    # tJavaFlex_1: rank customers by total_amount desc (ties by customer_id
    # asc), assign tier label, and compute tier counts (put into a
    # 'globalMap' equivalent - logged here since there is no real
    # globalMap in a plain batch job; also logs each ranked row like the
    # original System.out.println side effect).
    #
    # This block's exact runtime row-emission mechanism was flagged in the
    # spec as non-compilable/indeterminate in the original Talend source,
    # so this is a faithful re-implementation of the described logic
    # (rankCustomers / tierFor / tierCounts) using native PySpark, not a
    # verbatim Java translation (no translated Java block was supplied for
    # tJavaFlex_1 in this pass).
    # -----------------------------------------------------------------
    rank_window = Window.orderBy(
        F.col("total_amount").desc(),
        F.col("customer_id").asc(),
    )

    ranked_customers_df = agg_df.withColumn("rank", F.row_number().over(rank_window))

    def tier_for(total_amount_col):
        return (
            F.when(total_amount_col < 0, F.lit("REVIEW"))
            .when(total_amount_col >= 1000, F.lit("PLATINUM"))
            .when(total_amount_col >= 500, F.lit("GOLD"))
            .otherwise(F.lit("STANDARD"))
        )

    ranked_customers_df = ranked_customers_df.withColumn(
        "tier", tier_for(F.col("total_amount"))
    )

    # customer_id becomes id_String per tJavaFlex_1 output schema
    final_df = ranked_customers_df.select(
        F.col("customer_id").cast(StringType()).alias("customer_id"),
        F.col("rank").cast(IntegerType()).alias("rank"),
        F.col("tier"),
        F.col("total_amount").cast(DecimalType(12, 2)).alias("total_amount"),
        F.col("order_count").cast(IntegerType()).alias("order_count"),
    )

    # Log each ranked customer row (mirrors System.out.println side effect)
    for row in final_df.collect():
        logger.info(
            "Ranked customer: customer_id=%s rank=%s tier=%s total_amount=%s order_count=%s",
            row["customer_id"], row["rank"], row["tier"], row["total_amount"], row["order_count"],
        )

    # tier_counts: LinkedHashMap of tier -> count, keyed as 'job02_tierCounts'
    # in the original globalMap. No cross-component globalMap exists in a
    # plain batch job, so we compute and log it here as the closest
    # equivalent side effect.
    tier_counts_rows = (
        final_df.groupBy("tier").count().collect()
    )
    tier_counts = {row["tier"]: row["count"] for row in tier_counts_rows}
    logger.info("job02_tierCounts = %s", tier_counts)

    # -----------------------------------------------------------------
    # tDBOutput_1: INSERT into order_summary (Delta table, since no live
    # PostgreSQL instance is reachable from this workspace)
    # -----------------------------------------------------------------
    full_table_name = f"{TARGET_CATALOG_SCHEMA}.{TARGET_TABLE}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            customer_id STRING,
            rank INT,
            tier STRING,
            total_amount DECIMAL(12,2),
            order_count INT
        ) USING DELTA
    """)

    (
        final_df.write.format("delta")
        .mode("append")
        .saveAsTable(full_table_name)
    )
    logger.info("Wrote %d rows to %s (ACTION=INSERT)", final_df.count(), full_table_name)


if __name__ == "__main__":
    main()
