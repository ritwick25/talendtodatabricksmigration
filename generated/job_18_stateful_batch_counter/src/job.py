import logging
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_18_stateful_batch_counter")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Context variables (dev values). SOURCE_DIR is overridden to the real
# fixtures volume path in this workspace -- the spec's "dev" context value
# for source_dir is a local-generation artifact, not a real path here.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
TARGET_SCHEMA = "dev_samples"  # context.target_schema dev value, used only as metadata/logging here
MAX_RETRIES = 3  # context.max_retries dev value, unused in this subjob but kept for parity

INPUT_FILE = f"{SOURCE_DIR}/clickstream_events.csv"
REJECTS_OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/fixtures_output/clickstream_dedupe_rejects"

DELTA_TABLE = "talend_migration.samples.clickstream_clean"


def main():
    # -----------------------------------------------------------------
    # tFileInputDelimited_1: read the clickstream CSV with header.
    #
    # Read as a single-partition DataFrame so row order on disk (the
    # order the original single-threaded Java tJavaFlex loop processed
    # rows in) is preserved deterministically for the stateful logic
    # below. monotonically_increasing_id() alone is NOT safe for this --
    # it only guarantees increasing values within a partition, not a
    # stable global order across partitions -- so we force a single
    # partition before assigning the row-order key, then use
    # F.row_number() over an explicit order-by of that key as the only
    # source of row ordering used downstream.
    # -----------------------------------------------------------------
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(INPUT_FILE)
        .select(
            F.col("session_id").cast("string"),
            F.col("event_id").cast("string"),
            F.col("event_type").cast("string"),
            F.col("event_ts").cast("string"),
            F.col("user_id").cast("string"),
        )
    )

    # Force a single partition BEFORE generating the row-order id, so the
    # id assignment itself happens over one partition and is therefore a
    # true, stable, total order matching the original file's row order --
    # monotonically_increasing_id() on a single partition increases
    # monotonically with input order, which is the property we actually
    # need (it is unsafe only across multiple partitions).
    single_partition_df = raw_df.coalesce(1)
    ordered_df = single_partition_df.withColumn(
        "_row_seq", F.monotonically_increasing_id()
    )

    row_order_window = Window.orderBy("_row_seq")
    session_window = Window.partitionBy("session_id").orderBy("_row_seq")
    event_id_window = Window.partitionBy("event_id").orderBy("_row_seq")

    enriched_df = (
        ordered_df
        # running_total_events: 1-based running count of all rows in input order
        .withColumn("running_total_events", F.row_number().over(row_order_window).cast("long"))
        # session_event_seq: 1-based running count of rows per session, in input order
        .withColumn("session_event_seq", F.row_number().over(session_window).cast("int"))
        # is_duplicate: true for every row after the first occurrence of an event_id
        .withColumn("_event_id_seq", F.row_number().over(event_id_window))
        .withColumn("is_duplicate", F.col("_event_id_seq") > 1)
        .select(
            "session_id",
            "event_id",
            "event_type",
            "is_duplicate",
            "session_event_seq",
            "running_total_events",
        )
    )
    enriched_df.cache()

    total_events = enriched_df.count()
    distinct_event_ids = enriched_df.select("event_id").distinct().count()
    duplicate_count = total_events - distinct_event_ids

    busiest_row = (
        enriched_df.groupBy("session_id")
        .agg(F.max("session_event_seq").alias("max_session_events"))
        .orderBy(F.col("max_session_events").desc())
        .limit(1)
        .collect()
    )
    if busiest_row:
        busiest_session = busiest_row[0]["session_id"]
        max_session_events = busiest_row[0]["max_session_events"]
    else:
        busiest_session = None
        max_session_events = 0
    num_sessions = enriched_df.select("session_id").distinct().count()

    # Equivalent of the END_CODE summarize(...) println and globalMap.put calls
    summary_line = (
        f"summarize: total={total_events}, distinct={distinct_event_ids}, "
        f"sessions={num_sessions}, busiest={busiest_session}, "
        f"busiestCount={max_session_events}, dupes={duplicate_count}"
    )
    logger.info(summary_line)

    global_map = {
        "job18_totalEvents": total_events,
        "job18_duplicateCount": duplicate_count,
        "job18_busiestSession": busiest_session,
    }
    logger.info("globalMap: %s", global_map)

    # -----------------------------------------------------------------
    # tFilterRow_1: FILTER_EXPRESSION = !is_duplicate
    # Main flow -> rows where is_duplicate is false
    # Reject flow -> rows where is_duplicate is true
    # -----------------------------------------------------------------
    main_df = enriched_df.filter(~F.col("is_duplicate"))
    reject_df = enriched_df.filter(F.col("is_duplicate"))

    # -----------------------------------------------------------------
    # tDBOutput_1: TABLE=clickstream_clean, ACTION=INSERT -> append to
    # Delta table in talend_migration.samples (no live external DB).
    # -----------------------------------------------------------------
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DELTA_TABLE} (
            session_id STRING,
            event_id STRING,
            event_type STRING,
            is_duplicate BOOLEAN,
            session_event_seq INT,
            running_total_events LONG
        ) USING DELTA
        """
    )
    main_df.write.format("delta").mode("append").saveAsTable(DELTA_TABLE)
    logger.info("Wrote %d rows to %s", main_df.count(), DELTA_TABLE)

    # -----------------------------------------------------------------
    # tFileOutputDelimited_1: reject flow written as delimited CSV
    # -----------------------------------------------------------------
    (
        reject_df.write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(REJECTS_OUTPUT_DIR)
    )
    logger.info("Wrote %d reject rows to %s", reject_df.count(), REJECTS_OUTPUT_DIR)

    enriched_df.unpersist()


if __name__ == "__main__":
    main()
