import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_18_stateful_batch_counter")

# context.source_dir -- fixture inputs for this workspace live at a fixed Volume path,
# NOT the literal 'dev' context value from the original Talend export.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
INPUT_FILE = f"{SOURCE_DIR}/clickstream_events.csv"

# Reject output: original FILENAME was context.source_dir + "/../fixtures_output/...".
# Keep the same relative intent, anchored to a Volume path we know exists.
REJECT_OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output"
REJECT_OUTPUT_FILE = f"{REJECT_OUTPUT_DIR}/clickstream_dedupe_rejects.csv"

# context.target_schema (dev) -- not a path, safe to use literally per spec.
TARGET_SCHEMA = "dev_samples"
TARGET_TABLE = "clickstream_clean"
FULL_TABLE_NAME = f"talend_migration.samples.{TARGET_TABLE}"


def main():
    # --- tFileInputDelimited_1 ---
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .load(INPUT_FILE)
        .select("session_id", "event_id", "event_type", "event_ts", "user_id")
    )

    # --- tJavaFlex_1 translated as native Spark stateful logic ---
    # Original Java maintained:
    #   totalEvents        -> running count of rows processed so far (row ordinal)
    #   seenEventIds        -> dedupe set keyed on event_id (first occurrence wins)
    #   sessionEventCounts  -> running per-session sequence number
    # We reproduce this deterministically using window functions over a stable
    # row order (Talend processed rows in file order; we order by the row's
    # natural input position via monotonically_increasing_id as a stand-in
    # ordering key since no explicit sequence column exists in the source).
    ordered_df = raw_df.withColumn("_row_seq", F.monotonically_increasing_id())

    order_window = Window.orderBy("_row_seq")
    session_window = Window.partitionBy("session_id").orderBy("_row_seq")
    event_id_window = Window.partitionBy("event_id").orderBy("_row_seq")

    enriched_df = (
        ordered_df
        .withColumn("running_total_events", F.row_number().over(order_window).cast("long"))
        .withColumn("session_event_seq", F.row_number().over(session_window).cast("int"))
        .withColumn("_event_id_occurrence", F.row_number().over(event_id_window))
        .withColumn("is_duplicate", F.col("_event_id_occurrence") > 1)
        .select(
            "session_id",
            "event_id",
            "event_type",
            "is_duplicate",
            "session_event_seq",
            "running_total_events",
        )
    )

    enriched_df.createOrReplaceTempView("enriched_events")

    # --- END_CODE summary stats (globalMap equivalents) ---
    total_events = enriched_df.agg(F.max("running_total_events")).collect()[0][0] or 0
    distinct_event_ids = raw_df.select("event_id").distinct().count()
    duplicate_count = total_events - distinct_event_ids

    busiest_row = (
        enriched_df.groupBy("session_id")
        .agg(F.max("session_event_seq").alias("max_seq"))
        .orderBy(F.col("max_seq").desc(), F.col("session_id").asc())
        .first()
    )
    busiest_session = busiest_row["session_id"] if busiest_row else None
    busiest_count = busiest_row["max_seq"] if busiest_row else 0

    summary_line = (
        f"job18 summary: total={total_events} distinct={distinct_event_ids} "
        f"sessions={enriched_df.select('session_id').distinct().count()} "
        f"busiest={busiest_session} busiestCount={busiest_count} dupes={duplicate_count}"
    )
    logger.info(summary_line)

    global_map = {
        "job18_totalEvents": total_events,
        "job18_duplicateCount": duplicate_count,
        "job18_busiestSession": busiest_session,
    }
    logger.info("globalMap equivalent values: %s", global_map)

    # --- tFilterRow_1: FILTER_EXPRESSION = !is_duplicate ---
    main_df = enriched_df.filter(~F.col("is_duplicate"))
    reject_df = enriched_df.filter(F.col("is_duplicate"))

    # --- tDBOutput_1: INSERT into Delta table (no live JDBC connection) ---
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            session_id STRING,
            event_id STRING,
            event_type STRING,
            is_duplicate BOOLEAN,
            session_event_seq INT,
            running_total_events LONG
        ) USING DELTA
    """)

    (
        main_df.write.format("delta")
        .mode("append")
        .saveAsTable(FULL_TABLE_NAME)
    )

    # --- tFileOutputDelimited_1: REJECT branch (duplicates) ---
    (
        reject_df.write.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .save(REJECT_OUTPUT_FILE)
    )

    logger.info(
        "Job complete: %d rows written to %s, %d duplicate rows written to %s",
        main_df.count(),
        FULL_TABLE_NAME,
        reject_df.count(),
        REJECT_OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
