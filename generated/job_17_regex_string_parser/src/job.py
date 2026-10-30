import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd

# -----------------------------------------------------------------------------
# Context-derived constants
# -----------------------------------------------------------------------------
# Per migration runtime convention, Talend file-component source directories
# resolve in this workspace to the fixtures Volume (not the original local
# Talend dev-machine path recorded in the spec's context group), since that
# is where the actual input files for this migration are materialized.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
INPUT_FILE = f"{SOURCE_DIR}/support_tickets.csv"

# tDBOutput_1 target: dev-workspace Delta table standing in for the external DB.
TARGET_SCHEMA = "talend_migration.samples"
TARGET_TABLE = "tickets_parsed"
FULL_TABLE_NAME = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_17_regex_string_parser")


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


# -----------------------------------------------------------------------------
# tJavaRow_1 translated block (verbatim, as supplied by JAVAROUTINE agent)
# -----------------------------------------------------------------------------
import re

# Java: ^\[(?<priority>[A-Z]+)\](?=\s*\S)
# Python re supports named groups (?P<name>...) and lookahead (?=...) identically in syntax
# meaning here; translate group names and keep the lookahead as-is.
PRIORITY_PATTERN = re.compile(r'^\[(?P<priority>[A-Z]+)\](?=\s*\S)')
ORDER_PATTERN = re.compile(r'Order#(?P<orderid>\d+)')


def run(input: dict) -> dict:
    subject = input.get('raw_subject')
    if subject is None:
        subject = ""

    pm = PRIORITY_PATTERN.search(subject)
    priority = pm.group('priority') if pm else "NORMAL"

    om = ORDER_PATTERN.search(subject)
    order_id = om.group('orderid') if om else ""

    # Java replaceFirst("^\\[[A-Z]+\\]\\s*", "") -- only first match, anchored at start
    cleaned = re.sub(r'^\[[A-Z]+\]\s*', '', subject, count=1)
    # Java replaceAll("\\s+", " ") -- all whitespace runs collapsed to single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    # Java String.trim() strips ASCII <= U+0020 (close enough to Python strip() for this use)
    cleaned = cleaned.strip()

    # Java substring(0,3) then toUpperCase(Locale.ROOT) -- Locale.ROOT avoids locale-specific
    # casing (e.g. Turkish dotless i); Python str.upper() is Unicode-aware but not locale-
    # sensitive by default, which matches Locale.ROOT behavior closely enough for A-Z content.
    ticket_code = (cleaned[:3] if len(cleaned) >= 3 else cleaned).upper()

    ts = input.get('tag_string')
    if ts is None:
        ts = ""

    # Java String.split(",") drops trailing empty strings; split(",", -1) preserves them.
    # Python str.split(',') never drops trailing empties, so we must emulate Java's default
    # trailing-empty-removal behavior explicitly for `trimmed`.
    preserved = ts.split(',')

    def java_split_drop_trailing_empty(s, parts):
        # emulate Java's default split(regex) trailing-empty-string removal
        result = list(parts)
        while result and result[-1] == '':
            result.pop()
        return result

    trimmed = java_split_drop_trailing_empty(ts, preserved)

    kept = 0
    pieces = []
    for t in preserved:
        norm = t.strip()
        if norm != "":
            pieces.append(norm)
            kept += 1
    tags_normalized = "; ".join(pieces)

    return {
        'ticket_id': input.get('ticket_id'),
        'priority': priority,
        'order_id': order_id,
        'subject_clean': cleaned,
        'ticket_code': ticket_code,
        'tags_trimmed_split_len': len(trimmed),
        'tags_preserved_split_len': len(preserved),
        'tags_normalized': tags_normalized,
    }


# -----------------------------------------------------------------------------
# Output schema for the pandas UDF (mapInPandas) stage
# -----------------------------------------------------------------------------
OUTPUT_SCHEMA = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("subject_clean", StringType(), True),
    StructField("ticket_code", StringType(), True),
    StructField("tags_trimmed_split_len", IntegerType(), True),
    StructField("tags_preserved_split_len", IntegerType(), True),
    StructField("tags_normalized", StringType(), True),
])


def apply_tjavarow(batch_iter):
    """mapInPandas-compatible generator wrapping the per-row tJavaRow_1 logic.

    Uses mapInPandas (not df.rdd.map/mapPartitions) because raw RDD APIs are
    rejected outright on serverless compute.
    """
    for pdf in batch_iter:
        records = pdf.to_dict(orient="records")
        out_records = [run(rec) for rec in records]
        out_pdf = pd.DataFrame(out_records, columns=[f.name for f in OUTPUT_SCHEMA.fields])
        out_pdf["tags_trimmed_split_len"] = out_pdf["tags_trimmed_split_len"].astype("int32")
        out_pdf["tags_preserved_split_len"] = out_pdf["tags_preserved_split_len"].astype("int32")
        yield out_pdf


def main():
    spark = get_spark()

    # --- tFileInputDelimited_1 ---
    logger.info("Reading input file: %s", INPUT_FILE)
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(INPUT_FILE)
        .select("ticket_id", "raw_subject", "tag_string")
    )

    # --- tJavaRow_1 ---
    parsed_df = raw_df.mapInPandas(apply_tjavarow, schema=OUTPUT_SCHEMA)

    # --- tDBOutput_1: INSERT action -> append to Delta table ---
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            ticket_id STRING,
            priority STRING,
            order_id STRING,
            subject_clean STRING,
            ticket_code STRING,
            tags_trimmed_split_len INT,
            tags_preserved_split_len INT,
            tags_normalized STRING
        ) USING DELTA
    """)

    logger.info("Writing parsed tickets to %s (append/INSERT semantics)", FULL_TABLE_NAME)
    parsed_df.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME)

    logger.info("Job job_17_regex_string_parser completed successfully.")


if __name__ == "__main__":
    main()
