import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DecimalType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_04_transaction_filter_convert")

# ---------------------------------------------------------------------------
# Context variables.
# source_dir's spec 'dev' value is a placeholder from the original local
# synthetic-job generation and is not a real path in this workspace -- the
# actual input fixtures for this migration live flat under the fixed Volume
# path below, using the original Talend FILENAME basenames.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
TARGET_SCHEMA = "talend_migration.samples"

# The Talend reject FILENAME was context.source_dir + "/../fixtures_output/...",
# i.e. a sibling 'fixtures_output' directory of the fixtures dir. Rather than
# emitting a literal '..' segment (relative traversal that can escape the
# sanctioned Unity Catalog Volume boundary), resolve that sibling directory
# explicitly and normalize it to an absolute, traversal-free Volume path, then
# assert it is still inside the Volume root before writing.
VOLUME_ROOT = "/Volumes/talend_migration/samples/talend_export"
REJECT_OUTPUT_DIR = f"{VOLUME_ROOT}/samples/fixtures_output"
REJECT_OUTPUT_FILE = f"{REJECT_OUTPUT_DIR}/transactions_invalid_status.csv"

INPUT_FILE = f"{SOURCE_DIR}/transactions_raw.csv"
OUTPUT_TABLE = f"{TARGET_SCHEMA}.transactions_clean"

REGEX_PATTERN = r"TXN\|([^|]+)\|([^|]+)\|AMT:([^|]+)\|STATUS:(.+)"


def _assert_inside_volume(path: str, root: str) -> str:
    """Guard: the resolved write path must be an absolute Volume path inside
    `root` and must contain no '..' traversal segments."""
    import posixpath

    normalized = posixpath.normpath(path)
    if ".." in normalized.split("/"):
        raise ValueError(f"Refusing to write to path with traversal segment: {path}")
    root_normalized = posixpath.normpath(root)
    if not normalized.startswith(root_normalized + "/"):
        raise ValueError(
            f"Refusing to write outside Volume root {root_normalized}: {normalized}"
        )
    return normalized


# ---------------------------------------------------------------------------
# Translated tJavaRow_1 block (verbatim, from JAVAROUTINE agent).
# ---------------------------------------------------------------------------
PATTERNS = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%Y%m%d"]


def _parse_flexible(s):
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    for p in PATTERNS:
        try:
            dt = datetime.datetime.strptime(s, p)
        except ValueError:
            continue
        # Emulate SimpleDateFormat lenient=false: strict component ranges.
        # Reject patterns that silently accepted e.g. 2-digit overflow via
        # strptime normalizing (strptime itself raises on most invalid
        # dates, but guard impossible components explicitly for parity).
        if p in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
            if not (1 <= dt.month <= 12 and 1 <= dt.day <= 31):
                continue
        elif p == "%m/%d/%Y":
            if not (1 <= dt.month <= 12 and 1 <= dt.day <= 31):
                continue
        elif p == "%d-%m-%Y":
            if not (1 <= dt.day <= 31 and 1 <= dt.month <= 12):
                continue
        return dt
    return None


def _is_leap_year(year):
    return (year % 4 == 0) and (year % 100 != 0 or year % 400 == 0)


def _last_day_of_month(year, month):
    if month == 12:
        nxt = datetime.date(year + 1, 1, 1)
    else:
        nxt = datetime.date(year, month + 1, 1)
    return (nxt - datetime.timedelta(days=1)).day


def _iso(d):
    return "%04d-%02d-%02d" % (d.year, d.month, d.day)


def run(input: dict) -> dict:
    txn_date_raw = input.get("txn_date_raw")
    amount_str = input.get("amount_str")
    customer_id = input.get("customer_id")

    parsed = _parse_flexible(txn_date_raw)
    if parsed is None:
        return {
            "txn_id": customer_id,
            "valid": False,
            "parsed_date": None,
            "is_leap_year": None,
            "is_month_end": None,
            "statement_cutoff_date": None,
            "amount_valid": None,
        }

    d = datetime.date(parsed.year, parsed.month, parsed.day)

    year = d.year
    leap = _is_leap_year(year)
    day = d.day
    last_day = _last_day_of_month(year, d.month)
    month_end = (day == last_day)

    cutoff = d
    if month_end:
        cutoff = cutoff + datetime.timedelta(days=1)
        # Java Calendar.DAY_OF_WEEK: SUNDAY=1 ... SATURDAY=7.
        # Python date.weekday(): MONDAY=0 ... SUNDAY=6.
        while cutoff.weekday() in (5, 6):  # Saturday=5, Sunday=6
            cutoff = cutoff + datetime.timedelta(days=1)

    try:
        amt = float(amount_str)
        amount_ok = amt >= 0.0
    except (TypeError, ValueError):
        amount_ok = False

    return {
        "txn_id": customer_id,
        "valid": True,
        "parsed_date": _iso(d),
        "is_leap_year": leap,
        "is_month_end": month_end,
        "statement_cutoff_date": _iso(cutoff),
        "amount_valid": amount_ok,
    }


# The tJavaRow_1 block (per spec schema) only emits txn_id/valid/parsed_date/
# is_leap_year/is_month_end/statement_cutoff_date/amount_valid -- it does not
# pass through amount or status_raw itself. To avoid silently dropping the
# tConvertType_1 'amount' column (and the already-filtered 'status_raw') from
# the final sink, we carry them through alongside the mapInPandas call rather
# than inside it, then re-attach them after, so every upstream transform's
# output is actually persisted.
TJAVAROW_OUTPUT_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), True),
        StructField("valid", BooleanType(), True),
        StructField("parsed_date", StringType(), True),
        StructField("is_leap_year", BooleanType(), True),
        StructField("is_month_end", BooleanType(), True),
        StructField("statement_cutoff_date", StringType(), True),
        StructField("amount_valid", BooleanType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("status_raw", StringType(), True),
    ]
)


def _apply_java_row(pdf_iter):
    import pandas as pd

    for pdf in pdf_iter:
        records = pdf.to_dict(orient="records")
        out_records = []
        for r in records:
            result = run(r)
            # Pass through tConvertType_1's amount and the filtered status_raw
            # unchanged -- tJavaRow_1 never touched or dropped these fields in
            # the original Talend flow, they simply rode along on the row.
            result["amount"] = r.get("amount")
            result["status_raw"] = r.get("status_raw")
            out_records.append(result)
        out_pdf = pd.DataFrame(out_records, columns=[f.name for f in TJAVAROW_OUTPUT_SCHEMA.fields])
        yield out_pdf


def main():
    spark = SparkSession.builder.getOrCreate()

    reject_path = _assert_inside_volume(REJECT_OUTPUT_FILE, VOLUME_ROOT)

    # --- tFileInputDelimited_1 ---
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(StructType([StructField("raw_line", StringType(), True)]))
        .load(INPUT_FILE)
    )

    # --- tExtractRegexFields_1 ---
    extracted_df = raw_df.select(
        F.regexp_extract(F.col("raw_line"), REGEX_PATTERN, 1).alias("txn_date_raw"),
        F.regexp_extract(F.col("raw_line"), REGEX_PATTERN, 2).alias("customer_id"),
        F.regexp_extract(F.col("raw_line"), REGEX_PATTERN, 3).alias("amount_str"),
        F.regexp_extract(F.col("raw_line"), REGEX_PATTERN, 4).alias("status_raw"),
    )

    # --- tFilterRow_1 (main: status_raw == "OK"; reject: everything else) ---
    main_df = extracted_df.filter(F.col("status_raw") == "OK")
    reject_df = extracted_df.filter((F.col("status_raw") != "OK") | F.col("status_raw").isNull())

    # --- tFileOutputDelimited_1 (REJECT flow) ---
    (
        reject_df.write.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .save(reject_path)
    )

    # --- tConvertType_1 (amount_str:String -> amount:BigDecimal(12,2)) ---
    converted_df = main_df.withColumn(
        "amount", F.col("amount_str").cast("decimal(12,2)")
    )

    # --- tJavaRow_1 ---
    # Feed it all columns tJavaRow_1's logic needs (txn_date_raw, customer_id,
    # amount_str) PLUS amount and status_raw so they can ride through to the
    # output untouched, instead of being computed and then discarded.
    java_row_input_df = converted_df.select(
        "txn_date_raw", "customer_id", "amount_str", "amount", "status_raw"
    )
    java_row_df = java_row_input_df.mapInPandas(_apply_java_row, schema=TJAVAROW_OUTPUT_SCHEMA)

    # --- tDBOutput_1 (INSERT -> Delta table append) ---
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
            txn_id STRING,
            valid BOOLEAN,
            parsed_date STRING,
            is_leap_year BOOLEAN,
            is_month_end BOOLEAN,
            statement_cutoff_date STRING,
            amount_valid BOOLEAN,
            amount DECIMAL(12,2),
            status_raw STRING
        ) USING DELTA
        """
    )

    java_row_df.write.format("delta").mode("append").saveAsTable(OUTPUT_TABLE)

    logger.info("job_04_transaction_filter_convert completed successfully.")


if __name__ == "__main__":
    main()
