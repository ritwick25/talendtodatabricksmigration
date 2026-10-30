import logging
import datetime
import posixpath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_04_transaction_filter_convert")

# ---------------------------------------------------------------------------
# Context parameters (Talend context -> job widgets / dbutils widgets)
# ---------------------------------------------------------------------------
dbutils.widgets.text("source_dir", "/Volumes/talend_migration/default/landing")
dbutils.widgets.text("reject_dir", "/Volumes/talend_migration/default/fixtures_output")
dbutils.widgets.text("db_url", "")
dbutils.widgets.text("target_schema", "default")

source_dir = dbutils.widgets.get("source_dir")
reject_dir = dbutils.widgets.get("reject_dir")
db_url = dbutils.widgets.get("db_url")
target_schema = dbutils.widgets.get("target_schema")

# JDBC credentials come from secrets, never literals.
jdbc_user = dbutils.secrets.get(scope="talend-migration", key="db_user")
jdbc_password = dbutils.secrets.get(scope="talend-migration", key="db_password")


def _require_volume_path(path, label):
    """Normalize and assert a path stays inside a Unity Catalog Volume.

    The original Talend job built the reject path as
    context.source_dir + "/../fixtures_output/...", i.e. a sibling directory
    of source_dir reached via a relative '..' segment. Carrying that '..'
    through verbatim lets the write escape the configured Volume boundary
    (and possibly leave Volumes entirely), so instead we resolve the
    sibling directory to an absolute, '..'-free path and refuse anything
    that does not land under /Volumes/.
    """
    normalized = posixpath.normpath(path)
    if not normalized.startswith("/Volumes/"):
        raise ValueError(
            f"{label} must resolve to a Unity Catalog Volume path under /Volumes/, got: {normalized!r}"
        )
    if ".." in normalized.split("/"):
        raise ValueError(f"{label} must not contain '..' segments, got: {normalized!r}")
    # Require at least /Volumes/<catalog>/<schema>/<volume> so we cannot
    # resolve up to the /Volumes root or to a bare catalog/schema.
    if len(normalized.strip("/").split("/")) < 4:
        raise ValueError(
            f"{label} must be at least /Volumes/<catalog>/<schema>/<volume>, got: {normalized!r}"
        )
    return normalized


SOURCE_DIR = _require_volume_path(source_dir, "source_dir")
# Equivalent of the original "<source_dir>/../fixtures_output" sibling
# directory, but supplied/resolved as an explicit absolute Volume path
# rather than by traversing out of source_dir at write time.
REJECT_DIR = _require_volume_path(reject_dir, "reject_dir")

INPUT_FILE = posixpath.join(SOURCE_DIR, "transactions_raw.csv")
REJECT_FILE = posixpath.join(REJECT_DIR, "transactions_invalid_status.csv")

logger.info("Reading input from %s", INPUT_FILE)
logger.info("Reject output resolved to %s", REJECT_FILE)

# ---------------------------------------------------------------------------
# tJavaRow_1 translation (verbatim, as supplied by JAVAROUTINE agent)
# ---------------------------------------------------------------------------
PATTERNS = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%Y%m%d"]


def _parse_flexible(s):
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    for p in PATTERNS:
        try:
            dt = datetime.datetime.strptime(s, p)
            return dt
        except ValueError:
            continue
    return None


def _is_leap_year(year):
    return (year % 4 == 0) and (year % 100 != 0 or year % 400 == 0)


def _last_day_of_month(year, month):
    if month == 12:
        next_month_first = datetime.date(year + 1, 1, 1)
    else:
        next_month_first = datetime.date(year, month + 1, 1)
    return (next_month_first - datetime.timedelta(days=1)).day


def _iso(d):
    return "%04d-%02d-%02d" % (d.year, d.month, d.day)


def run(input: dict) -> dict:
    txn_date_raw = input.get("txn_date_raw")
    customer_id = input.get("customer_id")
    amount_str = input.get("amount_str")

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

    cal_date = datetime.date(parsed.year, parsed.month, parsed.day)

    year = cal_date.year
    leap = _is_leap_year(year)
    day = cal_date.day
    last_day = _last_day_of_month(cal_date.year, cal_date.month)
    month_end = (day == last_day)

    cutoff_date = cal_date
    if month_end:
        cutoff_date = cutoff_date + datetime.timedelta(days=1)
        while cutoff_date.weekday() in (5, 6):
            cutoff_date = cutoff_date + datetime.timedelta(days=1)

    try:
        amt = float(amount_str)
        amount_ok = amt >= 0.0
    except (TypeError, ValueError):
        amount_ok = False

    return {
        "txn_id": customer_id,
        "valid": True,
        "parsed_date": _iso(cal_date),
        "is_leap_year": leap,
        "is_month_end": month_end,
        "statement_cutoff_date": _iso(cutoff_date),
        "amount_valid": amount_ok,
    }


_JAVAROW_1_RESULT_SCHEMA = T.StructType([
    T.StructField("txn_id", T.StringType(), True),
    T.StructField("valid", T.BooleanType(), True),
    T.StructField("parsed_date", T.StringType(), True),
    T.StructField("is_leap_year", T.BooleanType(), True),
    T.StructField("is_month_end", T.BooleanType(), True),
    T.StructField("statement_cutoff_date", T.StringType(), True),
    T.StructField("amount_valid", T.BooleanType(), True),
])


def _java_row_1_udf(txn_date_raw, customer_id, amount_str):
    result = run({
        "txn_date_raw": txn_date_raw,
        "customer_id": customer_id,
        "amount_str": amount_str,
    })
    return (
        result.get("txn_id"),
        result.get("valid"),
        result.get("parsed_date"),
        result.get("is_leap_year"),
        result.get("is_month_end"),
        result.get("statement_cutoff_date"),
        result.get("amount_valid"),
    )


java_row_1_udf = F.udf(_java_row_1_udf, _JAVAROW_1_RESULT_SCHEMA)


# ---------------------------------------------------------------------------
# subjob_1: tFileInputDelimited_1 -> tExtractRegexFields_1 -> tFilterRow_1
#           -> (main) tConvertType_1 -> tJavaRow_1 -> tDBOutput_1
#           -> (reject) tFileOutputDelimited_1
# ---------------------------------------------------------------------------

# tFileInputDelimited_1: declared schema is a single 'raw_line' String
# column; HEADER=1 means skip the first physical line. Read as plain text
# (one column per physical line) and drop exactly the first line, which
# matches Talend's "skip N header lines" semantics without inventing a
# field delimiter.
_raw_text_df = spark.read.text(INPUT_FILE)
_raw_text_df = _raw_text_df.withColumn(
    "_rn", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)
raw_df = (
    _raw_text_df.filter(F.col("_rn") > 1)
    .select(F.col("value").alias("raw_line"))
)

# tExtractRegexFields_1: named-group regex applied to raw_line. Group order
# in the regex (date, cust, amt, status) maps positionally onto the declared
# output schema order (txn_date_raw, customer_id, amount_str, status_raw).
_REGEX = r"TXN\|(?P<date>[^|]+)\|(?P<cust>[^|]+)\|AMT:(?P<amt>[^|]+)\|STATUS:(?P<status>.+)"

extracted_df = raw_df.select(
    F.regexp_extract(F.col("raw_line"), _REGEX, 1).alias("txn_date_raw"),
    F.regexp_extract(F.col("raw_line"), _REGEX, 2).alias("customer_id"),
    F.regexp_extract(F.col("raw_line"), _REGEX, 3).alias("amount_str"),
    F.regexp_extract(F.col("raw_line"), _REGEX, 4).alias("status_raw"),
)

# tFilterRow_1: FILTER_EXPRESSION = status_raw.equals("OK") -> main/reject split.
main_df = extracted_df.filter(F.col("status_raw") == "OK")
reject_df = extracted_df.filter((F.col("status_raw") != "OK") | F.col("status_raw").isNull())

# tFileOutputDelimited_1 (REJECT branch target) -- writes to the validated,
# absolute Volume path resolved above.
(
    reject_df.write.format("csv")
    .option("header", "true")
    .mode("overwrite")
    .csv(REJECT_FILE)
)
logger.info("Wrote reject rows (status != OK) to %s", REJECT_FILE)

# tConvertType_1: amount_str:String -> amount:BigDecimal(12,2)
converted_df = main_df.withColumn(
    "amount", F.col("amount_str").cast(T.DecimalType(12, 2))
)

# tJavaRow_1
javarow_struct = java_row_1_udf(
    F.col("txn_date_raw"), F.col("customer_id"), F.col("amount_str")
)

enriched_df = (
    converted_df.withColumn("_jr", javarow_struct)
    .select(
        F.col("txn_date_raw"),
        F.col("customer_id"),
        F.col("amount_str"),
        F.col("amount"),
        F.col("_jr.txn_id").alias("txn_id"),
        F.col("_jr.valid").alias("valid"),
        F.col("_jr.parsed_date").alias("parsed_date"),
        F.col("_jr.is_leap_year").alias("is_leap_year"),
        F.col("_jr.is_month_end").alias("is_month_end"),
        F.col("_jr.statement_cutoff_date").alias("statement_cutoff_date"),
        F.col("_jr.amount_valid").alias("amount_valid"),
    )
)

# tDBOutput_1: ACTION = INSERT. tJavaRow_1 has no row-level reject routing
# (invalid rows are flagged via output fields per the spec), so there is no
# per-row DB reject path. A hard DB failure is logged with context and
# re-raised so the job aborts loudly, matching the original behavior.
jdbc_url = db_url
attempted_count = None

try:
    attempted_count = enriched_df.count()
    (
        enriched_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{target_schema}.transactions_clean")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("queryTimeout", "300")
        .mode("append")
        .save()
    )
    logger.info(
        "Inserted %d valid transaction rows into %s.transactions_clean",
        attempted_count,
        target_schema,
    )
except Py4JJavaError as e:
    logger.error(
        "JDBC insert into %s.transactions_clean failed for %s attempted rows: %s",
        target_schema,
        attempted_count,
        str(e),
    )
    raise
except Exception as e:
    logger.error(
        "Unexpected error writing to %s.transactions_clean: %s",
        target_schema,
        str(e),
    )
    raise
