import logging
import sys
from datetime import datetime
import calendar

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)
import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_16_date_calendar_processing")

# ---------------------------------------------------------------------------
# Context (Talend context.* variables) -- module-level constants.
# source_dir is a real input-fixtures Volume path in this workspace, not the
# spec's literal 'dev' value (which was only valid in the original local
# Talend job generation environment).
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"
TARGET_SCHEMA = "dev_samples"  # context.target_schema (dev) -- not a path, safe to use literally
MAX_RETRIES = 3  # context.max_retries (dev)

INPUT_FILE = f"{SOURCE_DIR}/subscription_events.csv"

# Delta table target for tDBOutput_1 (component TABLE param, lowercased)
TARGET_TABLE = "talend_migration.samples.subscription_billing_schedule"


# ---------------------------------------------------------------------------
# Translated Java block (tJavaRow_1) -- used verbatim, wired via pandas_udf
# per the no-raw-RDD rule for serverless compute.
# ---------------------------------------------------------------------------
_PATTERNS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%y",
    "%m/%d/%Y",
]

_ZONE_MAP = {
    "EST": "America/New_York",
    "PST": "America/Los_Angeles",
    "CET": "Europe/Paris",
}


def _zone_for(code):
    name = _ZONE_MAP.get(code, "UTC")
    if ZoneInfo is not None:
        return ZoneInfo(name)
    return None


def _parse_flexible(s, tz):
    if s is None or str(s).strip() == "":
        return None
    s = str(s).strip()
    for p in _PATTERNS:
        try:
            dt = datetime.strptime(s, p)
            # SimpleDateFormat with setLenient(false) rejects impossible
            # dates (e.g. day 31 in a 30-day month). strptime already
            # rejects those (raises ValueError), matching that behavior.
            if tz is not None:
                dt = dt.replace(tzinfo=tz)
            return dt
        except ValueError:
            continue
    return None


def _is_leap_year(year):
    return (year % 4 == 0) and (year % 100 != 0 or year % 400 == 0)


def _iso(dt):
    return "%04d-%02d-%02d" % (dt.year, dt.month, dt.day)


def _add_month_clamped(dt):
    orig_day = dt.day
    year = dt.year
    month = dt.month + 1
    if month > 12:
        month = 1
        year += 1
    max_day_next_month = calendar.monthrange(year, month)[1]
    new_day = min(orig_day, max_day_next_month)
    return dt.replace(year=year, month=month, day=new_day)


def run(input: dict) -> dict:
    sub_id = input.get("sub_id")
    event_ts_raw = input.get("event_ts_raw")
    timezone_code = input.get("timezone_code")

    src_tz = _zone_for(timezone_code)
    parsed = _parse_flexible(event_ts_raw, src_tz)

    if parsed is None:
        return {
            "sub_id": sub_id,
            "valid": False,
            "local_date": None,
            "utc_date": None,
            "is_leap_year": None,
            "next_billing_date": None,
            "is_leap_anniversary": None,
        }

    local_dt = parsed

    if ZoneInfo is not None and src_tz is not None:
        utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
    else:
        utc_dt = local_dt

    orig_month_is_feb = local_dt.month == 2
    leap_anniversary = orig_month_is_feb and local_dt.day == 29

    next_dt = _add_month_clamped(local_dt)

    return {
        "sub_id": sub_id,
        "valid": True,
        "local_date": _iso(local_dt),
        "utc_date": _iso(utc_dt),
        "is_leap_year": _is_leap_year(local_dt.year),
        "next_billing_date": _iso(next_dt),
        "is_leap_anniversary": leap_anniversary,
    }


_OUTPUT_SCHEMA = StructType(
    [
        StructField("sub_id", StringType(), True),
        StructField("valid", BooleanType(), True),
        StructField("local_date", StringType(), True),
        StructField("utc_date", StringType(), True),
        StructField("is_leap_year", BooleanType(), True),
        StructField("next_billing_date", StringType(), True),
        StructField("is_leap_anniversary", BooleanType(), True),
    ]
)


@pandas_udf(
    "sub_id string, valid boolean, local_date string, utc_date string, "
    "is_leap_year boolean, next_billing_date string, is_leap_anniversary boolean"
)
def process_subscription_rows(
    sub_id: pd.Series,
    event_type: pd.Series,
    event_ts_raw: pd.Series,
    timezone_code: pd.Series,
) -> pd.DataFrame:
    records = []
    for s, et, ts, tzc in zip(sub_id, event_type, event_ts_raw, timezone_code):
        row = run(
            {
                "sub_id": s,
                "event_type": et,
                "event_ts_raw": ts,
                "timezone_code": tzc,
            }
        )
        records.append(row)
    return pd.DataFrame.from_records(
        records,
        columns=[
            "sub_id",
            "valid",
            "local_date",
            "utc_date",
            "is_leap_year",
            "next_billing_date",
            "is_leap_anniversary",
        ],
    )


def main():
    spark = SparkSession.builder.getOrCreate()

    logger.info("Reading input file: %s", INPUT_FILE)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(
            StructType(
                [
                    StructField("sub_id", StringType(), True),
                    StructField("event_type", StringType(), True),
                    StructField("event_ts_raw", StringType(), True),
                    StructField("timezone_code", StringType(), True),
                ]
            )
        )
        .load(INPUT_FILE)
    )

    # tJavaRow_1: per-row date/calendar processing, wired via pandas_udf
    # (never raw RDD APIs -- rejected on serverless compute).
    processed = df.select(
        process_subscription_rows(
            col("sub_id"),
            col("event_type"),
            col("event_ts_raw"),
            col("timezone_code"),
        ).alias("result")
    ).select("result.*")

    # tDBOutput_1: ACTION=INSERT -> append to Delta table.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            sub_id STRING,
            valid BOOLEAN,
            local_date STRING,
            utc_date STRING,
            is_leap_year BOOLEAN,
            next_billing_date STRING,
            is_leap_anniversary BOOLEAN
        ) USING DELTA
        """
    )

    logger.info("Writing results to table: %s", TARGET_TABLE)
    processed.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

    logger.info("Job completed successfully.")


if __name__ == "__main__":
    main()
