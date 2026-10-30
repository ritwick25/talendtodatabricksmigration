import re
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_09_joblet_demo")

# ---------------------------------------------------------------------------
# Context variables (Talend context.* -> plain module constants, dev values)
#
# Fix (cycle 1): the previous version pointed at plain relative filesystem
# paths ("samples/talend_export_src/fixtures/..."), which on a Databricks
# cluster resolve to the local driver filesystem / an ungoverned location,
# not a Unity Catalog governed path. Source and output now live under a UC
# Volume path instead.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/fixtures"
OUTPUT_DIR = "/Volumes/talend_migration/samples/fixtures_output"

INPUT_FILE = f"{SOURCE_DIR}/addresses_raw.csv"
OUTPUT_FILE = f"{OUTPUT_DIR}/addresses_standardized.csv"

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# tJavaRow_1 translated block (from AddressStandardizationJoblet), used
# verbatim as given by the JAVAROUTINE agent, invoked here as Python UDFs.
#
# NOTE: com.sample.util.StringUtils is an external/custom Java library.
# No source was provided for normalizeWhitespace()/titleCase(), so this is a
# best-effort reimplementation based on common conventions:
#   normalizeWhitespace: collapse runs of whitespace to a single space, strip ends
#   titleCase: capitalize the first letter of each whitespace-separated word,
#              lowercase the rest (Locale.ROOT-like, ASCII-safe)
# If the real library applies different rules (e.g. special-casing for
# 'McDonald', hyphenated words, or Locale-sensitive casing), this will diverge.
# unresolved_dependency: com.sample.util.StringUtils (normalizeWhitespace, titleCase)
# -- custom external Java library with no provided source or JAR; behavior approximated.

def _normalize_whitespace(s):
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip()


def _title_case(s):
    if s is None:
        return None
    if s == "":
        return ""
    words = s.split(" ")
    out_words = []
    for w in words:
        if w == "":
            out_words.append(w)
        else:
            out_words.append(w[0].upper() + w[1:].lower())
    return " ".join(out_words)


def _java_row_transform(address_id, street, city, state, zip_, country):
    """Direct port of tJavaRow_1.CODE semantics (joblet AddressStandardizationJoblet)."""
    street_clean = _title_case(_normalize_whitespace(street))

    city_in = "" if city is None else city
    city_clean = _title_case(_normalize_whitespace(city_in))

    state_clean = "" if state is None else state.strip().upper()
    country_clean = "" if country is None else country.strip().upper()

    return (address_id, street_clean, city_clean, state_clean, zip_, country_clean)


# ---------------------------------------------------------------------------
# Main subjob: tFileInputDelimited_1 -> tAddressStandardizationJoblet_1 -> tFileOutputDelimited_1
# ---------------------------------------------------------------------------

def run_address_standardization_joblet(df):
    """Inlines joblet AddressStandardizationJoblet: tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1."""

    def transform_partition(rows):
        for row in rows:
            yield _java_row_transform(
                row["address_id"],
                row["street"],
                row["city"],
                row["state"],
                row["zip"],
                row["country"],
            )

    result_rdd = df.rdd.mapPartitions(transform_partition)
    result_df = result_rdd.toDF([
        "address_id",
        "street_clean",
        "city_clean",
        "state_clean",
        "zip",
        "country_clean",
    ])
    return result_df


def main():
    logger.info("Reading tFileInputDelimited_1 from %s", INPUT_FILE)

    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .load(INPUT_FILE)
        .select("address_id", "street", "city", "state", "zip", "country")
    )

    logger.info("Running AddressStandardizationJoblet (tAddressStandardizationJoblet_1)")
    standardized_df = run_address_standardization_joblet(raw_df)

    logger.info("Writing tFileOutputDelimited_1 to %s", OUTPUT_FILE)
    (
        standardized_df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .save(OUTPUT_FILE)
    )

    logger.info("job_09_joblet_demo completed successfully")


if __name__ == "__main__":
    main()
