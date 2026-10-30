import logging
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_09_joblet_demo")

spark = SparkSession.builder.getOrCreate()

# context.source_dir -- original Talend 'dev' context value is not a real path in this
# workspace; the job's input fixtures actually live flat in this UC Volume.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"

INPUT_FILE = f"{SOURCE_DIR}/addresses_raw.csv"
# tFileOutputDelimited_1 FILENAME was context.source_dir + "/../fixtures_output/..." ->
# sibling 'fixtures_output' dir relative to the fixtures volume.
OUTPUT_FILE = "/Volumes/talend_migration/samples/talend_export/samples/fixtures_output/addresses_standardized.csv"


# ---------------------------------------------------------------------------
# Joblet: AddressStandardizationJoblet
# tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1
#
# tJavaRow_1 translation (block_id=tJavaRow_1) is UNRESOLVED/uncertain: it depends on
# com.sample.util.StringUtils.normalizeWhitespace/titleCase, an external lib whose exact
# semantics are unknown and no JAR/library coordinates were supplied to load the real
# implementation via spark._jvm/py4j. The translated block below is used VERBATIM as
# provided by the JAVAROUTINE agent (best-effort stand-ins, clearly commented), wired in
# via a pandas UDF (no raw RDD APIs) since this runs on serverless compute.
# ---------------------------------------------------------------------------

def _normalize_whitespace_BEST_GUESS(s: str) -> str:
    # BEST-EFFORT STAND-IN ONLY -- collapse runs of whitespace to a single space, strip ends.
    # This may NOT match com.sample.util.StringUtils.normalizeWhitespace's real behavior
    # (e.g. it might also strip non-breaking spaces, tabs-as-separators, etc.)
    return re.sub(r'\s+', ' ', s).strip()


def _title_case_BEST_GUESS(s: str) -> str:
    # BEST-EFFORT STAND-IN ONLY -- naive Python .title(), which mishandles apostrophes
    # (e.g. "o'brien" -> "O'Brien" vs Python's "O'Brien"... but diverges on words like
    # "123rd" or hyphenated names) compared to an unknown custom titleCase implementation.
    return s.title()


def _java_row_run(input_row: dict) -> dict:
    street = input_row.get('street') or ''
    city = input_row.get('city') if input_row.get('city') is not None else ''
    state = input_row.get('state')
    country = input_row.get('country')

    output = {}
    output['address_id'] = input_row.get('address_id')
    output['street_clean'] = _title_case_BEST_GUESS(_normalize_whitespace_BEST_GUESS(street))
    output['city_clean'] = _title_case_BEST_GUESS(_normalize_whitespace_BEST_GUESS(city))
    output['state_clean'] = '' if state is None else state.strip().upper()
    output['country_clean'] = '' if country is None else country.strip().upper()
    output['zip'] = input_row.get('zip')
    return output


def run_address_standardization_joblet(df):
    """AddressStandardizationJoblet subjob: tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1."""

    out_schema = "address_id string, street_clean string, city_clean string, " \
                 "state_clean string, zip string, country_clean string"

    @pandas_udf(out_schema)
    def java_row_udf(address_id: pd.Series, street: pd.Series, city: pd.Series,
                      state: pd.Series, zip_: pd.Series, country: pd.Series) -> pd.DataFrame:
        records = []
        for i in range(len(address_id)):
            input_row = {
                'address_id': address_id.iloc[i],
                'street': street.iloc[i],
                'city': city.iloc[i],
                'state': state.iloc[i],
                'zip': zip_.iloc[i],
                'country': country.iloc[i],
            }
            records.append(_java_row_run(input_row))
        return pd.DataFrame.from_records(
            records,
            columns=['address_id', 'street_clean', 'city_clean', 'state_clean', 'zip', 'country_clean']
        )

    result = df.select(
        java_row_udf(
            col('address_id'), col('street'), col('city'), col('state'), col('zip'), col('country')
        ).alias('_std')
    ).select('_std.*')

    return result


def run_main_job():
    """job_09_joblet_demo_main: tFileInputDelimited_1 -> tAddressStandardizationJoblet_1 -> tFileOutputDelimited_1."""

    logger.info("Reading input delimited file: %s", INPUT_FILE)
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(INPUT_FILE)
        .select("address_id", "street", "city", "state", "zip", "country")
    )

    logger.info("Calling AddressStandardizationJoblet")
    standardized_df = run_address_standardization_joblet(raw_df)

    logger.info("Writing standardized output to: %s", OUTPUT_FILE)
    (
        standardized_df.write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(OUTPUT_FILE)
    )

    logger.info("job_09_joblet_demo completed successfully")


if __name__ == "__main__":
    run_main_job()
