import re
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_09_joblet_demo")

spark = SparkSession.builder.appName("job_09_joblet_demo").getOrCreate()

# ---------------------------------------------------------------------------
# Context parameters
# ---------------------------------------------------------------------------
# NOTE: context.source_dir in the original Talend job pointed at a local/
# on-prem filesystem path. In this Databricks workspace, file I/O must target
# a Unity Catalog Volume. Adjust this base path to the actual provisioned
# volume for this migration if different.
source_dir = spark.conf.get(
    "spark.job09.source_dir",
    "/Volumes/talend_migration/samples/job_09_joblet_demo_source",
)
output_dir = spark.conf.get(
    "spark.job09.output_dir",
    "/Volumes/talend_migration/samples/job_09_joblet_demo_output",
)

input_path = f"{source_dir}/addresses_raw.csv"
output_path = f"{output_dir}/addresses_standardized.csv"


# ---------------------------------------------------------------------------
# tJavaRow_1 translation (UNRESOLVED translation tier -- see rationale).
#
# com.sample.util.StringUtils.normalizeWhitespace / titleCase are an external
# library whose exact semantics (locale rules, apostrophe/hyphen handling,
# tab/newline stripping) are not available in this migration's inputs. The
# JAVAROUTINE agent flagged this block as translation_uncertain. We carry the
# best-effort pure-Python reimplementation forward as given, verbatim, and
# flag it clearly in logs/comments so a human can validate it against the
# real StringUtils behavior before this is trusted for production data.
# ---------------------------------------------------------------------------
def _normalize_whitespace(s):
    if s is None:
        return None
    # TODO: must match com.sample.util.StringUtils.normalizeWhitespace exactly
    # (unknown: does it strip leading/trailing, collapse tabs/newlines, etc.?)
    return re.sub(r'\s+', ' ', s).strip()


def _title_case(s):
    if s is None:
        return None
    # TODO: must match com.sample.util.StringUtils.titleCase exactly
    # (unknown: locale rules, handling of apostrophes/hyphens, min word length, etc.)
    return s.title()


def _java_row_1(input_row: dict) -> dict:
    street = input_row.get('street')
    city = input_row.get('city') if input_row.get('city') is not None else ''
    state = input_row.get('state')
    country = input_row.get('country')

    street_clean = _title_case(_normalize_whitespace(street))
    city_clean = _title_case(_normalize_whitespace(city))
    state_clean = '' if state is None else state.strip().upper()
    country_clean = '' if country is None else country.strip().upper()

    return {
        'address_id': input_row.get('address_id'),
        'street_clean': street_clean,
        'city_clean': city_clean,
        'state_clean': state_clean,
        'country_clean': country_clean,
        'zip': input_row.get('zip'),
    }


logger.warning(
    "tJavaRow_1 translation is UNRESOLVED/uncertain: com.sample.util.StringUtils "
    "normalizeWhitespace/titleCase semantics were approximated with regex + "
    "str.title(). Validate against the real StringUtils jar before trusting "
    "output for production use."
)


# ---------------------------------------------------------------------------
# AddressStandardizationJoblet (modelled as a Python function standing in
# for the joblet subjob: tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1).
# ---------------------------------------------------------------------------
_java_row_1_udf_schema_order = [
    "address_id", "street_clean", "city_clean", "state_clean", "zip", "country_clean",
]


def address_standardization_joblet(df):
    """tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1"""

    def _row_fn(address_id, street, city, state, zip_, country):
        result = _java_row_1({
            'address_id': address_id,
            'street': street,
            'city': city,
            'state': state,
            'zip': zip_,
            'country': country,
        })
        return (
            result['street_clean'],
            result['city_clean'],
            result['state_clean'],
            result['country_clean'],
        )

    from pyspark.sql.types import StructType, StructField

    result_schema = StructType([
        StructField("street_clean", StringType()),
        StructField("city_clean", StringType()),
        StructField("state_clean", StringType()),
        StructField("country_clean", StringType()),
    ])

    row_udf = F.udf(_row_fn, result_schema)

    out = df.withColumn(
        "_std",
        row_udf(
            F.col("address_id"),
            F.col("street"),
            F.col("city"),
            F.col("state"),
            F.col("zip"),
            F.col("country"),
        ),
    )
    out = out.select(
        F.col("address_id"),
        F.col("_std.street_clean").alias("street_clean"),
        F.col("_std.city_clean").alias("city_clean"),
        F.col("_std.state_clean").alias("state_clean"),
        F.col("zip"),
        F.col("_std.country_clean").alias("country_clean"),
    )
    return out


def run_main_subjob():
    """job_09_joblet_demo_main:
    tFileInputDelimited_1 -> tAddressStandardizationJoblet_1 -> tFileOutputDelimited_1
    """
    logger.info("Reading input delimited file from %s", input_path)

    raw_schema_cols = ["address_id", "street", "city", "state", "zip", "country"]

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .load(input_path)
        .select(*raw_schema_cols)
    )

    logger.info("Invoking AddressStandardizationJoblet")
    standardized_df = address_standardization_joblet(df)

    logger.info("Writing standardized output to %s", output_path)
    (
        standardized_df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .save(output_path)
    )

    logger.info("job_09_joblet_demo_main completed successfully")


if __name__ == "__main__":
    try:
        run_main_subjob()
    except Exception as exc:
        logger.error("job_09_joblet_demo failed: %s", exc, exc_info=True)
        raise
