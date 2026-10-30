import re
import os
import logging
from pyspark.sql import functions as F, SparkSession

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_09_joblet_demo")

# -----------------------------------------------------------------------------
# Context parameters (mirrors Talend context.source_dir)
# -----------------------------------------------------------------------------
# source_dir is expected to be a Unity Catalog Volume path, e.g.
# /Volumes/talend_migration/samples/landing
try:
    context_source_dir = dbutils.widgets.get("source_dir")
except Exception:
    context_source_dir = "/Volumes/talend_migration/samples/landing"

context_source_dir = context_source_dir.rstrip("/")

INPUT_PATH = f"{context_source_dir}/addresses_raw.csv"

# -----------------------------------------------------------------------------
# Output path resolution
# -----------------------------------------------------------------------------
# The original Talend FILENAME expression was
#   (String)context.source_dir + "/../fixtures_output/addresses_standardized.csv"
# On a Talend file system that meant "a sibling directory of source_dir".
# Resolving ".." against a Unity Catalog Volume path is NOT safe: for a shallow
# source_dir such as /Volumes/<catalog>/<schema> the parent collapses to
# /Volumes/<catalog>, which escapes the volume (and even the schema) entirely.
# A startswith("/Volumes/") check cannot detect that. So instead of computing a
# parent directory at all, the output directory is now an EXPLICIT parameter
# (output_dir) that defaults to a "fixtures_output" directory INSIDE the same
# volume root as source_dir. The volume root is identified positionally as the
# first three path segments after /Volumes (catalog/schema/volume); the output
# path is then validated to live under that exact root, so no ".." traversal
# and no silent escape to a different catalog/schema/volume is possible.

_VOLUMES_PREFIX = "/Volumes/"


def _volume_root(path: str) -> str:
    """Return /Volumes/<catalog>/<schema>/<volume> for a Volume path."""
    if not path.startswith(_VOLUMES_PREFIX):
        raise ValueError(
            f"Path '{path}' is not a Unity Catalog Volume path (expected it to "
            f"start with {_VOLUMES_PREFIX})."
        )
    parts = [p for p in path[len(_VOLUMES_PREFIX):].split("/") if p]
    if len(parts) < 3:
        raise ValueError(
            f"Path '{path}' is too shallow to identify a volume root; expected "
            f"{_VOLUMES_PREFIX}<catalog>/<schema>/<volume>/..."
        )
    return _VOLUMES_PREFIX + "/".join(parts[:3])


SOURCE_VOLUME_ROOT = _volume_root(context_source_dir)

try:
    _output_dir_param = dbutils.widgets.get("output_dir").rstrip("/")
except Exception:
    _output_dir_param = ""

if _output_dir_param:
    OUTPUT_DIR = _output_dir_param
else:
    # Default: sibling-style "fixtures_output" directory, but anchored at the
    # volume root rather than at the parent of source_dir.
    OUTPUT_DIR = f"{SOURCE_VOLUME_ROOT}/fixtures_output"

# Reject any ".." segment outright rather than silently resolving it, then
# confirm the result is contained within the source volume root.
if any(seg == os.pardir for seg in OUTPUT_DIR.split("/")):
    raise ValueError(
        f"output_dir '{OUTPUT_DIR}' contains a '..' path segment; relative "
        "traversal is not supported for Unity Catalog Volume paths."
    )

if OUTPUT_DIR != SOURCE_VOLUME_ROOT and not OUTPUT_DIR.startswith(
    SOURCE_VOLUME_ROOT + "/"
):
    raise ValueError(
        f"Resolved output directory '{OUTPUT_DIR}' is not contained within the "
        f"source volume root '{SOURCE_VOLUME_ROOT}'; refusing to write outside "
        "the intended catalog/schema/volume boundary."
    )

OUTPUT_PATH = f"{OUTPUT_DIR}/addresses_standardized.csv"


# -----------------------------------------------------------------------------
# tJavaRow_1 (inside joblet AddressStandardizationJoblet) -- translated verbatim
# from the JAVAROUTINE agent. UNRESOLVED tier: the core normalizeWhitespace /
# titleCase calls from com.sample.util.StringUtils have no available Python
# port or JAR/classpath to bridge via py4j, so they are implemented as clearly
# marked best-effort STUBs. This is surfaced below as a loud warning at run
# time (matching "tWarn"-style non-fatal logging, since the original Talend
# job has no reject/exception routing configured for this block either) so
# the discrepancy is visible in logs rather than silently passing.
# -----------------------------------------------------------------------------

def _normalize_whitespace_STUB(s: str) -> str:
    # TODO: replace with faithful port of com.sample.util.StringUtils.normalizeWhitespace
    # Best-effort guess only: collapse runs of whitespace, strip ends.
    return re.sub(r"\s+", " ", s).strip()


def _title_case_STUB(s: str) -> str:
    # TODO: replace with faithful port of com.sample.util.StringUtils.titleCase
    # Best-effort guess only: Python's str.title() does NOT match Java title-casing
    # rules for words containing apostrophes, digits, or mixed punctuation
    # (e.g. "O'BRIEN" -> "O'Brien" vs Python's "O'brien").
    return s.title()


def tJavaRow_1_run(input: dict) -> dict:
    street = input.get("street")
    city = input.get("city") if input.get("city") is not None else ""
    state = input.get("state")
    country = input.get("country")

    output = {}
    output["address_id"] = input.get("address_id")
    output["street_clean"] = _title_case_STUB(_normalize_whitespace_STUB(street))
    output["city_clean"] = _title_case_STUB(_normalize_whitespace_STUB(city))
    output["state_clean"] = "" if state is None else state.strip().upper()
    output["country_clean"] = "" if country is None else country.strip().upper()
    output["zip"] = input.get("zip")
    return output


logger.warning(
    "tJavaRow_1 (AddressStandardizationJoblet): UNRESOLVED dependency on "
    "com.sample.util.StringUtils.normalizeWhitespace/titleCase. No JAR/classpath "
    "or Python port was available to faithfully translate these calls. Running "
    "with best-effort STUB implementations (regex whitespace collapse + "
    "str.title()) for street_clean/city_clean -- these outputs are NOT "
    "guaranteed to match the original Java behavior and should be validated "
    "against known-good fixtures before this job is trusted in production."
)

_tJavaRow_1_udf = F.udf(
    lambda address_id, street, city, state, zip_, country: tJavaRow_1_run(
        {
            "address_id": address_id,
            "street": street,
            "city": city,
            "state": state,
            "zip": zip_,
            "country": country,
        }
    ),
    returnType="struct<address_id:string,street_clean:string,city_clean:string,state_clean:string,country_clean:string,zip:string>",
)


# -----------------------------------------------------------------------------
# Joblet: AddressStandardizationJoblet
# tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1
# Implemented as a plain function taking/returning a DataFrame, since this
# joblet has a single linear flow and is invoked once from main_job.
# -----------------------------------------------------------------------------

def run_address_standardization_joblet(df):
    """tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1"""
    mapped = df.withColumn(
        "_jr",
        _tJavaRow_1_udf(
            F.col("address_id"),
            F.col("street"),
            F.col("city"),
            F.col("state"),
            F.col("zip"),
            F.col("country"),
        ),
    )
    out = mapped.select(
        F.col("_jr.address_id").alias("address_id"),
        F.col("_jr.street_clean").alias("street_clean"),
        F.col("_jr.city_clean").alias("city_clean"),
        F.col("_jr.state_clean").alias("state_clean"),
        F.col("_jr.zip").alias("zip"),
        F.col("_jr.country_clean").alias("country_clean"),
    )
    return out


# -----------------------------------------------------------------------------
# main_job
# tFileInputDelimited_1 -> tAddressStandardizationJoblet_1 -> tFileOutputDelimited_1
# -----------------------------------------------------------------------------

def main():
    logger.info("Reading input file: %s", INPUT_PATH)
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .load(INPUT_PATH)
        .select("address_id", "street", "city", "state", "zip", "country")
    )

    standardized_df = run_address_standardization_joblet(raw_df)

    logger.info(
        "Writing standardized output to: %s (volume root: %s)",
        OUTPUT_PATH,
        SOURCE_VOLUME_ROOT,
    )
    (
        standardized_df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .save(OUTPUT_PATH)
    )

    logger.info("job_09_joblet_demo completed.")


if __name__ == "__main__":
    main()
