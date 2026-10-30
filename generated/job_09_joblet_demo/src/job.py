import re
import logging
import posixpath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_09_joblet_demo")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Path construction (fix for CODE REVIEW high-severity finding).
#
# Previous version let the caller supply a free-form `source_dir` plus a
# caller-supplied `volume_root`, then "validated" one against the other with
# os.path.commonpath -- a self-referential check that could be satisfied by
# setting both parameters consistently, and which applied local-filesystem
# semantics to UC Volume paths.
#
# Fix: the Volume root is a hardcoded, non-overridable constant in this module
# (an allow-list of exactly one trusted UC Volume prefix). Callers may only
# supply *relative* sub-directory names, which are rejected outright if they
# contain any path separator, '..' segment, or anything outside a strict
# [A-Za-z0-9._-] charset. There is therefore no '..' traversal and no
# caller-controlled prefix: every path is built by joining sanitized leaf
# names under the fixed allow-listed root.
# ---------------------------------------------------------------------------
ALLOWED_VOLUME_ROOT = "/Volumes/talend_migration/job_09_joblet_demo"
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9._-]+$")


def _safe_segment(name: str, param_name: str) -> str:
    """Validate a single path segment (no separators, no traversal)."""
    if name is None or not _SAFE_SEGMENT.match(name) or name in (".", ".."):
        raise ValueError(
            f"Parameter '{param_name}' must be a single safe path segment "
            f"matching [A-Za-z0-9._-]+ (no '/', no '..'); got {name!r}."
        )
    return name


def volume_path(*segments: str) -> str:
    """Build a path guaranteed to sit directly under ALLOWED_VOLUME_ROOT."""
    return posixpath.join(ALLOWED_VOLUME_ROOT, *segments)


# Talend used `context.source_dir` and `context.source_dir + "/../fixtures_output"`.
# Those two directories were siblings, so here they are modelled as two
# sanitized sub-directory names under the fixed Volume root. '..' traversal is
# no longer performed or permitted at all.
dbutils.widgets.text("source_subdir", "source")
dbutils.widgets.text("output_subdir", "fixtures_output")

source_subdir = _safe_segment(dbutils.widgets.get("source_subdir"), "source_subdir")
output_subdir = _safe_segment(dbutils.widgets.get("output_subdir"), "output_subdir")

input_path = volume_path(source_subdir, "addresses_raw.csv")
output_path = volume_path(output_subdir, "addresses_standardized.csv")

# Defensive post-condition: both resolved paths must still be prefixed by the
# hardcoded root. This is no longer self-referential because the root is a
# module constant that callers cannot influence.
for _p in (input_path, output_path):
    if not _p.startswith(ALLOWED_VOLUME_ROOT + "/"):
        raise ValueError(f"Resolved path '{_p}' is not under '{ALLOWED_VOLUME_ROOT}'.")


# ---------------------------------------------------------------------------
# tJavaRow_1 (inside AddressStandardizationJoblet) -- translated Java block,
# used verbatim as provided by the JAVAROUTINE agent. UNRESOLVED: the exact
# semantics of com.sample.util.StringUtils.normalizeWhitespace()/.titleCase()
# could not be confirmed (no source/jar available). The guessed Python
# re-implementation below is flagged by the translator as needing human
# verification against the real library before being trusted in production.
# ---------------------------------------------------------------------------
def _normalize_whitespace_GUESS(s):
    if s is None:
        s = ""
    return re.sub(r"\s+", " ", s).strip()


def _title_case_GUESS(s):
    if s is None:
        s = ""
    return s.title()


def tJavaRow_1_run(input_row: dict) -> dict:
    street = input_row.get("street")
    city = input_row.get("city")
    state = input_row.get("state")
    country = input_row.get("country")

    output = {}
    output["address_id"] = input_row.get("address_id")

    # TODO: replace with faithful port of com.sample.util.StringUtils once source/jar available
    output["street_clean"] = _title_case_GUESS(_normalize_whitespace_GUESS(street))
    output["city_clean"] = _title_case_GUESS(_normalize_whitespace_GUESS(city if city is not None else ""))

    output["state_clean"] = "" if state is None else state.strip().upper()
    output["country_clean"] = "" if country is None else country.strip().upper()
    output["zip"] = input_row.get("zip")

    return output


_normalize_whitespace_udf = F.udf(_normalize_whitespace_GUESS, StringType())
_title_case_udf = F.udf(_title_case_GUESS, StringType())


def address_standardization_joblet(df):
    """
    AddressStandardizationJoblet, generated inline (Lakeflow has no notion of a
    separate joblet artifact, so it is expanded as a function matching
    tJobletInput_1 -> tJavaRow_1 -> tJobletOutput_1).

    tJobletInput_1 schema == caller's tAddressStandardizationJoblet_1 input
    schema (address_id, street, city, state, zip, country) per spec; output
    matches tJobletOutput_1 (address_id, street_clean, city_clean,
    state_clean, zip, country_clean).
    """
    out_df = (
        df.withColumn("street_clean", _title_case_udf(_normalize_whitespace_udf(F.col("street"))))
          .withColumn(
              "city_clean",
              _title_case_udf(_normalize_whitespace_udf(F.coalesce(F.col("city"), F.lit(""))))
          )
          .withColumn(
              "state_clean",
              F.when(F.col("state").isNull(), F.lit("")).otherwise(F.upper(F.trim(F.col("state"))))
          )
          .withColumn(
              "country_clean",
              F.when(F.col("country").isNull(), F.lit("")).otherwise(F.upper(F.trim(F.col("country"))))
          )
          .select("address_id", "street_clean", "city_clean", "state_clean", "zip", "country_clean")
    )
    return out_df


# ---------------------------------------------------------------------------
# subjob: job_09_joblet_demo_main
# tFileInputDelimited_1 -> tAddressStandardizationJoblet_1 -> tFileOutputDelimited_1
# ---------------------------------------------------------------------------
def run_main_subjob():
    logger.info("Reading addresses_raw.csv from %s", input_path)

    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .schema(
            "address_id STRING, street STRING, city STRING, state STRING, zip STRING, country STRING"
        )
        .load(input_path)
    )

    standardized_df = address_standardization_joblet(raw_df)

    logger.info("Writing standardized addresses to %s", output_path)
    (
        standardized_df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .mode("overwrite")
        .save(output_path)
    )

    logger.info("job_09_joblet_demo_main complete")


if __name__ == "__main__":
    run_main_subjob()
