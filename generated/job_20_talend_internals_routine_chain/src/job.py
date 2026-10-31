import sys
import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_20_talend_internals_routine_chain")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Context variables.
#
# source_dir is a path-style context variable. The spec's own "dev" value
# ("samples/talend_export_src/fixtures") is a local-generation artifact, not
# a real path in this workspace -- the actual fixture files for this sample
# job live flat under the UC Volume below. We still make this overridable
# (sys.argv[1] if this task is ever given a real job parameter, else an
# environment variable, else the known-good fixtures Volume default) instead
# of hardcoding a single literal with no way to point elsewhere -- this
# addresses the prior review finding that the constant silently forced dev
# fixture data with no override path for other environments.
# ---------------------------------------------------------------------------
DEFAULT_SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"


def _resolve_source_dir() -> str:
    if len(sys.argv) > 1 and sys.argv[1]:
        return sys.argv[1]
    env_val = os.environ.get("TALEND_SOURCE_DIR")
    if env_val:
        return env_val
    return DEFAULT_SOURCE_DIR


SOURCE_DIR = _resolve_source_dir()

# Non-path context values can use the spec's dev value literally.
TARGET_SCHEMA = os.environ.get("TALEND_TARGET_SCHEMA", "dev_samples")
MAX_CLAIM_AMOUNT = int(os.environ.get("TALEND_MAX_CLAIM_AMOUNT", "5000"))
BATCH_RUN_PREFIX = os.environ.get("TALEND_BATCH_RUN_PREFIX", "DEV")

TABLE_NAME = "claims_validated"
FULL_TABLE_NAME = f"talend_migration.samples.{TABLE_NAME}"


# ---------------------------------------------------------------------------
# ClaimRoutines.validateClaim translation (pure routine, referenced by
# tJavaRow_1's translated block).
# ---------------------------------------------------------------------------
def validate_claim(claim_id, amount, max_claim_amount):
    """
    Mirrors routines.ClaimRoutines.validateClaim(claimId, amount, maxClaimAmount).
    Returns a claim code string, or None if claim is fine.
    FATAL code triggers the fatal flow; non-fatal codes are logged via audit line.
    """
    if claim_id is None:
        return "FATAL_MISSING_CLAIM_ID"
    if amount is None or amount < 0:
        return "FATAL_INVALID_AMOUNT"
    if amount > max_claim_amount:
        return "REVIEW_OVER_LIMIT"
    return None


# ---------------------------------------------------------------------------
# tJava_0: one-shot setup block (translated, RESTRUCTURED tier).
# Produces batchRunId and initial job20_fatalFlag, used as driver-level state.
# ---------------------------------------------------------------------------
def tjava_0_run(batch_run_prefix: str) -> dict:
    current_time_millis = int(time.time() * 1000)
    batch_run_id = f"{batch_run_prefix}-claims-{current_time_millis}"
    return {
        "batchRunId": batch_run_id,
        "job20_fatalFlag": False,
    }


global_map = tjava_0_run(BATCH_RUN_PREFIX)
batch_run_id = global_map["batchRunId"]
logger.info("tJava_0: batchRunId=%s", batch_run_id)


# ---------------------------------------------------------------------------
# tFileInputDelimited_1: read claims_batch.csv
# ---------------------------------------------------------------------------
source_path = f"{SOURCE_DIR}/claims_batch.csv"

input_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("claim_status", StringType(), True),
])

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .schema(input_schema)
    .load(source_path)
)

# claim_status is read from source but intentionally dropped downstream,
# per spec uncertainty note -- not propagated into tJavaRow_1's output.
raw_df = raw_df.drop("claim_status")
raw_df.createOrReplaceTempView("claims_raw")


# ---------------------------------------------------------------------------
# tJavaRow_1: per-row validation via mapInPandas (RESTRUCTURED translation).
# Fatal detection is carried back as a per-row flag column and aggregated at
# the driver level afterward (no raw RDD APIs).
# ---------------------------------------------------------------------------
output_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("claim_code", StringType(), True),
    StructField("batch_run_id", StringType(), True),
    StructField("is_fatal", StringType(), True),  # "true"/"false" flag per row
])


def _tjavarow_1_partition(iterator):
    for pdf in iterator:
        claim_ids = []
        policy_ids = []
        claim_amounts = []
        claim_codes = []
        batch_run_ids = []
        is_fatals = []

        for _, row in pdf.iterrows():
            claim_id = row["claim_id"]
            policy_id = row["policy_id"]
            claim_amount = row["claim_amount"]
            claim_amount_d = 0.0 if pd.isna(claim_amount) else float(claim_amount)

            claim_code = validate_claim(claim_id, claim_amount_d, float(MAX_CLAIM_AMOUNT))

            is_fatal = False
            if claim_code is not None and "FATAL" in claim_code:
                is_fatal = True
                sys.stderr.write(
                    "tJavaRow_1: FATAL claim {} code={} batch={}\n".format(
                        claim_id, claim_code, batch_run_id
                    )
                )
            elif claim_code is not None:
                audit = "[claims-validation batch={}] claim={} policy={} code={}".format(
                    batch_run_id, claim_id, policy_id, claim_code
                )
                sys.stdout.write(audit + "\n")

            claim_ids.append(claim_id)
            policy_ids.append(policy_id)
            claim_amounts.append(claim_amount if not pd.isna(claim_amount) else None)
            claim_codes.append("" if claim_code is None else claim_code)
            batch_run_ids.append(batch_run_id)
            is_fatals.append("true" if is_fatal else "false")

        yield pd.DataFrame({
            "claim_id": claim_ids,
            "policy_id": policy_ids,
            "claim_amount": claim_amounts,
            "claim_code": claim_codes,
            "batch_run_id": batch_run_ids,
            "is_fatal": is_fatals,
        })


validated_df = raw_df.mapInPandas(_tjavarow_1_partition, schema=output_schema)
validated_df.createOrReplaceTempView("claims_validated_raw")

# Driver-level fatal detection: mirrors globalMap.put("job20_fatalFlag", true)
# happening on any row, then RUN_IF routing to tDie_1.
fatal_count = validated_df.filter(col("is_fatal") == "true").count()
if fatal_count > 0:
    global_map["job20_fatalFlag"] = True

# tWarn_1: ON_COMPONENT_ERROR soft-fail path -- log a warning if any fatal rows
# were encountered at the row level (component-level error indicator).
if fatal_count > 0:
    logger.warning(
        "tJavaRow_1 soft-failed a claim validation, batch=%s", batch_run_id
    )

# Final output: drop the internal is_fatal flag column, matching tJavaRow_1's
# actual output_row schema (claim_id, policy_id, claim_amount, claim_code, batch_run_id).
final_df = validated_df.drop("is_fatal")


# ---------------------------------------------------------------------------
# tDBOutput_1: INSERT into claims_validated Delta table.
# ---------------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
        claim_id STRING,
        policy_id STRING,
        claim_amount DOUBLE,
        claim_code STRING,
        batch_run_id STRING
    ) USING DELTA
""")

final_df.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME)
logger.info("tDBOutput_1: inserted %d rows into %s", final_df.count(), FULL_TABLE_NAME)


# ---------------------------------------------------------------------------
# tDie_1: RUN_IF job20_fatalFlag == true -> abort the job.
# ---------------------------------------------------------------------------
if global_map.get("job20_fatalFlag") is True:
    error_message = (
        f"job_20_talend_internals_routine_chain: fatal claim detected in batch "
        f"{batch_run_id}"
    )
    logger.error(error_message)
    raise RuntimeError(f"{error_message} (ERROR_CODE=4)")

logger.info("job_20_talend_internals_routine_chain completed successfully, batch=%s", batch_run_id)
