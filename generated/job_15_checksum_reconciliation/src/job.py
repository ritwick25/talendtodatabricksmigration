import sys
import logging
from decimal import Decimal, ROUND_HALF_UP
import zlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, LongType,
    IntegerType, DecimalType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_15_checksum_reconciliation")

# ---------------------------------------------------------------------------
# Context variables (Talend context.* -> module constants)
# ---------------------------------------------------------------------------
# NOTE: context.source_dir's spec 'dev' value ("samples/talend_export_src/fixtures")
# is a local-generation artifact, not a real path in this workspace. The actual
# fixture files for sample jobs live flat under the Volume path below.
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"

# Output dir for the reject file. The original Talend relative path
# ("<source_dir>/../fixtures_output/...") doesn't resolve meaningfully against
# the Volume fixtures path, so we write rejects to a sibling Volume location.
OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output"

TARGET_SCHEMA = "talend_migration.samples"  # Delta catalog.schema used instead of live JDBC
TARGET_TABLE = "ledger_reconciled"

LEDGER_ENTRIES_FILE = f"{SOURCE_DIR}/ledger_entries.csv"
LEDGER_CONTROL_TOTALS_FILE = f"{SOURCE_DIR}/ledger_control_totals.csv"
MISMATCHES_OUTPUT_FILE = f"{OUTPUT_DIR}/ledger_mismatches.csv"


# ---------------------------------------------------------------------------
# tJavaRow_1 translated block (verbatim logic from JAVAROUTINE agent)
# ---------------------------------------------------------------------------

def _set_scale_2(value):
    if value is None:
        d = Decimal(0)
    else:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
    return d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def _nearly_equal(a, b, eps):
    # Mirrors com.sample.util.ChecksumUtil.nearlyEqual(double,double,double):
    # a simple absolute-difference tolerance check on doubles.
    return abs(float(a) - float(b)) <= eps


def _luhn_check(account_id):
    # Mirrors com.sample.util.ChecksumUtil.luhnCheck(String): standard Luhn
    # checksum over the digit characters of account_id; non-digit chars
    # (and null/empty) are treated as invalid, matching typical ChecksumUtil
    # defensive behavior on malformed input.
    if account_id is None:
        return False
    s = str(account_id)
    digits = [c for c in s]
    if not digits or not all(c.isdigit() for c in digits):
        return False
    total = 0
    reverse_digits = [int(c) for c in reversed(digits)]
    for i, d in enumerate(reverse_digits):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _crc32_hex(entry_id):
    # Mirrors com.sample.util.ChecksumUtil.crc32Hex(String): CRC32 of the
    # UTF-8 bytes of the string, rendered as lowercase hex (unsigned).
    if entry_id is None:
        data = b''
    else:
        data = str(entry_id).encode('utf-8')
    crc = zlib.crc32(data) & 0xFFFFFFFF
    return format(crc, '08x')


FLAG_DEBIT = 1
FLAG_CREDIT = 2
FLAG_LARGE = 4
FLAG_MISMATCH = 8

INT32_MIN = -2147483648
INT32_MAX = 2147483647


class ArithmeticException_Replacement(Exception):
    """Raised to mirror Java's ArithmeticException from BigDecimal.longValueExact()
    when an exact conversion is not possible."""
    pass


def _describe_flags(flags):
    parts = []
    if flags & 1:
        parts.append('DEBIT ')
    if flags & 2:
        parts.append('CREDIT ')
    if flags & 4:
        parts.append('LARGE ')
    if flags & 8:
        parts.append('MISMATCH ')
    return 'flags=[' + ''.join(parts) + ']'


def _java_row_run(input: dict) -> dict:
    debit = _set_scale_2(input.get('debit'))
    credit = _set_scale_2(input.get('credit'))
    net = (debit - credit).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    control_total = input.get('control_total')
    control = _set_scale_2(control_total) if control_total is not None else Decimal('0.00')

    flags = 0
    if debit > Decimal('0'):
        flags |= FLAG_DEBIT
    if credit > Decimal('0'):
        flags |= FLAG_CREDIT
    if abs(net) >= Decimal('10000.00'):
        flags |= FLAG_LARGE

    matches = _nearly_equal(net, control, 0.005)
    if not matches:
        flags |= FLAG_MISMATCH

    account_id = input.get('account_id')
    account_luhn_valid = _luhn_check(account_id)

    entry_id = input.get('entry_id')
    entry_crc = _crc32_hex(entry_id)

    # net.movePointRight(2).longValueExact(): exact scaling to cents, raising
    # on any fractional remainder -- mirrored via Decimal exact arithmetic.
    cents_decimal = net * Decimal(100)
    cents_int = int(cents_decimal)
    if Decimal(cents_int) != cents_decimal:
        raise ArithmeticException_Replacement(
            f"Rounding necessary: {net} cannot be converted exactly to cents"
        )
    cents = cents_int

    overflow_safe = INT32_MIN <= cents <= INT32_MAX

    if not account_luhn_valid:
        flag_desc = _describe_flags(flags)
        print(
            f"tJavaRow_1: account_id for entry {entry_id} failed Luhn validation -- {flag_desc}",
        )

    return {
        'entry_id': entry_id,
        'net_amount': net,
        'flags_bitmask': flags,
        'flag_mismatch': (flags & FLAG_MISMATCH) != 0,
        'account_luhn_valid': account_luhn_valid,
        'entry_crc32': entry_crc,
        'net_cents': cents,
        'cents_int_overflow_safe': overflow_safe,
    }


_JAVA_ROW_OUTPUT_SCHEMA = StructType([
    StructField("entry_id", StringType(), True),
    StructField("net_amount", DecimalType(18, 2), True),
    StructField("flags_bitmask", IntegerType(), True),
    StructField("flag_mismatch", BooleanType(), True),
    StructField("account_luhn_valid", BooleanType(), True),
    StructField("entry_crc32", StringType(), True),
    StructField("net_cents", LongType(), True),
    StructField("cents_int_overflow_safe", BooleanType(), True),
])


def _java_row_map_partition(pdf_iter):
    """mapInPandas-compatible function applying _java_row_run row-by-row."""
    import pandas as pd

    for pdf in pdf_iter:
        records = pdf.to_dict(orient="records")
        out_records = [_java_row_run(r) for r in records]
        out_pdf = pd.DataFrame(out_records, columns=[f.name for f in _JAVA_ROW_OUTPUT_SCHEMA.fields])
        yield out_pdf


def main():
    spark = SparkSession.builder.getOrCreate()

    # -----------------------------------------------------------------
    # tFileInputDelimited_1 / tFileInputDelimited_2
    # -----------------------------------------------------------------
    ledger_entries = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(LEDGER_ENTRIES_FILE)
        .select(
            F.col("entry_id").cast(StringType()),
            F.col("account_id").cast(StringType()),
            F.col("debit").cast(DecimalType(18, 2)),
            F.col("credit").cast(DecimalType(18, 2)),
            F.col("entry_date").cast(StringType()),
        )
    )

    control_totals = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(LEDGER_CONTROL_TOTALS_FILE)
        .select(
            F.col("account_id").cast(StringType()),
            F.col("control_total").cast(DecimalType(18, 2)),
        )
    )

    # -----------------------------------------------------------------
    # tMap_1: left outer join on account_id (LOAD_ONCE lookup -- control
    # totals are read once above, standard batch semantics apply)
    # -----------------------------------------------------------------
    mapped = (
        ledger_entries.alias("row1")
        .join(control_totals.alias("row2"), on="account_id", how="left_outer")
        .select(
            F.col("row1.entry_id").alias("entry_id"),
            F.col("row1.account_id").alias("account_id"),
            F.col("row1.debit").alias("debit"),
            F.col("row1.credit").alias("credit"),
            F.col("row2.control_total").alias("control_total"),
        )
    )

    # -----------------------------------------------------------------
    # tJavaRow_1: per-row UDF logic via mapInPandas (no raw RDD API)
    # -----------------------------------------------------------------
    java_row_input_cols = ["entry_id", "account_id", "debit", "credit", "control_total"]
    java_row_out = mapped.select(*java_row_input_cols).mapInPandas(
        _java_row_map_partition, schema=_JAVA_ROW_OUTPUT_SCHEMA
    )

    # -----------------------------------------------------------------
    # tFilterRow_1: main (!flag_mismatch) vs REJECT (flag_mismatch)
    # -----------------------------------------------------------------
    reconciled_df = java_row_out.filter(~F.col("flag_mismatch"))
    mismatches_df = java_row_out.filter(F.col("flag_mismatch"))

    # -----------------------------------------------------------------
    # tDBOutput_1: ACTION=INSERT -> append to Delta table
    # -----------------------------------------------------------------
    full_table_name = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            entry_id STRING,
            net_amount DECIMAL(18,2),
            flags_bitmask INT,
            flag_mismatch BOOLEAN,
            account_luhn_valid BOOLEAN,
            entry_crc32 STRING,
            net_cents BIGINT,
            cents_int_overflow_safe BOOLEAN
        ) USING DELTA
    """)

    reconciled_df.write.format("delta").mode("append").saveAsTable(full_table_name)
    logger.info("Wrote %d reconciled rows to %s", reconciled_df.count(), full_table_name)

    # -----------------------------------------------------------------
    # tFileOutputDelimited_1 (REJECT branch): mismatches -> CSV
    # -----------------------------------------------------------------
    (
        mismatches_df.write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(MISMATCHES_OUTPUT_FILE)
    )
    logger.info("Wrote mismatch rows to %s", MISMATCHES_OUTPUT_FILE)


if __name__ == "__main__":
    main()
