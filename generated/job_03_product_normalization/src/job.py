import re
import unicodedata
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job_03_product_normalization")

# ---------------------------------------------------------------------------
# Context variables (Talend context.* -> module-level constants).
# The spec's own "dev" value for source_dir is null / not a real workspace
# path, so per migration convention we point at the fixtures volume that
# actually holds this job's sample input files in this workspace.
# ---------------------------------------------------------------------------
SOURCE_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures"

# Original Talend FILENAME params used "context.source_dir + /../fixtures_output/...";
# since source_dir now points directly at the fixtures volume (no nested subdir to go
# "up" from), write outputs to a sibling-style output area under the same volume root.
OUTPUT_DIR = "/Volumes/talend_migration/samples/talend_export/samples/_fixtures_output"

INPUT_A = f"{SOURCE_DIR}/products_supplier_a.csv"
INPUT_B = f"{SOURCE_DIR}/products_supplier_b.csv"
OUTPUT_NORMALIZED = f"{OUTPUT_DIR}/products_normalized.csv"
OUTPUT_DENORMALIZED = f"{OUTPUT_DIR}/products_denormalized.csv"

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# tJavaRow_1 translated block (verbatim from JAVAROUTINE agent), wired in via
# a pandas-UDF-friendly row function -- applied with mapInPandas, never raw RDD APIs.
# ---------------------------------------------------------------------------
SKU_PATTERN = re.compile(r"SKU-(?P<year>\d{4})-(?P<seq>\d{5})-(?P<rev>[A-Z])")
_COMBINING_MARKS_RE = re.compile(r"[\u0300-\u036f]")


def _normalize_whitespace(s):
    if s is None:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def _title_case(s):
    if s is None:
        return ""
    return " ".join(w[:1].upper() + w[1:].lower() if w else w for w in s.split(" "))


def _slugify(name):
    normalized_ws = _normalize_whitespace(name)
    decomposed = unicodedata.normalize("NFD", normalized_ws)
    ascii_str = _COMBINING_MARKS_RE.sub("", decomposed).lower()
    sb = []
    last_dash = False
    for c in ascii_str:
        if c.isalnum():
            sb.append(c)
            last_dash = False
        else:
            if not last_dash:
                sb.append("-")
                last_dash = True
    slug = "".join(sb)
    slug = slug.rstrip("-")
    slug = slug.lstrip("-")
    return slug


def run(input: dict) -> dict:
    name = input.get("name")
    sku_raw = input.get("sku_raw") if input.get("sku_raw") is not None else ""
    tags_csv = input.get("tags_original") if input.get("tags_original") is not None else ""

    output_row = {}
    output_row["product_id"] = input.get("product_id")
    output_row["tag"] = input.get("tag")
    output_row["slug"] = _slugify(name if name is not None else "")

    display_name = _title_case(_normalize_whitespace(name if name is not None else ""))
    output_row["display_label"] = "%s (%s)" % (display_name, sku_raw)

    m = SKU_PATTERN.search(sku_raw)
    if m:
        output_row["sku_year"] = m.group("year")
        output_row["sku_seq"] = m.group("seq")
        output_row["sku_rev"] = m.group("rev")
    else:
        output_row["sku_year"] = ""
        output_row["sku_seq"] = ""
        output_row["sku_rev"] = ""

    parts_all = re.split(r"\|", tags_csv)

    trimming = list(parts_all)
    while trimming and trimming[-1] == "":
        trimming.pop()

    preserving = parts_all

    output_row["tags_trimming_count"] = len(trimming)
    output_row["tags_preserving_count"] = len(preserving)

    return output_row


def _java_row_partition_fn(iterator):
    import pandas as pd
    for pdf in iterator:
        records = pdf.to_dict(orient="records")
        out_records = [run(r) for r in records]
        out_pdf = pd.DataFrame(
            out_records,
            columns=[
                "product_id", "tag", "slug", "display_label",
                "sku_year", "sku_seq", "sku_rev",
                "tags_trimming_count", "tags_preserving_count",
            ],
        )
        yield out_pdf


JAVAROW_OUTPUT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("display_label", StringType(), True),
    StructField("sku_year", StringType(), True),
    StructField("sku_seq", StringType(), True),
    StructField("sku_rev", StringType(), True),
    StructField("tags_trimming_count", IntegerType(), True),
    StructField("tags_preserving_count", IntegerType(), True),
])


def read_supplier_csv(path):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(
            StructType([
                StructField("product_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("tags", StringType(), True),
                StructField("categories", StringType(), True),
                StructField("price", StringType(), True),
            ])
        )
        .load(path)
        .withColumn("price", F.col("price").cast("decimal(10,2)"))
    )


def main():
    logger.info("Reading supplier A from %s", INPUT_A)
    df_a = read_supplier_csv(INPUT_A)

    logger.info("Reading supplier B from %s", INPUT_B)
    df_b = read_supplier_csv(INPUT_B)

    # tUnite_1: UNION_ALL = true
    df_united = df_a.unionByName(df_b)

    # tReplicate_1: fan-out, used as-is for both downstream branches (no transform)
    df_replicated = df_united

    # -----------------------------------------------------------------
    # Branch 1: tNormalize_1 -> tJavaRow_1 -> tFileOutputDelimited_1
    # tNormalize_1 splits the 'tags' column on '|' into one row per tag,
    # keeping the original tags string as tags_original.
    # -----------------------------------------------------------------
    df_normalized = (
        df_replicated
        .withColumn("tags_original", F.col("tags"))
        .withColumn("tag", F.explode(F.split(F.col("tags"), r"\|")))
        .select("product_id", "name", "tag", "tags_original")
    )

    # tNormalize_1's declared output schema references a 'sku_raw' column that
    # does not exist anywhere upstream (noted as an unresolved schema gap in the
    # spec's uncertainties). We materialize it as an empty string so tJavaRow_1's
    # SKU regex logic runs safely (no match -> empty sku_year/sku_seq/sku_rev),
    # matching the Java block's own non-match fallback behavior.
    df_normalized = df_normalized.withColumn("sku_raw", F.lit(""))

    df_javarow_out = df_normalized.mapInPandas(
        _java_row_partition_fn, schema=JAVAROW_OUTPUT_SCHEMA
    )

    logger.info("Writing normalized output to %s", OUTPUT_NORMALIZED)
    (
        df_javarow_out.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(OUTPUT_NORMALIZED)
    )

    # -----------------------------------------------------------------
    # Branch 2: tDenormalize_1 -> tFileOutputDelimited_2
    # Inverse of tNormalize: group by product_id, concatenate 'tag' with '; '.
    # Since tDenormalize_1 is fed directly from tReplicate_1 (not from the
    # already-exploded tNormalize_1 branch), we re-split 'tags' into an array
    # and join with the target delimiter to rebuild tags_combined per the
    # spec's own description (inverse of tNormalize_1 on the same source column).
    # -----------------------------------------------------------------
    df_denormalized = (
        df_replicated
        .withColumn("tag_array", F.split(F.col("tags"), r"\|"))
        .withColumn("tags_combined", F.concat_ws("; ", F.col("tag_array")))
        .select("product_id", "tags_combined")
    )

    logger.info("Writing denormalized output to %s", OUTPUT_DENORMALIZED)
    (
        df_denormalized.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(OUTPUT_DENORMALIZED)
    )

    logger.info("Job completed successfully.")


if __name__ == "__main__":
    main()
