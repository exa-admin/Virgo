"""Step 3 — standardize and exclude.

Derives the ``c_*`` match attributes the rules compare on, and works out which records are
held out of new matching. Exclusions stop *new* matches only; they never undo an
Informatica grouping.
"""
from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from matching import read
from matching.helpers import clean_text, compact_key, sql_or, standardize_address

def standardize_input(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """Derive the ``c_*`` match and blocking attributes from the source columns."""
    std = cfg["standardization"]
    if "MDMRowId" not in df.columns:
        raise ValueError("MDMRowId column is missing. Merge source rows into mdm_row_registry before standardization.")

    zip_digits = F.regexp_replace(F.coalesce(F.col(std["zip_column"]).cast("string"), F.lit("")), r"[^0-9]", "")
    state = clean_text(F.col(std["state_column"])) if std.get("state_column") else F.lit("").cast("string")

    return (
        df.withColumn("MDMRowId", F.col("MDMRowId").cast("long"))
        .withColumn("record_id", F.col("MDMRowId"))
        .withColumn("c_name", clean_text(F.col(std["name_column"])))
        .withColumn("c_name_exact", compact_key(F.col(std["name_column"])))
        .withColumn("c_city", clean_text(F.col(std["city_column"])))
        .withColumn("c_state", state)
        .withColumn("c_address", standardize_address(F.concat_ws(" ", *[F.col(c) for c in std.get("address_columns", [])])))
        .withColumn("c_zip", zip_digits)
        .withColumn("c_zip_exact", zip_digits)
        .withColumn("c_name_prefix4", F.substring(F.col("c_name"), 1, 4))
        # Spark's soundex returns the input unchanged when the first character is not an
        # A-Z letter, so a non-Latin name became its own soundex key and could never share
        # a block with anything. Gate it to ASCII and let everything else fall through to
        # the prefix, which does group. Matters for MY, where Chinese names are common.
        .withColumn(
            "_soundex",
            F.when(
                (F.length(F.col("c_name")) > 0) & F.col("c_name").rlike(r"^[a-z0-9 ]+$"),
                F.soundex(F.col("c_name")),
            ),
        )
        .withColumn(
            "c_name_soundex",
            F.when(F.col("_soundex").isNull() | (F.length(F.col("_soundex")) == 0), F.col("c_name_prefix4")).otherwise(
                F.col("_soundex")
            ),
        )
        .drop("_soundex")
    )


def excluded_record_ids(processed: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """Records held out of new matching: config filters plus the mdm_match_exclusions table."""
    spark = processed.sparkSession
    key_column = cfg["rowRegistryKeyColumn"]

    from_config = (
        processed.filter(sql_or(cfg.get("exclude_from_match_filters")) or "false")
        .select("record_id")
        .dropDuplicates(["record_id"])
    )

    exclusions = (
        read.read(spark, cfg, "match_exclusions")
        .select(
            F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))).alias("CountryCode"),
            F.trim(F.coalesce(F.col("OperatorConcatId").cast("string"), F.lit(""))).alias(key_column),
        )
        .filter((F.col("CountryCode") != "") & (F.col(key_column) != ""))
        .dropDuplicates(["CountryCode", key_column])
    )
    from_table = (
        processed.select("record_id", "CountryCode", key_column)
        .join(exclusions, ["CountryCode", key_column], "inner")
        .select("record_id")
        .dropDuplicates(["record_id"])
    )

    return (
        from_config.unionByName(from_table)
        .dropDuplicates(["record_id"])
        .withColumn("is_excluded_from_match", F.lit(True))
    )
