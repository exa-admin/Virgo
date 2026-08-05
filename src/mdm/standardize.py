"""Input standardization for match keys and blocking attributes."""
from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mdm.utils import _clean_text, _compact_key, _standardize_address

def standardize_input(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    std = cfg["standardization"]
    if "MDMRowId" not in df.columns:
        raise ValueError("MDMRowId column is missing. Merge source rows into MDMRowRegistry before standardization.")

    address_expr = F.concat_ws(" ", *[F.col(c) for c in std.get("address_columns", [])])
    name = _clean_text(F.col(std["name_column"]))
    state_column = std.get("state_column")
    state_expr = _clean_text(F.col(state_column)) if state_column else F.lit("").cast("string")

    result = (
        df.withColumn("MDMRowId", F.col("MDMRowId").cast("long"))
        .withColumn("record_id", F.col("MDMRowId"))
        .withColumn("c_name", name)
        .withColumn("c_name_exact", _compact_key(F.col(std["name_column"])))
        .withColumn("c_city", _clean_text(F.col(std["city_column"])))
        .withColumn("c_state", state_expr)
        .withColumn("c_address", _standardize_address(address_expr))
        .withColumn("c_zip", F.regexp_replace(F.coalesce(F.col(std["zip_column"]).cast("string"), F.lit("")), r"[^0-9]", ""))
        .withColumn("c_zip_exact", F.regexp_replace(F.coalesce(F.col(std["zip_column"]).cast("string"), F.lit("")), r"[^0-9]", ""))
        .withColumn("c_name_prefix4", F.substring(F.col("c_name"), 1, 4))
        .withColumn("c_name_soundex_raw", F.soundex(F.col("c_name")))
        .withColumn(
            "c_name_soundex",
            F.when(
                F.col("c_name_soundex_raw").isNull() | (F.length(F.col("c_name_soundex_raw")) == 0),
                F.col("c_name_prefix4"),
            ).otherwise(F.col("c_name_soundex_raw")),
        )
        .drop("c_name_soundex_raw")
    )
    return result
