"""Step 2 — the row registry.

MERGE the country's source keys into ``mdm_row_registry`` so every operator gets a stable
``MDMRowId``, then attach that id and the golden-id context the later steps need.

Informatica id policy: ``SourceGoldenRecordId`` is written only when the source carries a
value. A known id is never downgraded to NULL — Informatica stops populating it once a
country migrates — but it may change to another non-null id if Informatica re-merged while
it still owned the country.
"""
from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from matching import read
from matching.golden_ids import (
    PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN,
    PREVIOUS_GOLDEN_ID_COLUMN,
    PREVIOUS_GOLDEN_ID_SOURCE_COLUMN,
)
from matching.helpers import is_empty, require_dataframe_columns, valid_value

ROW_REGISTRY_COLUMNS = [
    "CountryCode",
    "MDMRowId",
    "OperatorConcatId",
    "SourceGoldenRecordId",
    "GoldenIDCreatedDate",
    "MDMGoldenId",
    "MDMGoldenIdSource",
    "MDMGoldenIdAssignedDate",
    "DateCreated",
    "DateUpdated",
]

_NUMERIC_ID = r"^[0-9]+$"


def _validate_source_golden_ids(source: DataFrame) -> None:
    """Reject a non-blank GoldenRecordId that is not a plain integer string.

    Informatica sends the id as a numeric STRING. A silent cast would turn a malformed
    value into NULL, and the record would lose its Informatica grouping (and could be
    minted a brand-new engine id).
    """
    trimmed = F.trim(F.col("GoldenRecordId").cast("string"))
    invalid = source.filter(trimmed.isNotNull() & (trimmed != F.lit("")) & ~trimmed.rlike(_NUMERIC_ID))
    if not is_empty(invalid):
        sample = [r["GoldenRecordId"] for r in invalid.select("GoldenRecordId").limit(5).collect()]
        raise ValueError(
            f"Source GoldenRecordId contains non-numeric values (sample: {sample}). Informatica golden ids must be "
            "integer strings; fix the source or clean the values before running the match."
        )


def attach_row_registry(source: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """MERGE the source keys into mdm_row_registry, then attach MDMRowId and golden id context.

    Informatica id policy: ``SourceGoldenRecordId`` is only written when the source carries
    a value. A known id is never downgraded to NULL (Informatica stops populating it once a
    country migrates to this engine), but it may change to another non-null id (Informatica
    re-merged while it still owns the country).
    """
    spark = source.sparkSession
    registry_table = cfg["rowRegistryTable"]
    key_column = cfg["rowRegistryKeyColumn"]

    require_dataframe_columns(source, [key_column, "CountryCode", "GoldenRecordId", "BDLLoadTimestamp"], "Source DataFrame")
    _validate_source_golden_ids(source)

    # Deterministic one row per key: prefer a row carrying an Informatica id, then the
    # earliest load timestamp (dropDuplicates would pick arbitrarily).
    key_order = Window.partitionBy("CountryCode", "OperatorConcatId").orderBy(
        F.col("SourceGoldenRecordId").asc_nulls_last(),
        F.col("GoldenIDCreatedDate").asc_nulls_last(),
    )
    trimmed_id = F.trim(F.col("GoldenRecordId").cast("string"))
    merge_source = (
        source.select(
            F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))).alias("CountryCode"),
            F.trim(F.coalesce(F.col(key_column).cast("string"), F.lit(""))).alias("OperatorConcatId"),
            F.when(trimmed_id.rlike(_NUMERIC_ID), trimmed_id.cast("long")).alias("SourceGoldenRecordId"),
            F.col("BDLLoadTimestamp").cast("timestamp").alias("GoldenIDCreatedDate"),
        )
        .filter(F.col("CountryCode") != "")
        .filter(valid_value(F.col("OperatorConcatId"), cfg.get("invalid_values", []), 1))
        .withColumn("_rank", F.row_number().over(key_order))
        .filter(F.col("_rank") == F.lit(1))
        .drop("_rank")
    )

    view = "tmp_mdm_row_registry_source"
    merge_source.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {registry_table} AS target
        USING {view} AS source
        ON target.CountryCode = source.CountryCode
           AND target.OperatorConcatId = source.OperatorConcatId
        WHEN MATCHED AND source.SourceGoldenRecordId IS NOT NULL
             AND (target.SourceGoldenRecordId IS NULL
                  OR target.SourceGoldenRecordId <> source.SourceGoldenRecordId) THEN UPDATE SET
          target.SourceGoldenRecordId = source.SourceGoldenRecordId,
          target.GoldenIDCreatedDate = source.GoldenIDCreatedDate,
          target.DateUpdated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          CountryCode, OperatorConcatId, SourceGoldenRecordId, GoldenIDCreatedDate, DateCreated, DateUpdated
        )
        VALUES (
          source.CountryCode, source.OperatorConcatId, source.SourceGoldenRecordId, source.GoldenIDCreatedDate,
          current_timestamp(), current_timestamp()
        )
        """
    )

    # Registry values win over the raw source, so SourceGoldenRecordId keeps the last known
    # Informatica id even when the source has since gone NULL for a migrated country.
    registry = read.read(spark, cfg, "row_registry").select(
        "CountryCode",
        F.col("OperatorConcatId").alias(key_column),
        "MDMRowId",
        "SourceGoldenRecordId",
        "GoldenIDCreatedDate",
        F.col("MDMGoldenId").alias(PREVIOUS_GOLDEN_ID_COLUMN),
        F.col("MDMGoldenIdSource").alias(PREVIOUS_GOLDEN_ID_SOURCE_COLUMN),
        F.col("MDMGoldenIdAssignedDate").alias(PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN),
    )
    # MDMRowId presence is checked by the caller, after standardization (avoids a second
    # full scan of the source here).
    return (
        source.withColumn("CountryCode", F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))))
        .withColumn(key_column, F.trim(F.coalesce(F.col(key_column).cast("string"), F.lit(""))))
        .join(registry, ["CountryCode", key_column], "left")
    )
