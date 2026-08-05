"""Final golden ID assignment (reuse SourceGoldenRecordId or synthetic offset)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from mdm.config import SYNTHETIC_GOLDEN_ID_OFFSET

def collect_record_match_rules(match_links_df: DataFrame) -> DataFrame:
    edge_rules = (
        match_links_df.filter(F.col("src_is_rule_subject")).select(F.col("src").alias("record_id"), "match_rule")
        .unionByName(match_links_df.filter(F.col("dst_is_rule_subject")).select(F.col("dst").alias("record_id"), "match_rule"))
        .dropDuplicates(["record_id", "match_rule"])
    )
    return edge_rules.groupBy("record_id").agg(F.concat_ws(", ", F.sort_array(F.collect_set("match_rule"))).alias("final_match_rule"))

def _build_final_golden_ids(processed_df: DataFrame, cluster_labels_df: DataFrame) -> DataFrame:
    cluster_rows = (
        processed_df.select(
            "CountryCode",
            "record_id",
            "MDMRowId",
            "OperatorConcatId",
            "SourceGoldenRecordId",
            "GoldenIDCreatedDate",
        )
        .join(
            cluster_labels_df.select("record_id", F.col("golden_id").alias("TempClusterId")),
            "record_id",
            "left",
        )
        .withColumn("TempClusterId", F.coalesce(F.col("TempClusterId"), F.col("record_id")).cast("long"))
    )

    source_golden_window = Window.partitionBy(
        "CountryCode", "TempClusterId"
    ).orderBy(
        F.col("GoldenIDCreatedDate").asc_nulls_last(),
        F.col("SourceGoldenRecordId").asc(),
    )

    source_golden_choices = (
        cluster_rows.filter(F.col("SourceGoldenRecordId").isNotNull())
        .withColumn("_source_golden_rank", F.row_number().over(source_golden_window))
        .filter(F.col("_source_golden_rank") == F.lit(1))
        .select(
            "CountryCode",
            "TempClusterId",
            F.col("SourceGoldenRecordId").cast("long").alias("FinalGoldenId"),
        )
    )

    cluster_golden_ids = (
        cluster_rows.select("CountryCode", "TempClusterId")
        .dropDuplicates(["CountryCode", "TempClusterId"])
        .join(source_golden_choices, ["CountryCode", "TempClusterId"], "left")
        .withColumn(
            "FinalGoldenId",
            F.coalesce(
                F.col("FinalGoldenId"),
                (F.lit(SYNTHETIC_GOLDEN_ID_OFFSET) + F.col("TempClusterId")).cast("long"),
            ),
        )
    )

    return (
        cluster_rows.join(cluster_golden_ids, ["CountryCode", "TempClusterId"], "left")
        .select(
            "record_id",
            "TempClusterId",
            F.col("FinalGoldenId").cast("long").alias("golden_id"),
        )
    )
