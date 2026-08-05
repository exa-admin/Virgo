"""Native connected components via iterative min-label propagation (no GraphFrames)."""
from __future__ import annotations

from typing import Any, Dict

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mdm.delta_io import _materialize_component_labels

def connected_components_native(
    country_code: str,
    matched_record_ids_df: DataFrame,
    match_links_df: DataFrame,
    cfg: Dict[str, Any],
    max_iterations: int = 30,
) -> DataFrame:
    labels = _materialize_component_labels(
        matched_record_ids_df.select(F.col("record_id"), F.col("record_id").alias("golden_id")).dropDuplicates(["record_id"]),
        cfg["componentLabelsTable"],
        country_code,
        "labels_initial",
        0,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    bidirectional_links = (
        match_links_df.select(F.col("src").alias("record_id"), F.col("dst").alias("neighbor_id"))
        .unionByName(match_links_df.select(F.col("dst").alias("record_id"), F.col("src").alias("neighbor_id")))
        .dropDuplicates(["record_id", "neighbor_id"])
    ).persist(StorageLevel.MEMORY_AND_DISK)

    if bidirectional_links.limit(1).count() == 0:
        bidirectional_links.unpersist()
        return labels

    for iteration in range(1, max_iterations + 1):
        propagated = (
            bidirectional_links.join(labels.select(F.col("record_id").alias("neighbor_id"), "golden_id"), "neighbor_id", "inner")
            .select("record_id", "golden_id")
        )
        next_labels = _materialize_component_labels(
            labels.select("record_id", "golden_id")
            .unionByName(propagated)
            .groupBy("record_id")
            .agg(F.min("golden_id").alias("golden_id")),
            cfg["componentLabelsTable"],
            country_code,
            f"labels_iter_{iteration:03d}",
            iteration,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        changed = (
            labels.alias("old")
            .join(next_labels.alias("new"), "record_id", "inner")
            .filter(F.col("old.golden_id") != F.col("new.golden_id"))
            .limit(1)
            .count()
        )
        labels.unpersist()
        labels = next_labels

        print(f"  -> Connected component iteration {iteration}, changed={changed}")
        if changed == 0:
            break

    bidirectional_links.unpersist()
    return labels
