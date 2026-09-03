"""Native connected components via iterative min-label propagation (no GraphFrames)."""
from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from matching.config import DEFAULT_COMPONENTS_MAX_ITERATIONS
from matching.delta_io import _materialize_component_labels
from matching.utils import _is_empty

def connected_components_native(
    country_code: str,
    matched_record_ids_df: DataFrame,
    match_links_df: DataFrame,
    cfg: Dict[str, Any],
    max_iterations: Optional[int] = None,
) -> DataFrame:
    """Min-label propagation. Labels only ever decrease, so "no label changed" is a sound
    convergence test. Raises if the graph has not converged within ``max_iterations``
    (cfg ``components_max_iterations``), because partially propagated labels would split
    real components and mint wrong golden ids.
    """
    if max_iterations is None:
        max_iterations = int(cfg.get("components_max_iterations", DEFAULT_COMPONENTS_MAX_ITERATIONS))
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

    if _is_empty(bidirectional_links):
        bidirectional_links.unpersist()
        return labels

    converged = False
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

        changed = not _is_empty(
            labels.alias("old")
            .join(next_labels.alias("new"), "record_id", "inner")
            .filter(F.col("old.golden_id") != F.col("new.golden_id"))
        )
        labels.unpersist()
        labels = next_labels

        print(f"  -> Connected component iteration {iteration}, changed={changed}")
        if not changed:
            converged = True
            break

    bidirectional_links.unpersist()
    if not converged:
        labels.unpersist()
        raise RuntimeError(
            f"Connected components for {country_code} did not converge within {max_iterations} iterations "
            "(a match chain is longer than the iteration budget). Increase 'components_max_iterations' in the "
            "country config; do not use the partial labels."
        )
    return labels
