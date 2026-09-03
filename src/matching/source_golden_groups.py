"""Informatica golden groupings as hard match links.

Policy (``preserve_source_golden_groups``): every set of records that Informatica already
grouped under one ``GoldenRecordId`` (``SourceGoldenRecordId`` in the registry) is joined
by star edges *before* connected components. The engine therefore can never split an
Informatica group and, because tier-1 selection in ``golden_ids.py`` prefers the
Informatica id, the group always keeps its id. New records that match a member inherit
that id; records that form a brand-new cluster are minted an engine id.

The edges are built from the full processed population (including records excluded from
new matching by config filters / ``MDMMatchExclusions``): exclusions only stop *new*
matching, they do not undo what Informatica already decided.

Cross-group merges (``allow_source_golden_group_merge``): engine rules may bridge two
Informatica groups. When the flag is False the bridging edges are removed so that no
Informatica id changes at all:

1. **Direct** bridges (an edge whose endpoints carry two different non-null ids) are
   filtered before components.
2. **Transitive** bridges (a new record links to members of two groups, or a chain of
   new records does) are only visible after components. Components that contain more
   than one Informatica id are re-labelled with a seeded min-label propagation: records
   with an Informatica id keep their own id as label, other records take the smallest
   label reachable through neighbours without an id. Edges whose endpoints end up with
   different labels are dropped; each label then forms exactly one connected component.

All dropped edges are written to ``MDMRuleResults`` (stage
``999_Blocked_Source_GoldenRecordId_Merge``, ``blocked_source_group_merge = true``) with
the Informatica group each endpoint resolved to, for stewardship review.

Everything is Spark-native (no UDFs, no GraphFrames).
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from matching.config import (
    BLOCKED_SOURCE_GROUP_MERGE_RULE_STAGE,
    DEFAULT_COMPONENTS_MAX_ITERATIONS,
    SOURCE_GOLDEN_GROUP_BLOCK_NAME,
    SOURCE_GOLDEN_GROUP_EDGE_TYPE,
    SOURCE_GOLDEN_GROUP_RULE_NAME,
    SOURCE_GOLDEN_GROUP_RULE_PRIORITY,
)
from matching.delta_io import _materialize_component_labels, _overwrite_delta_slice
from matching.utils import _is_empty, _sql_literal

# Columns persisted to MDMMatchLinks (the fuzzy evidence columns are dropped there).
MATCH_LINK_BASE_COLUMNS = [
    "src",
    "dst",
    "match_rule",
    "rule_priority",
    "edge_type",
    "block_name",
    "match_key",
    "src_is_rule_subject",
    "dst_is_rule_subject",
]
# Full in-flight match link schema (see utils._empty_match_links).
MATCH_LINK_COLUMNS = MATCH_LINK_BASE_COLUMNS + [
    "NameLevenshteinSimilarity",
    "AddressLevenshteinSimilarity",
    "AddressTokenJaccardSimilarity",
    "AddressBestSimilarity",
    "ZipExactMatch",
]

# Columns of a "blocked link" DataFrame: the base match link plus the Informatica group each
# endpoint belongs to (own id for direct bridges, resolved label for transitive ones).
BLOCKED_LINK_EXTRA_COLUMNS = ["src_source_golden_id", "dst_source_golden_id"]
BLOCKED_LINK_COLUMNS = MATCH_LINK_BASE_COLUMNS + BLOCKED_LINK_EXTRA_COLUMNS


def source_golden_group_blocking_enabled(cfg: Dict[str, Any]) -> bool:
    """Cross-group merge blocking only makes sense when groups are preserved."""
    return bool(cfg.get("preserve_source_golden_groups", True)) and not bool(
        cfg.get("allow_source_golden_group_merge", False)
    )


def _source_golden_ids(processed_df: DataFrame) -> DataFrame:
    """(record_id, SourceGoldenRecordId) for records that carry an Informatica id."""
    return (
        processed_df.select(
            F.col("record_id").cast("long").alias("record_id"),
            F.col("SourceGoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
        )
        .filter(F.col("SourceGoldenRecordId").isNotNull())
        .dropDuplicates(["record_id"])
    )


def build_source_golden_group_links(processed_df: DataFrame) -> DataFrame:
    """Star edges (anchor = min record_id) for every Informatica id shared by >= 2 records.

    Same shape as engine match links. No block-size cap: Informatica groupings are
    authoritative, however large.
    """
    ids = _source_golden_ids(processed_df)
    groups = (
        ids.groupBy("SourceGoldenRecordId")
        .agg(F.min("record_id").alias("anchor_id"), F.count(F.lit(1)).alias("group_size"))
        .filter(F.col("group_size") >= F.lit(2))
        .select("SourceGoldenRecordId", "anchor_id")
    )
    return (
        ids.join(groups, "SourceGoldenRecordId", "inner")
        .filter(F.col("record_id") != F.col("anchor_id"))
        .select(
            F.col("anchor_id").cast("long").alias("src"),
            F.col("record_id").cast("long").alias("dst"),
            F.lit(SOURCE_GOLDEN_GROUP_RULE_NAME).alias("match_rule"),
            F.lit(int(SOURCE_GOLDEN_GROUP_RULE_PRIORITY)).alias("rule_priority"),
            F.lit(SOURCE_GOLDEN_GROUP_EDGE_TYPE).alias("edge_type"),
            F.lit(SOURCE_GOLDEN_GROUP_BLOCK_NAME).alias("block_name"),
            F.col("SourceGoldenRecordId").cast("string").alias("match_key"),
            F.lit(True).alias("src_is_rule_subject"),
            F.lit(True).alias("dst_is_rule_subject"),
            F.lit(None).cast("double").alias("NameLevenshteinSimilarity"),
            F.lit(None).cast("double").alias("AddressLevenshteinSimilarity"),
            F.lit(None).cast("double").alias("AddressTokenJaccardSimilarity"),
            F.lit(None).cast("double").alias("AddressBestSimilarity"),
            F.lit(None).cast("boolean").alias("ZipExactMatch"),
        )
        .dropDuplicates(["src", "dst"])
    )


def split_direct_source_group_bridges(match_links_df: DataFrame, processed_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Return (kept_links, blocked_links).

    A link is blocked when both endpoints carry a non-null Informatica id and the ids
    differ. Source golden group links are never blocked (both endpoints share the id).
    """
    ids = _source_golden_ids(processed_df)
    src_ids = ids.select(F.col("record_id").alias("src"), F.col("SourceGoldenRecordId").alias("src_source_golden_id"))
    dst_ids = ids.select(F.col("record_id").alias("dst"), F.col("SourceGoldenRecordId").alias("dst_source_golden_id"))
    joined = match_links_df.join(src_ids, "src", "left").join(dst_ids, "dst", "left")
    bridge = (
        F.col("src_source_golden_id").isNotNull()
        & F.col("dst_source_golden_id").isNotNull()
        & (F.col("src_source_golden_id") != F.col("dst_source_golden_id"))
    )
    kept = joined.filter(~F.coalesce(bridge, F.lit(False))).select(*MATCH_LINK_COLUMNS)
    blocked = joined.filter(bridge).select(*BLOCKED_LINK_COLUMNS)
    return kept, blocked


def resolve_transitive_source_group_bridges(
    country_code: str,
    components_df: DataFrame,
    match_links_df: DataFrame,
    processed_df: DataFrame,
    cfg: Dict[str, Any],
) -> Optional[Tuple[DataFrame, DataFrame]]:
    """Detect components holding >1 Informatica id and split them along group lines.

    Returns ``None`` when no component is conflicted. Otherwise returns
    ``(blocked_links, resolved_components)`` where ``blocked_links`` are the engine edges
    to remove (``BLOCKED_LINK_COLUMNS``; ``match_links_df`` may carry just the base link
    columns as persisted in MDMMatchLinks) and ``resolved_components`` is the
    full component label set (``record_id``, ``golden_id`` = component label) with the
    conflicted components re-labelled so every component carries at most one Informatica id.
    """
    max_iterations = int(cfg.get("components_max_iterations", DEFAULT_COMPONENTS_MAX_ITERATIONS))
    ids = _source_golden_ids(processed_df)
    components = components_df.select(
        F.col("record_id").cast("long").alias("record_id"),
        F.col("golden_id").cast("long").alias("component_id"),
    )

    conflicted_components = (
        components.join(ids, "record_id", "inner")
        .groupBy("component_id")
        .agg(F.countDistinct("SourceGoldenRecordId").alias("source_id_count"))
        .filter(F.col("source_id_count") > F.lit(1))
        .select("component_id")
    )
    conflicted_records = components.join(conflicted_components, "component_id", "inner").persist(StorageLevel.MEMORY_AND_DISK)
    conflicted_record_count = conflicted_records.count()
    if conflicted_record_count == 0:
        conflicted_records.unpersist()
        return None
    print(
        f"  -> {conflicted_record_count} records sit in components that bridge several Informatica groups; "
        "resolving along Source_GoldenRecordId lines"
    )

    # Edges inside conflicted components (src and dst share a component, so joining on src suffices).
    sub_links = match_links_df.join(conflicted_records.select(F.col("record_id").alias("src")), "src", "inner").persist(
        StorageLevel.MEMORY_AND_DISK
    )
    bidirectional = (
        sub_links.select(F.col("src").alias("record_id"), F.col("dst").alias("neighbor_id"))
        .unionByName(sub_links.select(F.col("dst").alias("record_id"), F.col("src").alias("neighbor_id")))
        .dropDuplicates(["record_id", "neighbor_id"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # Seeded min-label propagation: seeds are fixed, other labels only ever decrease.
    seeds = conflicted_records.select("record_id").join(ids, "record_id", "left").select(
        "record_id", F.col("SourceGoldenRecordId").alias("seed")
    )
    labels = _materialize_component_labels(
        seeds.select("record_id", F.col("seed").alias("golden_id")),
        cfg["componentLabelsTable"],
        country_code,
        "source_group_labels_initial",
        0,
    ).persist(StorageLevel.MEMORY_AND_DISK)

    converged = False
    for iteration in range(1, max_iterations + 1):
        neighbor_min = (
            bidirectional.join(
                labels.filter(F.col("golden_id").isNotNull()).select(F.col("record_id").alias("neighbor_id"), F.col("golden_id").alias("neighbor_label")),
                "neighbor_id",
                "inner",
            )
            .groupBy("record_id")
            .agg(F.min("neighbor_label").alias("neighbor_label"))
        )
        next_labels = _materialize_component_labels(
            labels.join(seeds, "record_id", "left")
            .join(neighbor_min, "record_id", "left")
            .select(
                "record_id",
                F.coalesce(F.col("seed"), F.least(F.col("golden_id"), F.col("neighbor_label"))).alias("golden_id"),
            ),
            cfg["componentLabelsTable"],
            country_code,
            f"source_group_labels_iter_{iteration:03d}",
            iteration,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        changed = not _is_empty(
            labels.alias("old")
            .join(next_labels.alias("new"), "record_id", "inner")
            .filter(~F.col("old.golden_id").eqNullSafe(F.col("new.golden_id")))
        )
        labels.unpersist()
        labels = next_labels
        print(f"  -> Source group label iteration {iteration}, changed={changed}")
        if not changed:
            converged = True
            break

    if not converged:
        raise RuntimeError(
            f"Source golden group resolution for {country_code} did not converge within {max_iterations} iterations. "
            "Increase 'components_max_iterations' in the country config; do not use the partial labels."
        )
    if not _is_empty(labels.filter(F.col("golden_id").isNull())):
        raise RuntimeError(
            f"Source golden group resolution for {country_code} left records without a group label; "
            "this should be impossible inside a connected component and indicates inconsistent match links."
        )

    src_labels = labels.select(F.col("record_id").alias("src"), F.col("golden_id").alias("src_source_golden_id"))
    dst_labels = labels.select(F.col("record_id").alias("dst"), F.col("golden_id").alias("dst_source_golden_id"))
    blocked = (
        sub_links.join(src_labels, "src", "inner")
        .join(dst_labels, "dst", "inner")
        .filter(F.col("src_source_golden_id") != F.col("dst_source_golden_id"))
        .select(*BLOCKED_LINK_COLUMNS)
    )

    # Each (component, label) group is connected by construction; its new label is its min record_id.
    resolved = (
        conflicted_records.join(labels.select("record_id", F.col("golden_id").alias("group_label")), "record_id", "inner")
        .withColumn("new_component_id", F.min("record_id").over(Window.partitionBy("component_id", "group_label")))
        .select("record_id", F.col("new_component_id").alias("golden_id"))
    )
    untouched = components.join(conflicted_components, "component_id", "left_anti").select(
        "record_id", F.col("component_id").alias("golden_id")
    )
    resolved_components = _materialize_component_labels(
        untouched.unionByName(resolved),
        cfg["componentLabelsTable"],
        country_code,
        "labels_source_groups_resolved",
        9999,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    resolved_components.count()
    blocked = blocked.persist(StorageLevel.MEMORY_AND_DISK)
    blocked.count()

    labels.unpersist()
    bidirectional.unpersist()
    sub_links.unpersist()
    conflicted_records.unpersist()
    return blocked, resolved_components


def remove_links(match_links_df: DataFrame, blocked_links_df: DataFrame) -> DataFrame:
    """Anti-join blocked edges out of the link set (by endpoints + rule)."""
    return match_links_df.join(
        blocked_links_df.select("src", "dst", "match_rule", "edge_type"),
        ["src", "dst", "match_rule", "edge_type"],
        "left_anti",
    )


def materialize_blocked_source_group_links(
    blocked_links_df: DataFrame,
    reference_df: DataFrame,
    cfg: Dict[str, Any],
    country_code: str,
) -> int:
    """Write blocked bridging edges to MDMRuleResults for stewardship; returns row count.

    Rows carry ``RuleType = 'blocked'``, ``RuleStageName = 999_Blocked_Source_GoldenRecordId_Merge``,
    ``blocked_source_group_merge = true`` and the Informatica group of each endpoint. The
    original ``match_rule`` / ``match_key`` are kept so stewards can see which rule wanted
    to bridge the groups. Columns beyond the base DDL are added via mergeSchema.
    """
    row_registry_key_column = cfg["rowRegistryKeyColumn"]
    left_context = reference_df.select(
        F.col("record_id").alias("src"),
        F.col(row_registry_key_column).cast("string").alias("SrcOperatorConcatId"),
    ).dropDuplicates(["src"])
    right_context = reference_df.select(
        F.col("record_id").alias("dst"),
        F.col(row_registry_key_column).cast("string").alias("DstOperatorConcatId"),
    ).dropDuplicates(["dst"])
    slice_df = (
        blocked_links_df.join(left_context, "src", "left")
        .join(right_context, "dst", "left")
        .select(
            F.lit(country_code).alias("CountryCode"),
            F.lit("blocked").alias("RuleType"),
            F.lit(BLOCKED_SOURCE_GROUP_MERGE_RULE_STAGE).alias("RuleStageName"),
            F.lit(999).alias("RuleExecutionOrder"),
            F.col("src").cast("long").alias("src"),
            F.col("dst").cast("long").alias("dst"),
            "SrcOperatorConcatId",
            "DstOperatorConcatId",
            "match_rule",
            F.col("rule_priority").cast("int").alias("rule_priority"),
            "edge_type",
            "block_name",
            "match_key",
            "src_is_rule_subject",
            "dst_is_rule_subject",
            F.lit(True).alias("blocked_source_group_merge"),
            F.col("src_source_golden_id").cast("long").alias("src_source_golden_id"),
            F.col("dst_source_golden_id").cast("long").alias("dst_source_golden_id"),
        )
    )
    written = _overwrite_delta_slice(
        slice_df,
        cfg["ruleResultsTable"],
        f"CountryCode = '{_sql_literal(country_code)}' AND RuleStageName = '{_sql_literal(BLOCKED_SOURCE_GROUP_MERGE_RULE_STAGE)}'",
        merge_schema=True,
    )
    return written.count()
