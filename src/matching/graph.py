"""Connected components over the match graph, and Informatica group preservation.

Both halves are min-label propagation, Spark-native (no UDFs, no GraphFrames), with each
iteration checkpointed so the query plan never grows unbounded — locally by default, to
mdm_component_labels when ``component_label_detail`` is "all".

**Components** — every record starts labelled with its own id and repeatedly takes the
smallest label among its neighbours. Labels only ever decrease, so "no label changed" is
a sound convergence test.

**Informatica groups** (``preserve_source_golden_groups``) — records sharing a
``SourceGoldenRecordId`` are star-linked *before* components run, so the engine can never
split a group and the group always keeps its id. When
``allow_source_golden_group_merge`` is false, engine edges that would fuse two Informatica
groups are removed instead: direct bridges (both endpoints carry different ids) before
components, transitive ones (a new record linking two groups) after, by re-labelling the
conflicted component from the Informatica ids as fixed seeds. All dropped edges land in
mdm_rule_results stage ``999_Blocked_Source_GoldenRecordId_Merge`` for stewardship.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from matching import write
from matching.config import (
    BLOCKED_SOURCE_GROUP_MERGE_RULE_STAGE,
    SOURCE_GOLDEN_GROUP_BLOCK_NAME,
    SOURCE_GOLDEN_GROUP_EDGE_TYPE,
    SOURCE_GOLDEN_GROUP_RULE_NAME,
    SOURCE_GOLDEN_GROUP_RULE_PRIORITY,
)
from matching.helpers import (
    MATCH_LINK_BASE_COLUMNS,
    MATCH_LINK_COLUMNS,
    is_empty,
    no_evidence,
    sql_literal,
)

# A blocked link plus the Informatica group each endpoint resolved to.
BLOCKED_LINK_COLUMNS = MATCH_LINK_BASE_COLUMNS + ["src_source_golden_id", "dst_source_golden_id"]


def _bidirectional(links: DataFrame) -> DataFrame:
    """Both directions of every edge, so a record can see all of its neighbours."""
    return (
        links.select(F.col("src").alias("record_id"), F.col("dst").alias("neighbor_id"))
        .unionByName(links.select(F.col("dst").alias("record_id"), F.col("src").alias("neighbor_id")))
        .dropDuplicates(["record_id", "neighbor_id"])
    )


def _stage_labels(
    labels: DataFrame,
    cfg: Dict[str, Any],
    country_code: str,
    stage_name: str,
    iteration: int,
    final: bool = False,
) -> DataFrame:
    """Materialize one generation of labels and truncate the query plan behind it.

    Truncation is the point: each iteration joins onto the previous one, so without it the
    logical plan grows without bound and the driver's optimizer time explodes. Writing a
    Delta slice does that, but it costs a full write + transaction + read-back of every
    record, every iteration, in every propagation loop — and nothing ever reads the
    intermediates. A local checkpoint truncates the same way with no table I/O, so only the
    final generation is persisted to mdm_component_labels.

    Set ``component_label_detail = "all"`` to write every iteration again when debugging a
    grouping; it is far slower on a large country.
    """
    if final or cfg.get("component_label_detail", "final") == "all":
        return write.save_component_labels(
            labels, cfg["componentLabelsTable"], country_code, stage_name, iteration
        ).persist(StorageLevel.MEMORY_AND_DISK)
    return labels.localCheckpoint(eager=True)


def connected_components(
    country_code: str,
    record_ids: DataFrame,
    match_links: DataFrame,
    cfg: Dict[str, Any],
    stage_prefix: str = "labels",
) -> DataFrame:
    """Label every record with its component id. Raises if propagation has not converged.

    Partially propagated labels would split real components and mint wrong golden ids, so
    running out of iterations is a failure, not a result. ``stage_prefix`` names the
    mdm_component_labels checkpoints, so two passes in one run do not overwrite each other.
    """
    max_iterations = int(cfg["components_max_iterations"])
    labels = _stage_labels(
        record_ids.select("record_id", F.col("record_id").alias("golden_id")).dropDuplicates(["record_id"]),
        cfg,
        country_code,
        f"{stage_prefix}_initial",
        0,
    )

    neighbours = _bidirectional(match_links).persist(StorageLevel.MEMORY_AND_DISK)
    if is_empty(neighbours):
        neighbours.unpersist()
        return _stage_labels(labels, cfg, country_code, f"{stage_prefix}_final", 0, final=True)

    for iteration in range(1, max_iterations + 1):
        propagated = neighbours.join(
            labels.select(F.col("record_id").alias("neighbor_id"), "golden_id"), "neighbor_id", "inner"
        ).select("record_id", "golden_id")

        next_labels = _stage_labels(
            labels.select("record_id", "golden_id")
            .unionByName(propagated)
            .groupBy("record_id")
            .agg(F.min("golden_id").alias("golden_id")),
            cfg,
            country_code,
            f"{stage_prefix}_iter_{iteration:03d}",
            iteration,
        )

        changed = not is_empty(
            labels.alias("old")
            .join(next_labels.alias("new"), "record_id", "inner")
            .filter(F.col("old.golden_id") != F.col("new.golden_id"))
        )
        labels.unpersist()
        labels = next_labels
        print(f"  -> {stage_prefix} iteration {iteration}, changed={changed}")
        if not changed:
            neighbours.unpersist()
            return _stage_labels(labels, cfg, country_code, f"{stage_prefix}_final", iteration, final=True)

    neighbours.unpersist()
    labels.unpersist()
    raise RuntimeError(
        f"Connected components for {country_code} did not converge within {max_iterations} iterations "
        "(a match chain is longer than the iteration budget). Increase 'components_max_iterations' in the "
        "country config; do not use the partial labels."
    )


# ----------------------------------------------------------------- Informatica groups


def blocking_enabled(cfg: Dict[str, Any]) -> bool:
    """Cross-group merge blocking only makes sense when groups are preserved."""
    return bool(cfg["preserve_source_golden_groups"]) and not bool(cfg["allow_source_golden_group_merge"])


def _group_ids(processed: DataFrame) -> DataFrame:
    """(record_id, SourceGoldenRecordId) for records that carry an Informatica id."""
    return (
        processed.select(
            F.col("record_id").cast("long").alias("record_id"),
            F.col("SourceGoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
        )
        .filter(F.col("SourceGoldenRecordId").isNotNull())
        .dropDuplicates(["record_id"])
    )


def build_group_links(processed: DataFrame) -> DataFrame:
    """Star edges (anchor = lowest record id) for every Informatica id shared by >= 2 records.

    Same shape as engine match links, but with no block-size cap: Informatica groupings are
    authoritative however large.
    """
    ids = _group_ids(processed)
    anchors = (
        ids.groupBy("SourceGoldenRecordId")
        .agg(F.min("record_id").alias("anchor_id"), F.count(F.lit(1)).alias("group_size"))
        .filter(F.col("group_size") >= F.lit(2))
        .select("SourceGoldenRecordId", "anchor_id")
    )
    return (
        ids.join(anchors, "SourceGoldenRecordId", "inner")
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
            *no_evidence(),
        )
        .dropDuplicates(["src", "dst"])
    )


def split_direct_bridges(match_links: DataFrame, processed: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split links into (kept, blocked); blocked = endpoints carry two different Informatica ids.

    Group links themselves are never blocked, since both endpoints share the id.
    """
    ids = _group_ids(processed)
    joined = (
        match_links.join(
            ids.select(F.col("record_id").alias("src"), F.col("SourceGoldenRecordId").alias("src_source_golden_id")),
            "src",
            "left",
        )
        .join(
            ids.select(F.col("record_id").alias("dst"), F.col("SourceGoldenRecordId").alias("dst_source_golden_id")),
            "dst",
            "left",
        )
    )
    bridge = (
        F.col("src_source_golden_id").isNotNull()
        & F.col("dst_source_golden_id").isNotNull()
        & (F.col("src_source_golden_id") != F.col("dst_source_golden_id"))
    )
    return (
        joined.filter(~F.coalesce(bridge, F.lit(False))).select(*MATCH_LINK_COLUMNS),
        joined.filter(bridge).select(*BLOCKED_LINK_COLUMNS),
    )


def resolve_transitive_bridges(
    country_code: str,
    components: DataFrame,
    match_links: DataFrame,
    processed: DataFrame,
    cfg: Dict[str, Any],
) -> Optional[Tuple[DataFrame, DataFrame]]:
    """Split components that ended up holding more than one Informatica id.

    Returns ``None`` when no component is conflicted, else ``(blocked_links, components)``:
    the engine edges to remove, and the full label set with conflicted components
    re-labelled so each carries at most one Informatica id.

    Labels are propagated with the Informatica ids as *fixed seeds*: a record that has an
    id keeps it, and every other record takes the smallest label reachable through its
    neighbours. Edges whose endpoints settle on different labels are the bridges.
    """
    max_iterations = int(cfg["components_max_iterations"])
    ids = _group_ids(processed)
    labelled = components.select(
        F.col("record_id").cast("long").alias("record_id"),
        F.col("golden_id").cast("long").alias("component_id"),
    )

    conflicted_components = (
        labelled.join(ids, "record_id", "inner")
        .groupBy("component_id")
        .agg(F.countDistinct("SourceGoldenRecordId").alias("source_id_count"))
        .filter(F.col("source_id_count") > F.lit(1))
        .select("component_id")
    )
    conflicted = labelled.join(conflicted_components, "component_id", "inner").persist(StorageLevel.MEMORY_AND_DISK)
    conflicted_count = conflicted.count()
    if conflicted_count == 0:
        conflicted.unpersist()
        return None
    print(
        f"  -> {conflicted_count} records sit in components that bridge several Informatica groups; "
        "resolving along Source_GoldenRecordId lines"
    )

    # src and dst always share a component, so joining on src is enough to select the subgraph.
    sub_links = match_links.join(conflicted.select(F.col("record_id").alias("src")), "src", "inner").persist(
        StorageLevel.MEMORY_AND_DISK
    )
    neighbours = _bidirectional(sub_links).persist(StorageLevel.MEMORY_AND_DISK)

    seeds = conflicted.select("record_id").join(ids, "record_id", "left").select(
        "record_id", F.col("SourceGoldenRecordId").alias("seed")
    )
    labels = _stage_labels(
        seeds.select("record_id", F.col("seed").alias("golden_id")),
        cfg,
        country_code,
        "source_group_labels_initial",
        0,
    )

    converged = False
    for iteration in range(1, max_iterations + 1):
        neighbour_min = (
            neighbours.join(
                labels.filter(F.col("golden_id").isNotNull()).select(
                    F.col("record_id").alias("neighbor_id"), F.col("golden_id").alias("neighbor_label")
                ),
                "neighbor_id",
                "inner",
            )
            .groupBy("record_id")
            .agg(F.min("neighbor_label").alias("neighbor_label"))
        )
        next_labels = _stage_labels(
            labels.join(seeds, "record_id", "left")
            .join(neighbour_min, "record_id", "left")
            .select("record_id", F.coalesce(F.col("seed"), F.least(F.col("golden_id"), F.col("neighbor_label"))).alias("golden_id")),
            cfg,
            country_code,
            f"source_group_labels_iter_{iteration:03d}",
            iteration,
        )

        changed = not is_empty(
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
    if not is_empty(labels.filter(F.col("golden_id").isNull())):
        raise RuntimeError(
            f"Source golden group resolution for {country_code} left records without a group label; "
            "this should be impossible inside a connected component and indicates inconsistent match links."
        )

    blocked = (
        sub_links.join(labels.select(F.col("record_id").alias("src"), F.col("golden_id").alias("src_source_golden_id")), "src", "inner")
        .join(labels.select(F.col("record_id").alias("dst"), F.col("golden_id").alias("dst_source_golden_id")), "dst", "inner")
        .filter(F.col("src_source_golden_id") != F.col("dst_source_golden_id"))
        .select(*BLOCKED_LINK_COLUMNS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    blocked.count()

    # Each (component, label) group is connected by construction, so its lowest record id
    # is a valid new component label.
    resolved = (
        conflicted.join(labels.select("record_id", F.col("golden_id").alias("group_label")), "record_id", "inner")
        .withColumn("new_component_id", F.min("record_id").over(Window.partitionBy("component_id", "group_label")))
        .select("record_id", F.col("new_component_id").alias("golden_id"))
    )
    untouched = labelled.join(conflicted_components, "component_id", "left_anti").select(
        "record_id", F.col("component_id").alias("golden_id")
    )
    resolved_components = write.save_component_labels(
        untouched.unionByName(resolved),
        cfg["componentLabelsTable"],
        country_code,
        "labels_source_groups_resolved",
        9999,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    resolved_components.count()

    labels.unpersist()
    neighbours.unpersist()
    sub_links.unpersist()
    conflicted.unpersist()
    return blocked, resolved_components


def remove_links(match_links: DataFrame, blocked_links: DataFrame) -> DataFrame:
    """Anti-join blocked edges out of the link set (by endpoints + rule)."""
    return match_links.join(
        blocked_links.select("src", "dst", "match_rule", "edge_type"),
        ["src", "dst", "match_rule", "edge_type"],
        "left_anti",
    )


def save_blocked_links(blocked_links: DataFrame, reference: DataFrame, cfg: Dict[str, Any], country_code: str) -> int:
    """Write blocked bridging edges to mdm_rule_results for stewardship; returns the row count.

    The original ``match_rule`` / ``match_key`` are kept so stewards can see which rule
    wanted to bridge the groups. Columns beyond the base DDL arrive via mergeSchema.
    """
    key_column = cfg["rowRegistryKeyColumn"]
    src_keys = reference.select(
        F.col("record_id").alias("src"), F.col(key_column).cast("string").alias("SrcOperatorConcatId")
    ).dropDuplicates(["src"])
    dst_keys = reference.select(
        F.col("record_id").alias("dst"), F.col(key_column).cast("string").alias("DstOperatorConcatId")
    ).dropDuplicates(["dst"])

    slice_df = (
        blocked_links.join(src_keys, "src", "left")
        .join(dst_keys, "dst", "left")
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
    written = write.overwrite_slice(
        slice_df,
        cfg["ruleResultsTable"],
        f"CountryCode = '{sql_literal(country_code)}' "
        f"AND RuleStageName = '{sql_literal(BLOCKED_SOURCE_GROUP_MERGE_RULE_STAGE)}'",
        merge_schema=True,
    )
    return written.count()
