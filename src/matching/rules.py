"""Turn one match rule into match links.

Both passes produce the same link shape (``expressions.MATCH_LINK_COLUMNS``):

* **exact** — hash the rule's key columns, keep blocks of 2..max_block_size records, and
  emit star edges from the block's lowest record id. O(n) instead of O(n^2).
* **fuzzy** — build candidate pairs from the rule's blocking keys, score them
  (Levenshtein / token Jaccard / exact), and keep the pairs the conditions accept. The
  accepted pairs and their evidence go to mdm_rule_evaluations; ``rule_evaluation_detail``
  decides whether rejected candidates are written too (they are quadratic in block size,
  so not by default).

Both return a lazy DataFrame. The caller materializes it through ``save_rule_results``,
which drops the similarity columns — the same shape as the working notebook. Do not
``persist().count()`` the evidence-bearing plan: that HashAggregate is what whole-stage
codegen NPEs on, and the notebook never runs it.

``subject_ids`` carries the priority waterfall: once set, a link is only emitted if at
least one endpoint is still unmatched by a higher-priority rule, and the flags say which
endpoint that was.
"""
from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Any, Dict, List, Optional, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from matching import write
from matching.helpers import (
    apply_exclusion,
    compared_values,
    dedupe_match_links,
    exact_empty_match,
    exact_match_key,
    exact_not_empty_match,
    evidence_condition,
    levenshtein_similarity,
    no_evidence,
    rule_matches,
    timed,
    token_jaccard_similarity,
    valid_value,
)

def _subject_flags(links: DataFrame, subject_ids: Optional[DataFrame]) -> DataFrame:
    """Flag each endpoint as a rule subject, dropping links where neither endpoint is one.

    Without a subject set (no waterfall) both endpoints count as subjects.
    """
    if subject_ids is None:
        return links.withColumn("src_is_rule_subject", F.lit(True)).withColumn("dst_is_rule_subject", F.lit(True))

    src_subjects = subject_ids.select(F.col("record_id").alias("src"), F.lit(True).alias("_src")).distinct()
    dst_subjects = subject_ids.select(F.col("record_id").alias("dst"), F.lit(True).alias("_dst")).distinct()
    return (
        links.join(src_subjects, "src", "left")
        .join(dst_subjects, "dst", "left")
        .filter(F.coalesce(F.col("_src"), F.lit(False)) | F.coalesce(F.col("_dst"), F.lit(False)))
        .withColumn("src_is_rule_subject", F.coalesce(F.col("_src"), F.lit(False)))
        .withColumn("dst_is_rule_subject", F.coalesce(F.col("_dst"), F.lit(False)))
        .drop("_src", "_dst")
    )


def _rule_identity(rule: Dict[str, Any], edge_type: str) -> List[F.Column]:
    return [
        F.lit(rule["rule_name"]).alias("match_rule"),
        F.lit(int(rule.get("priority", 100))).alias("rule_priority"),
        F.lit(edge_type).alias("edge_type"),
    ]


# ----------------------------------------------------------------------------- exact


def run_exact_rule(
    df: DataFrame,
    rule: Dict[str, Any],
    cfg: Dict[str, Any],
    subject_ids: Optional[DataFrame] = None,
) -> DataFrame:
    """Star edges between records whose rule key columns are identical."""
    invalid_values = cfg.get("invalid_values", [])
    max_block_size = int(cfg.get("exact_max_block_size", 50000))

    with timed(f"Exact rule: {rule['rule_name']}"):
        candidates = apply_exclusion(df, rule.get("match_exclusion_filter"), "rule exclusion")

        # Every key column must hold a real value, else the "key" would be shared junk.
        value_checks = [
            valid_value(F.col(spec["column"]), invalid_values, int(spec.get("min_length", 1)))
            for spec in rule["columns"]
            if spec.get("method", "exact_not_empty") != "exact_empty"
        ]
        if value_checks:
            candidates = candidates.filter(reduce(and_, value_checks))

        keyed = candidates.select(
            "record_id",
            F.sha2(F.concat_ws("||", *[exact_match_key(spec) for spec in rule["columns"]]), 256).alias("match_key"),
        ).dropDuplicates(["record_id", "match_key"])

        # A block of 1 has nothing to link; an oversized block is junk (a shared default value).
        usable_keys = (
            keyed.groupBy("match_key")
            .agg(F.count("*").alias("block_count"))
            .filter((F.col("block_count") >= 2) & (F.col("block_count") <= max_block_size))
            .select("match_key")
        )
        keyed = keyed.join(usable_keys, "match_key", "inner")

        anchors = keyed.groupBy("match_key").agg(F.min("record_id").alias("anchor_id"))
        star_edges = (
            keyed.join(anchors, "match_key", "inner")
            .filter(F.col("record_id") != F.col("anchor_id"))
            .select(
                F.col("anchor_id").alias("src"),
                F.col("record_id").alias("dst"),
                "match_key",
                *_rule_identity(rule, "exact"),
                F.lit("exact_key").alias("block_name"),
                *no_evidence(),
            )
        )
        return dedupe_match_links(_subject_flags(star_edges, subject_ids))


# ----------------------------------------------------------------------------- fuzzy


def _resolve_blocking(rule: Dict[str, Any], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """A rule's blocking definitions: inline ``blocking``, named ones, or the country default."""
    if rule.get("blocking"):
        return rule["blocking"]

    defaults = cfg.get("default_fuzzy_blocking", [])
    if not rule.get("blocking_names"):
        return defaults

    by_name = {block["block_name"]: block for block in defaults}
    missing = [name for name in rule["blocking_names"] if name not in by_name]
    if missing:
        raise ValueError(
            f"Fuzzy rule {rule['rule_name']} references unknown blocking_names {missing}. "
            "Define them in default_fuzzy_blocking."
        )
    return [by_name[name] for name in rule["blocking_names"]]


def _candidate_pairs(
    df: DataFrame, rule: Dict[str, Any], cfg: Dict[str, Any], subject_ids: Optional[DataFrame]
) -> Tuple[DataFrame, DataFrame]:
    """(src, dst) pairs that share at least one blocking key, plus the cached block rows.

    The block cache stays live so the caller can score pairs while it is warm, then
    unpersist it. Pairs themselves stay lazy — counting them here would split pair
    generation from scoring into a slimmer whole-stage plan, which is the NPE shape.
    """
    invalid_values = cfg.get("invalid_values", [])
    default_max_block_size = int(cfg.get("fuzzy_max_block_size", 500))

    blocks_per_definition = []
    for block in _resolve_blocking(rule, cfg):
        columns = block["columns"]
        blocks_per_definition.append(
            df.filter(reduce(and_, [valid_value(F.col(c), invalid_values, 1) for c in columns]))
            .select(
                "record_id",
                F.lit(block["block_name"]).alias("block_name"),
                F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]), 256).alias("block_key"),
                F.lit(int(block.get("max_block_size", default_max_block_size))).alias("block_max_size"),
            )
            .dropDuplicates(["record_id", "block_name", "block_key"])
        )
    if not blocks_per_definition:
        raise ValueError(f"Fuzzy rule {rule['rule_name']} must define at least one blocking rule")

    blocks = reduce(lambda a, b: a.unionByName(b), blocks_per_definition)
    usable = (
        blocks.groupBy("block_name", "block_key")
        .agg(F.count("*").alias("block_count"), F.max("block_max_size").alias("block_max_size"))
        .filter((F.col("block_count") >= 2) & (F.col("block_count") <= F.col("block_max_size")))
        .select("block_name", "block_key")
    )
    blocks = blocks.drop("block_max_size").join(usable, ["block_name", "block_key"], "inner").persist(
        StorageLevel.MEMORY_AND_DISK
    )

    right = blocks.alias("b")
    if subject_ids is None:
        # No waterfall: compare each unordered pair once via src < dst.
        pairs = blocks.alias("a").join(
            right,
            (F.col("a.block_name") == F.col("b.block_name"))
            & (F.col("a.block_key") == F.col("b.block_key"))
            & (F.col("a.record_id") < F.col("b.record_id")),
            "inner",
        )
        ends = (F.col("a.record_id").alias("src"), F.col("b.record_id").alias("dst"))
    else:
        # Subjects must be compared against every block member, so pairs are emitted in both
        # directions and normalized to (least, greatest) afterwards.
        pairs = blocks.join(subject_ids.select("record_id").distinct(), "record_id", "inner").alias("a").join(
            right,
            (F.col("a.block_name") == F.col("b.block_name"))
            & (F.col("a.block_key") == F.col("b.block_key"))
            & (F.col("a.record_id") != F.col("b.record_id")),
            "inner",
        )
        ends = (
            F.least(F.col("a.record_id"), F.col("b.record_id")).alias("src"),
            F.greatest(F.col("a.record_id"), F.col("b.record_id")).alias("dst"),
        )

    result = (
        pairs.select(*ends, F.col("a.block_name").alias("block_name"), F.col("a.block_key").alias("match_key"))
        .groupBy("src", "dst")
        .agg(
            F.concat_ws(",", F.sort_array(F.collect_set("block_name"))).alias("block_name"),
            F.concat_ws(",", F.sort_array(F.collect_set("match_key"))).alias("match_key"),
        )
    )
    return result, blocks


def _score_pairs(candidates: DataFrame, rule: Dict[str, Any], invalid_values: List[str]) -> DataFrame:
    """Attach every similarity / condition-passed column the rule's conditions can use."""
    specs = {spec["column"]: spec for spec in rule.get("conditions", [])}
    none_double = F.lit(None).cast("double")

    def similarity(column: str, methods: set) -> F.Column:
        spec = specs.get(column)
        if spec is None or spec.get("method") not in methods:
            return none_double
        return levenshtein_similarity(
            F.col(f"a.{column}"), F.col(f"b.{column}"), invalid_values, int(spec.get("min_length", 1))
        )

    address = specs.get("c_address")
    address_method = address.get("method", "") if address else ""
    address_levenshtein = similarity("c_address", {"levenshtein_similarity", "levenshtein_or_token_jaccard"})
    address_jaccard = (
        token_jaccard_similarity(
            F.col("a.c_address"),
            F.col("b.c_address"),
            invalid_values,
            int(address.get("min_length", 1)),
            int(address.get("min_token_length", 2)),
        )
        if address_method in {"token_jaccard", "levenshtein_or_token_jaccard"}
        else none_double
    )
    address_best = {
        "levenshtein_similarity": address_levenshtein,
        "token_jaccard": address_jaccard,
        "levenshtein_or_token_jaccard": F.greatest(
            F.coalesce(address_levenshtein, F.lit(0.0)), F.coalesce(address_jaccard, F.lit(0.0))
        ),
    }.get(address_method, none_double)

    zip_spec = specs.get("c_zip")
    zip_method = zip_spec.get("method", "exact_not_empty") if zip_spec else None
    if zip_method == "exact_empty":
        zip_match = exact_empty_match(F.col("a.c_zip"), F.col("b.c_zip"))
    elif zip_method == "exact_not_empty":
        zip_match = exact_not_empty_match(
            F.col("a.c_zip"), F.col("b.c_zip"), invalid_values, int(zip_spec.get("min_length", 1))
        )
    else:
        zip_match = F.lit(None).cast("boolean")

    scored = (
        candidates.withColumn("NameLevenshteinSimilarity", similarity("c_name", {"levenshtein_similarity"}))
        .withColumn("AddressLevenshteinSimilarity", address_levenshtein)
        .withColumn("AddressTokenJaccardSimilarity", address_jaccard)
        .withColumn("AddressBestSimilarity", address_best)
        .withColumn("CityLevenshteinSimilarity", similarity("c_city", {"levenshtein_similarity"}))
        .withColumn("StateLevenshteinSimilarity", similarity("c_state", {"levenshtein_similarity"}))
        .withColumn("ZipExactMatch", zip_match)
    )
    for column, flag in (
        ("c_name", "NameConditionPassed"),
        ("c_address", "AddressConditionPassed"),
        ("c_city", "CityConditionPassed"),
        ("c_state", "StateConditionPassed"),
        ("c_zip", "ZipConditionPassed"),
    ):
        spec = specs.get(column)
        passed = evidence_condition(spec, invalid_values).cast("boolean") if spec else F.lit(None).cast("boolean")
        scored = scored.withColumn(flag, passed)
    return scored


def run_fuzzy_rule(
    df: DataFrame,
    rule: Dict[str, Any],
    cfg: Dict[str, Any],
    country_code: str,
    rule_stage_name: str,
    rule_execution_order: int,
    subject_ids: Optional[DataFrame] = None,
) -> DataFrame:
    """Score blocked candidate pairs and keep the ones the rule's conditions accept."""
    invalid_values = cfg.get("invalid_values", [])
    key_column = cfg["rowRegistryKeyColumn"]

    with timed(f"Fuzzy rule: {rule['rule_name']}"):
        population = apply_exclusion(df, rule.get("match_exclusion_filter"), "rule exclusion")
        population = population.filter(F.trim(F.coalesce(F.col("c_address"), F.lit(""))) != "").persist(
            StorageLevel.MEMORY_AND_DISK
        )

        pairs, blocks = _candidate_pairs(population, rule, cfg, subject_ids)

        condition_columns = sorted({spec["column"] for spec in rule.get("conditions", [])})
        trace_columns = sorted({key_column, "c_name", "c_address", "c_city", "c_state", "c_zip", *condition_columns})
        left = population.select("record_id", *trace_columns).alias("a")
        right = population.select("record_id", *trace_columns).alias("b")
        candidates = pairs.join(left, F.col("src") == F.col("a.record_id"), "inner").join(
            right, F.col("dst") == F.col("b.record_id"), "inner"
        )

        scored = _subject_flags(_score_pairs(candidates, rule, invalid_values), subject_ids)
        accepted = scored.filter(rule_matches(rule, invalid_values))

        # This table used to take every candidate pair: ~11M rows x 34 columns per rule on
        # MY, of evidence nothing reads back, built on a self-join of `scored` with a
        # filtered copy of itself. Default to the accepted pairs — one row per link, and no
        # self-join at all. "all" restores the old behaviour for threshold tuning; use it on
        # a filtered slice, not a whole country.
        detail = cfg.get("rule_evaluation_detail", "matched")
        if detail == "all":
            evaluated = scored.join(
                accepted.select("src", "dst").dropDuplicates(["src", "dst"]).withColumn("_matched", F.lit(True)),
                ["src", "dst"],
                "left",
            ).withColumn("IsMatched", F.coalesce(F.col("_matched"), F.lit(False)))
        else:
            evaluated = accepted.withColumn("IsMatched", F.lit(True))

        if detail != "none":
            write.save_rule_evaluations(
                evaluated.select(
                    "src",
                    "dst",
                    F.col(f"a.{key_column}").cast("string").alias("SrcOperatorConcatId"),
                    F.col(f"b.{key_column}").cast("string").alias("DstOperatorConcatId"),
                    F.col("a.c_name").cast("string").alias("SrcName"),
                    F.col("b.c_name").cast("string").alias("DstName"),
                    F.col("a.c_address").cast("string").alias("SrcAddress"),
                    F.col("b.c_address").cast("string").alias("DstAddress"),
                    F.col("a.c_city").cast("string").alias("SrcCity"),
                    F.col("b.c_city").cast("string").alias("DstCity"),
                    F.col("a.c_state").cast("string").alias("SrcState"),
                    F.col("b.c_state").cast("string").alias("DstState"),
                    F.col("a.c_zip").cast("string").alias("SrcZip"),
                    F.col("b.c_zip").cast("string").alias("DstZip"),
                    compared_values("a", condition_columns).alias("SrcComparedValues"),
                    compared_values("b", condition_columns).alias("DstComparedValues"),
                    *_rule_identity(rule, "fuzzy"),
                    "block_name",
                    "match_key",
                    "src_is_rule_subject",
                    "dst_is_rule_subject",
                    "NameLevenshteinSimilarity",
                    "AddressLevenshteinSimilarity",
                    "AddressTokenJaccardSimilarity",
                    "AddressBestSimilarity",
                    "CityLevenshteinSimilarity",
                    "StateLevenshteinSimilarity",
                    "ZipExactMatch",
                    "NameConditionPassed",
                    "AddressConditionPassed",
                    "CityConditionPassed",
                    "StateConditionPassed",
                    "ZipConditionPassed",
                    "IsMatched",
                ),
                cfg["ruleEvaluationsTable"],
                country_code,
                rule_stage_name,
                rule_execution_order,
            )

        # Filter already used the scores. Drop them before the aggregate so this plan
        # cannot HashAggregate nullable doubles / ZipExactMatch — that persist().count()
        # is what NPEd, and the notebook never does it (rule_results has no evidence).
        links = dedupe_match_links(
            accepted.select(
                "src",
                "dst",
                *_rule_identity(rule, "fuzzy"),
                "block_name",
                "match_key",
                "src_is_rule_subject",
                "dst_is_rule_subject",
                *no_evidence(),
            )
        )
        blocks.unpersist()
        population.unpersist()
        return links
