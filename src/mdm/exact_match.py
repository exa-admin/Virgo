"""Exact-match star-edge generation with block-size caps and priority waterfall."""
from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mdm.utils import (
    _apply_exclusion,
    _dedupe_match_links,
    _empty_ids,
    _empty_match_links,
    _exact_match_key_value,
    _matched_record_ids_from_links,
    _sort_rules_by_priority,
    _union_all,
    _valid_value,
    timed,
)

def run_exact_match(
    df: DataFrame,
    rules: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    rule_subject_ids: Optional[DataFrame] = None,
) -> DataFrame:
    spark = df.sparkSession
    invalid_values = cfg.get("invalid_values", [])
    max_block_size = int(cfg.get("exact_max_block_size", cfg.get("max_block_size", 50000)))
    priority_matching = bool(cfg.get("priorityMatching", cfg.get("waterfall_rules", True)))
    matched_record_ids = _empty_ids(spark)
    match_links = []

    for rule in _sort_rules_by_priority(rules):
        with timed(f"Exact rule: {rule['rule_name']}"):
            df_for_rule = df
            if priority_matching and rule_subject_ids is None:
                df_for_rule = df_for_rule.join(matched_record_ids, "record_id", "left_anti")

            df_for_rule = _apply_exclusion(df_for_rule, rule.get("match_exclusion_filter"), "rule exclusion")
            valid_conditions = [
                _valid_value(F.col(spec["column"]), invalid_values, int(spec.get("min_length", 1)))
                for spec in rule["columns"]
                if spec.get("method", "exact_not_empty") != "exact_empty"
            ]
            if valid_conditions:
                df_for_rule = df_for_rule.filter(reduce(and_, valid_conditions))

            key_cols = [_exact_match_key_value(spec) for spec in rule["columns"]]
            keyed = df_for_rule.select(
                "record_id",
                F.sha2(F.concat_ws("||", *key_cols), 256).alias("match_key"),
            ).dropDuplicates(["record_id", "match_key"])

            valid_keys = (
                keyed.groupBy("match_key")
                .agg(F.count("*").alias("block_count"))
                .filter((F.col("block_count") >= 2) & (F.col("block_count") <= max_block_size))
                .select("match_key")
            )
            keyed = keyed.join(valid_keys, "match_key", "inner")

            anchors = keyed.groupBy("match_key").agg(F.min("record_id").alias("anchor_id"))
            star_edges = (
                keyed.join(anchors, "match_key", "inner")
                .filter(F.col("record_id") != F.col("anchor_id"))
                .select(
                    F.col("anchor_id").alias("src"),
                    F.col("record_id").alias("dst"),
                    F.col("match_key"),
                )
            )

            if rule_subject_ids is not None:
                subject_src = rule_subject_ids.select(F.col("record_id").alias("src"), F.lit(True).alias("_src_is_rule_subject")).distinct()
                subject_dst = rule_subject_ids.select(F.col("record_id").alias("dst"), F.lit(True).alias("_dst_is_rule_subject")).distinct()
                matches = _dedupe_match_links(
                    star_edges.join(subject_src, "src", "left")
                    .join(subject_dst, "dst", "left")
                    .filter(F.coalesce(F.col("_src_is_rule_subject"), F.lit(False)) | F.coalesce(F.col("_dst_is_rule_subject"), F.lit(False)))
                    .select(
                        "src",
                        "dst",
                        F.lit(rule["rule_name"]).alias("match_rule"),
                        F.lit(int(rule.get("priority", 100))).alias("rule_priority"),
                        F.lit("exact").alias("edge_type"),
                        F.lit("exact_key").alias("block_name"),
                        F.col("match_key"),
                        F.coalesce(F.col("_src_is_rule_subject"), F.lit(False)).alias("src_is_rule_subject"),
                        F.coalesce(F.col("_dst_is_rule_subject"), F.lit(False)).alias("dst_is_rule_subject"),
                        F.lit(None).cast("double").alias("NameLevenshteinSimilarity"),
                        F.lit(None).cast("double").alias("AddressLevenshteinSimilarity"),
                        F.lit(None).cast("double").alias("AddressTokenJaccardSimilarity"),
                        F.lit(None).cast("double").alias("AddressBestSimilarity"),
                        F.lit(None).cast("boolean").alias("ZipExactMatch"),
                    )
                )
            else:
                matches = (
                    star_edges.select(
                        "src",
                        "dst",
                        F.lit(rule["rule_name"]).alias("match_rule"),
                        F.lit(int(rule.get("priority", 100))).alias("rule_priority"),
                        F.lit("exact").alias("edge_type"),
                        F.lit("exact_key").alias("block_name"),
                        F.col("match_key"),
                        F.lit(True).alias("src_is_rule_subject"),
                        F.lit(True).alias("dst_is_rule_subject"),
                        F.lit(None).cast("double").alias("NameLevenshteinSimilarity"),
                        F.lit(None).cast("double").alias("AddressLevenshteinSimilarity"),
                        F.lit(None).cast("double").alias("AddressTokenJaccardSimilarity"),
                        F.lit(None).cast("double").alias("AddressBestSimilarity"),
                        F.lit(None).cast("boolean").alias("ZipExactMatch"),
                    )
                    .dropDuplicates(["src", "dst", "match_rule", "match_key"])
                )

            match_links.append(matches)
            if priority_matching:
                matched_record_ids = matched_record_ids.unionByName(_matched_record_ids_from_links(matches)).dropDuplicates(["record_id"])

    return _dedupe_match_links(_union_all(match_links)) if match_links else _empty_match_links(spark)
