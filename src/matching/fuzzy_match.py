"""Fuzzy match with blocking, Levenshtein / token Jaccard, and rule evaluation materialization."""
from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Any, Dict, List, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from matching.delta_io import _materialize_rule_evaluations
from matching.utils import (
    _apply_exclusion,
    _compared_values_from_alias_expr,
    _dedupe_match_links,
    _empty_ids,
    _empty_match_links,
    _evidence_condition,
    _exact_empty_condition,
    _exact_not_empty_condition,
    _levenshtein_similarity_expr,
    _matched_record_ids_from_links,
    _rule_conditions_from_evidence,
    _sort_rules_by_priority,
    _token_jaccard_similarity_expr,
    _union_all,
    _valid_value,
    timed,
)

def _resolve_fuzzy_blocking(rule: Dict[str, Any], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    if rule.get("blocking"):
        return rule["blocking"]

    default_fuzzy_blocking = cfg.get("default_fuzzy_blocking", [])
    if rule.get("blocking_names"):
        default_blocks_by_name = {block["block_name"]: block for block in default_fuzzy_blocking}
        resolved_blocks = []
        missing_block_names = []
        for block_name in rule["blocking_names"]:
            if block_name not in default_blocks_by_name:
                missing_block_names.append(block_name)
                continue
            resolved_blocks.append(default_blocks_by_name[block_name])
        if missing_block_names:
            raise ValueError(
                f"Fuzzy rule {rule['rule_name']} references unknown blocking_names {missing_block_names}. "
                "Define them in default_fuzzy_blocking."
            )
        return resolved_blocks

    return default_fuzzy_blocking

def _make_candidate_blocks(df: DataFrame, rule: Dict[str, Any], cfg: Dict[str, Any]) -> DataFrame:
    invalid_values = cfg.get("invalid_values", [])
    default_max_block_size = int(cfg.get("fuzzy_max_block_size", cfg.get("max_block_size", 500)))
    block_dfs = []
    for block in _resolve_fuzzy_blocking(rule, cfg):
        block_name = block["block_name"]
        columns = block["columns"]
        block_max_size = int(block.get("max_block_size", default_max_block_size))
        block_filter = reduce(and_, [_valid_value(F.col(c), invalid_values, 1) for c in columns])
        block_key = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]), 256)
        block_dfs.append(
            df.filter(block_filter)
            .select(
                "record_id",
                F.lit(block_name).alias("block_name"),
                block_key.alias("block_key"),
                F.lit(block_max_size).alias("block_max_size"),
            )
            .dropDuplicates(["record_id", "block_name", "block_key"])
        )

    if not block_dfs:
        raise ValueError(f"Fuzzy rule {rule['rule_name']} must define at least one blocking rule")

    blocks = _union_all(block_dfs)
    valid_blocks = (
        blocks.groupBy("block_name", "block_key")
        .agg(
            F.count("*").alias("block_count"),
            F.max("block_max_size").alias("block_max_size"),
        )
        .filter((F.col("block_count") >= 2) & (F.col("block_count") <= F.col("block_max_size")))
        .select("block_name", "block_key")
    )
    return blocks.drop("block_max_size").join(valid_blocks, ["block_name", "block_key"], "inner")

def run_fuzzy_match(
    df: DataFrame,
    rules: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    matched_record_ids: Optional[DataFrame] = None,
    rule_subject_ids: Optional[DataFrame] = None,
    country_code: Optional[str] = None,
    rule_stage_name: Optional[str] = None,
    rule_execution_order: Optional[int] = None,
) -> DataFrame:
    spark = df.sparkSession
    invalid_values = cfg.get("invalid_values", [])
    priority_matching = bool(cfg.get("priorityMatching", cfg.get("waterfall_rules", True)))
    matched_record_ids = matched_record_ids if matched_record_ids is not None else _empty_ids(spark)
    match_links = []

    for rule in _sort_rules_by_priority(rules):
        with timed(f"Fuzzy rule: {rule['rule_name']}"):
            df_for_rule = df
            if priority_matching and rule_subject_ids is None:
                df_for_rule = df_for_rule.join(matched_record_ids.select("record_id").distinct(), "record_id", "left_anti")

            df_for_rule = _apply_exclusion(df_for_rule, rule.get("match_exclusion_filter"), "rule exclusion")
            df_for_rule = df_for_rule.filter(F.trim(F.coalesce(F.col("c_address"), F.lit(""))) != "")
            df_for_rule = df_for_rule.persist(StorageLevel.MEMORY_AND_DISK)

            blocks = _make_candidate_blocks(df_for_rule, rule, cfg).persist(StorageLevel.MEMORY_AND_DISK)
            if rule_subject_ids is not None:
                subject_blocks = blocks.join(rule_subject_ids.select("record_id").distinct(), "record_id", "inner")
            else:
                subject_blocks = blocks

            a_blocks = subject_blocks.alias("a")
            b_blocks = blocks.alias("b")
            if rule_subject_ids is not None:
                raw_pairs = a_blocks.join(
                    b_blocks,
                    (F.col("a.block_name") == F.col("b.block_name"))
                    & (F.col("a.block_key") == F.col("b.block_key"))
                    & (F.col("a.record_id") != F.col("b.record_id")),
                    "inner",
                )
                pairs = raw_pairs.select(
                    F.least(F.col("a.record_id"), F.col("b.record_id")).alias("src"),
                    F.greatest(F.col("a.record_id"), F.col("b.record_id")).alias("dst"),
                    F.col("a.block_name").alias("block_name"),
                    F.col("a.block_key").alias("match_key"),
                )
            else:
                pairs = (
                    a_blocks.join(
                        b_blocks,
                        (F.col("a.block_name") == F.col("b.block_name"))
                        & (F.col("a.block_key") == F.col("b.block_key"))
                        & (F.col("a.record_id") < F.col("b.record_id")),
                        "inner",
                    )
                    .select(
                        F.col("a.record_id").alias("src"),
                        F.col("b.record_id").alias("dst"),
                        F.col("a.block_name").alias("block_name"),
                        F.col("a.block_key").alias("match_key"),
                    )
                )
            pairs = (
                pairs.groupBy("src", "dst")
                .agg(
                    F.concat_ws(",", F.sort_array(F.collect_set("block_name"))).alias("block_name"),
                    F.concat_ws(",", F.sort_array(F.collect_set("match_key"))).alias("match_key"),
                )
            )

            needed_cols = sorted(
                {
                    c["column"]
                    for c in rule.get("conditions", [])
                }
            )
            trace_cols = sorted(
                {
                    cfg["rowRegistryKeyColumn"],
                    "c_name",
                    "c_address",
                    "c_city",
                    "c_state",
                    "c_zip",
                    *needed_cols,
                }
            )
            left = df_for_rule.select("record_id", *trace_cols).alias("a")
            right = df_for_rule.select("record_id", *trace_cols).alias("b")
            candidates = (
                pairs.join(left, F.col("src") == F.col("a.record_id"), "inner")
                .join(right, F.col("dst") == F.col("b.record_id"), "inner")
            )

            condition_specs = {spec["column"]: spec for spec in rule.get("conditions", [])}

            name_spec = condition_specs.get("c_name")
            if name_spec and name_spec.get("method") == "levenshtein_similarity":
                name_similarity = _levenshtein_similarity_expr(
                    F.col("a.c_name"),
                    F.col("b.c_name"),
                    invalid_values,
                    int(name_spec.get("min_length", 1)),
                )
            else:
                name_similarity = F.lit(None).cast("double")

            address_spec = condition_specs.get("c_address")
            if address_spec:
                address_method = address_spec.get("method", "")
                address_min_length = int(address_spec.get("min_length", 1))
                address_min_token_length = int(address_spec.get("min_token_length", 2))
                if address_method in {"levenshtein_similarity", "levenshtein_or_token_jaccard"}:
                    address_levenshtein_similarity = _levenshtein_similarity_expr(
                        F.col("a.c_address"),
                        F.col("b.c_address"),
                        invalid_values,
                        address_min_length,
                    )
                else:
                    address_levenshtein_similarity = F.lit(None).cast("double")
                if address_method in {"token_jaccard", "levenshtein_or_token_jaccard"}:
                    address_token_jaccard_similarity = _token_jaccard_similarity_expr(
                        F.col("a.c_address"),
                        F.col("b.c_address"),
                        invalid_values,
                        address_min_length,
                        address_min_token_length,
                    )
                else:
                    address_token_jaccard_similarity = F.lit(None).cast("double")
                if address_method == "levenshtein_similarity":
                    address_best_similarity = address_levenshtein_similarity
                elif address_method == "token_jaccard":
                    address_best_similarity = address_token_jaccard_similarity
                elif address_method == "levenshtein_or_token_jaccard":
                    address_best_similarity = F.greatest(address_levenshtein_similarity, address_token_jaccard_similarity)
                else:
                    address_best_similarity = F.lit(None).cast("double")
            else:
                address_levenshtein_similarity = F.lit(None).cast("double")
                address_token_jaccard_similarity = F.lit(None).cast("double")
                address_best_similarity = F.lit(None).cast("double")

            zip_spec = condition_specs.get("c_zip")
            if zip_spec:
                zip_method = zip_spec.get("method", "exact_not_empty")
                zip_min_length = int(zip_spec.get("min_length", 1))
                if zip_method == "exact_empty":
                    zip_exact_match = _exact_empty_condition(F.col("a.c_zip"), F.col("b.c_zip"))
                elif zip_method == "exact_not_empty":
                    zip_exact_match = _exact_not_empty_condition(
                        F.col("a.c_zip"),
                        F.col("b.c_zip"),
                        invalid_values,
                        zip_min_length,
                    )
                else:
                    zip_exact_match = F.lit(None).cast("boolean")
            else:
                zip_exact_match = F.lit(None).cast("boolean")

            city_spec = condition_specs.get("c_city")
            if city_spec and city_spec.get("method") == "levenshtein_similarity":
                city_levenshtein_similarity = _levenshtein_similarity_expr(
                    F.col("a.c_city"),
                    F.col("b.c_city"),
                    invalid_values,
                    int(city_spec.get("min_length", 1)),
                )
            else:
                city_levenshtein_similarity = F.lit(None).cast("double")

            state_spec = condition_specs.get("c_state")
            if state_spec and state_spec.get("method") == "levenshtein_similarity":
                state_levenshtein_similarity = _levenshtein_similarity_expr(
                    F.col("a.c_state"),
                    F.col("b.c_state"),
                    invalid_values,
                    int(state_spec.get("min_length", 1)),
                )
            else:
                state_levenshtein_similarity = F.lit(None).cast("double")

            name_condition_passed = (
                _evidence_condition(name_spec, invalid_values).cast("boolean")
                if name_spec is not None
                else F.lit(None).cast("boolean")
            )
            address_condition_passed = (
                _evidence_condition(address_spec, invalid_values).cast("boolean")
                if address_spec is not None
                else F.lit(None).cast("boolean")
            )
            city_condition_passed = (
                _evidence_condition(city_spec, invalid_values).cast("boolean")
                if city_spec is not None
                else F.lit(None).cast("boolean")
            )
            state_condition_passed = (
                _evidence_condition(state_spec, invalid_values).cast("boolean")
                if state_spec is not None
                else F.lit(None).cast("boolean")
            )
            zip_condition_passed = (
                _evidence_condition(zip_spec, invalid_values).cast("boolean")
                if zip_spec is not None
                else F.lit(None).cast("boolean")
            )

            candidates_with_evidence = (
                candidates.withColumn("NameLevenshteinSimilarity", name_similarity)
                .withColumn("AddressLevenshteinSimilarity", address_levenshtein_similarity)
                .withColumn("AddressTokenJaccardSimilarity", address_token_jaccard_similarity)
                .withColumn("AddressBestSimilarity", address_best_similarity)
                .withColumn("CityLevenshteinSimilarity", city_levenshtein_similarity)
                .withColumn("StateLevenshteinSimilarity", state_levenshtein_similarity)
                .withColumn("ZipExactMatch", zip_exact_match)
                .withColumn("NameConditionPassed", name_condition_passed)
                .withColumn("AddressConditionPassed", address_condition_passed)
                .withColumn("CityConditionPassed", city_condition_passed)
                .withColumn("StateConditionPassed", state_condition_passed)
                .withColumn("ZipConditionPassed", zip_condition_passed)
            )

            filtered_candidates = candidates_with_evidence.filter(_rule_conditions_from_evidence(rule, invalid_values))
            matched_candidates = filtered_candidates.select("src", "dst").dropDuplicates(["src", "dst"]).withColumn("_IsMatched", F.lit(True))

            if rule_subject_ids is not None:
                subject_src = rule_subject_ids.select(F.col("record_id").alias("src"), F.lit(True).alias("_src_is_rule_subject")).distinct()
                subject_dst = rule_subject_ids.select(F.col("record_id").alias("dst"), F.lit(True).alias("_dst_is_rule_subject")).distinct()
            else:
                subject_src = None
                subject_dst = None

            if country_code is not None and rule_stage_name is not None and rule_execution_order is not None:
                candidate_evaluations = (
                    candidates_with_evidence.join(matched_candidates, ["src", "dst"], "left")
                    .withColumn("IsMatched", F.coalesce(F.col("_IsMatched"), F.lit(False)))
                    .drop("_IsMatched")
                )
                if subject_src is not None and subject_dst is not None:
                    candidate_evaluations = (
                        candidate_evaluations.join(subject_src, "src", "left")
                        .join(subject_dst, "dst", "left")
                        .withColumn("src_is_rule_subject", F.coalesce(F.col("_src_is_rule_subject"), F.lit(False)))
                        .withColumn("dst_is_rule_subject", F.coalesce(F.col("_dst_is_rule_subject"), F.lit(False)))
                        .drop("_src_is_rule_subject", "_dst_is_rule_subject")
                    )
                else:
                    candidate_evaluations = (
                        candidate_evaluations.withColumn("src_is_rule_subject", F.lit(True))
                        .withColumn("dst_is_rule_subject", F.lit(True))
                    )

                candidate_evaluations = candidate_evaluations.select(
                    "src",
                    "dst",
                    F.col(f"a.{cfg['rowRegistryKeyColumn']}").cast("string").alias("SrcOperatorConcatId"),
                    F.col(f"b.{cfg['rowRegistryKeyColumn']}").cast("string").alias("DstOperatorConcatId"),
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
                    _compared_values_from_alias_expr("a", needed_cols).alias("SrcComparedValues"),
                    _compared_values_from_alias_expr("b", needed_cols).alias("DstComparedValues"),
                    F.lit(rule["rule_name"]).alias("match_rule"),
                    F.lit(int(rule.get("priority", 100))).alias("rule_priority"),
                    F.lit("fuzzy").alias("edge_type"),
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
                )
                _materialize_rule_evaluations(
                    candidate_evaluations,
                    cfg["ruleEvaluationsTable"],
                    country_code,
                    "fuzzy",
                    rule_stage_name,
                    rule_execution_order,
                )

            if rule_subject_ids is not None:
                matches = _dedupe_match_links(
                    filtered_candidates.select(
                        "src",
                        "dst",
                        F.lit(rule["rule_name"]).alias("match_rule"),
                        F.lit(int(rule.get("priority", 100))).alias("rule_priority"),
                        F.lit("fuzzy").alias("edge_type"),
                        "block_name",
                        "match_key",
                        "NameLevenshteinSimilarity",
                        "AddressLevenshteinSimilarity",
                        "AddressTokenJaccardSimilarity",
                        "AddressBestSimilarity",
                        "ZipExactMatch",
                    )
                    .join(subject_src, "src", "left")
                    .join(subject_dst, "dst", "left")
                    .select(
                        "src",
                        "dst",
                        "match_rule",
                        "rule_priority",
                        "edge_type",
                        "block_name",
                        "match_key",
                        F.coalesce(F.col("_src_is_rule_subject"), F.lit(False)).alias("src_is_rule_subject"),
                        F.coalesce(F.col("_dst_is_rule_subject"), F.lit(False)).alias("dst_is_rule_subject"),
                        "NameLevenshteinSimilarity",
                        "AddressLevenshteinSimilarity",
                        "AddressTokenJaccardSimilarity",
                        "AddressBestSimilarity",
                        "ZipExactMatch",
                    )
                )
            else:
                matches = (
                    filtered_candidates.select(
                        "src",
                        "dst",
                        F.lit(rule["rule_name"]).alias("match_rule"),
                        F.lit(int(rule.get("priority", 100))).alias("rule_priority"),
                        F.lit("fuzzy").alias("edge_type"),
                        "block_name",
                        "match_key",
                        F.lit(True).alias("src_is_rule_subject"),
                        F.lit(True).alias("dst_is_rule_subject"),
                        "NameLevenshteinSimilarity",
                        "AddressLevenshteinSimilarity",
                        "AddressTokenJaccardSimilarity",
                        "AddressBestSimilarity",
                        "ZipExactMatch",
                    )
                    .dropDuplicates(["src", "dst", "match_rule", "match_key"])
                )

            match_links.append(matches)
            if priority_matching:
                matched_record_ids = matched_record_ids.unionByName(_matched_record_ids_from_links(matches)).dropDuplicates(["record_id"])
            blocks.unpersist()
            df_for_rule.unpersist()

    return _dedupe_match_links(_union_all(match_links)) if match_links else _empty_match_links(spark)
