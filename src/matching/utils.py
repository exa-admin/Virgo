"""Shared helpers: timing, SQL literals, text cleaning, similarity, empty schemas."""
from __future__ import annotations

import re
import time
from contextlib import contextmanager
from functools import reduce
from operator import and_, or_
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def timed(step_name: str):
    started = time.time()
    print(f"[{step_name}] STARTED")
    yield
    print(f"[{step_name}] FINISHED in {time.time() - started:0.2f}s")

def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()

def _sql_literal(value: str) -> str:
    return value.replace("'", "''")

def _require_dataframe_columns(df: DataFrame, required_columns: List[str], label: str) -> None:
    missing_columns = [column_name for column_name in required_columns if column_name not in df.columns]
    if missing_columns:
        raise ValueError(f"{label} is missing required columns {missing_columns}")

def _empty_match_links(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("src", T.LongType(), False),
            T.StructField("dst", T.LongType(), False),
            T.StructField("match_rule", T.StringType(), False),
            T.StructField("rule_priority", T.IntegerType(), False),
            T.StructField("edge_type", T.StringType(), False),
            T.StructField("block_name", T.StringType(), False),
            T.StructField("match_key", T.StringType(), False),
            T.StructField("src_is_rule_subject", T.BooleanType(), False),
            T.StructField("dst_is_rule_subject", T.BooleanType(), False),
            T.StructField("NameLevenshteinSimilarity", T.DoubleType(), True),
            T.StructField("AddressLevenshteinSimilarity", T.DoubleType(), True),
            T.StructField("AddressTokenJaccardSimilarity", T.DoubleType(), True),
            T.StructField("AddressBestSimilarity", T.DoubleType(), True),
            T.StructField("ZipExactMatch", T.BooleanType(), True),
        ]
    )
    return spark.createDataFrame([], schema)

def _empty_ids(spark: SparkSession) -> DataFrame:
    schema = T.StructType([T.StructField("record_id", T.LongType(), False)])
    return spark.createDataFrame([], schema)

def _sort_rules_by_priority(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rules, key=lambda r: (int(r.get("priority", 100)), r.get("rule_name", "")))

def _matched_record_ids_from_links(match_links_df: DataFrame) -> DataFrame:
    return (
        match_links_df.select(F.col("src").alias("record_id"))
        .unionByName(match_links_df.select(F.col("dst").alias("record_id")))
        .dropDuplicates(["record_id"])
    )

def _dedupe_match_links(match_links_df: DataFrame) -> DataFrame:
    return (
        match_links_df.groupBy("src", "dst", "match_rule", "rule_priority", "edge_type")
        .agg(
            F.concat_ws(",", F.sort_array(F.collect_set("block_name"))).alias("block_name"),
            F.concat_ws(",", F.sort_array(F.collect_set("match_key"))).alias("match_key"),
            F.max(F.when(F.col("src_is_rule_subject"), F.lit(1)).otherwise(F.lit(0))).alias("_src_is_rule_subject"),
            F.max(F.when(F.col("dst_is_rule_subject"), F.lit(1)).otherwise(F.lit(0))).alias("_dst_is_rule_subject"),
            F.max("NameLevenshteinSimilarity").alias("NameLevenshteinSimilarity"),
            F.max("AddressLevenshteinSimilarity").alias("AddressLevenshteinSimilarity"),
            F.max("AddressTokenJaccardSimilarity").alias("AddressTokenJaccardSimilarity"),
            F.max("AddressBestSimilarity").alias("AddressBestSimilarity"),
            F.max(F.when(F.col("ZipExactMatch"), F.lit(1)).otherwise(F.lit(0))).alias("_ZipExactMatch"),
            F.max(F.when(F.col("ZipExactMatch").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("_HasZipExactMatch"),
        )
        .select(
            "src",
            "dst",
            "match_rule",
            "rule_priority",
            "edge_type",
            "block_name",
            "match_key",
            (F.col("_src_is_rule_subject") == F.lit(1)).alias("src_is_rule_subject"),
            (F.col("_dst_is_rule_subject") == F.lit(1)).alias("dst_is_rule_subject"),
            "NameLevenshteinSimilarity",
            "AddressLevenshteinSimilarity",
            "AddressTokenJaccardSimilarity",
            "AddressBestSimilarity",
            F.when(
                F.col("_HasZipExactMatch") == F.lit(0),
                F.lit(None).cast("boolean"),
            ).otherwise(F.col("_ZipExactMatch") == F.lit(1)).alias("ZipExactMatch"),
        )
    )

def _union_all(dfs: List[DataFrame]) -> DataFrame:
    if not dfs:
        raise ValueError("No DataFrames to union")
    return reduce(lambda a, b: a.unionByName(b), dfs)

def _sql_or(filters: Optional[Iterable[str]]) -> Optional[str]:
    cleaned = [f"({f})" for f in (filters or []) if f and f.strip()]
    return " OR ".join(cleaned) if cleaned else None

def _apply_exclusion(df: DataFrame, filters: Optional[Iterable[str]], label: str) -> DataFrame:
    where_clause = _sql_or(filters)
    if not where_clause:
        return df
    print(f"  -> Applying {label}: NOT ({where_clause})")
    return df.filter(f"NOT ({where_clause})")

def _clean_text(c: F.Column) -> F.Column:
    text = F.lower(F.coalesce(c.cast("string"), F.lit("")))
    text = F.regexp_replace(text, r"[^\p{L}0-9 ]", " ")
    text = F.regexp_replace(text, r"\s+", " ")
    return F.trim(text)

def _compact_key(c: F.Column) -> F.Column:
    text = F.lower(F.coalesce(c.cast("string"), F.lit("")))
    return F.regexp_replace(text, r"[^\p{L}0-9]", "")

def _remove_leading_zeroes(c: F.Column) -> F.Column:
    return F.regexp_replace(c, r"^0+", "")

def _normalize_exact_match_value(c: F.Column) -> F.Column:
    text = _clean_text(c)
    text = _remove_leading_zeroes(text)
    return text

def _raw_exact_match_value(c: F.Column) -> F.Column:
    return F.trim(F.coalesce(c.cast("string"), F.lit("")))

def _exact_match_key_value(method_spec: Dict[str, Any]) -> F.Column:
    method = method_spec.get("method", "exact_not_empty")
    column_expr = F.col(method_spec["column"])
    if method == "exact_raw_not_empty":
        return _raw_exact_match_value(column_expr)
    return _normalize_exact_match_value(column_expr)

def _standardize_address(c: F.Column) -> F.Column:
    addr = _clean_text(c)
    addr = F.regexp_replace(addr, r"\bst\b", "street")
    addr = F.regexp_replace(addr, r"\brd\b", "road")
    addr = F.regexp_replace(addr, r"\bave\b", "avenue")
    addr = F.regexp_replace(addr, r"\bav\b", "avenue")
    addr = F.regexp_replace(addr, r"\s+", " ")
    return F.trim(addr)

def _valid_value(c: F.Column, invalid_values: List[str], min_length: int = 1) -> F.Column:
    lowered = F.lower(F.trim(F.coalesce(c.cast("string"), F.lit(""))))
    return (
        lowered.isNotNull()
        & (F.length(lowered) >= F.lit(min_length))
        & (~lowered.isin([v.lower() for v in invalid_values]))
    )

def _levenshtein_similarity_expr(left: F.Column, right: F.Column, invalid_values: List[str], min_length: int) -> F.Column:
    max_len = F.greatest(F.length(left), F.length(right))
    return F.when(
        _valid_value(left, invalid_values, min_length)
        & _valid_value(right, invalid_values, min_length)
        & (max_len > 0),
        F.lit(1.0) - (F.levenshtein(left, right).cast("double") / max_len.cast("double")),
    ).otherwise(F.lit(0.0))

def _token_jaccard_similarity_expr(
    left: F.Column,
    right: F.Column,
    invalid_values: List[str],
    min_length: int,
    min_token_length: int,
) -> F.Column:
    left_tokens = F.array_distinct(F.split(F.trim(F.coalesce(left.cast("string"), F.lit(""))), r"\s+"))
    right_tokens = F.array_distinct(F.split(F.trim(F.coalesce(right.cast("string"), F.lit(""))), r"\s+"))
    left_tokens = F.filter(left_tokens, lambda x: F.length(x) >= F.lit(min_token_length))
    right_tokens = F.filter(right_tokens, lambda x: F.length(x) >= F.lit(min_token_length))
    intersection_size = F.size(F.array_intersect(left_tokens, right_tokens))
    union_size = F.size(F.array_union(left_tokens, right_tokens))
    return F.when(
        _valid_value(left, invalid_values, min_length)
        & _valid_value(right, invalid_values, min_length)
        & (union_size > 0),
        intersection_size.cast("double") / union_size.cast("double"),
    ).otherwise(F.lit(0.0))

def _exact_empty_condition(left: F.Column, right: F.Column) -> F.Column:
    return left.eqNullSafe(right)

def _exact_not_empty_condition(left: F.Column, right: F.Column, invalid_values: List[str], min_length: int) -> F.Column:
    return (left == right) & _valid_value(left, invalid_values, min_length) & _valid_value(right, invalid_values, min_length)

def _ensure_columns(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    result = df
    for c in columns:
        if c not in result.columns:
            result = result.withColumn(c, F.lit(None).cast("string"))
    return result

def collect_required_columns(cfg: Dict[str, Any]) -> List[str]:
    cols = {cfg["rowRegistryKeyColumn"]}
    std = cfg.get("standardization", {})
    for key in ("name_column", "city_column", "state_column", "zip_column"):
        if std.get(key):
            cols.add(std[key])
    cols.update(std.get("address_columns", []))

    for rule in cfg.get("exact_match_rules", []):
        for c in rule.get("columns", []):
            if c["column"] not in {
                "c_name",
                "c_name_exact",
                "c_city",
                "c_state",
                "c_zip",
                "c_zip_exact",
                "c_address",
                "c_name_prefix4",
                "c_name_soundex",
            }:
                cols.add(c["column"])
    return sorted(cols)

def _compared_values_expr(column_names: List[str]) -> F.Column:
    if not column_names:
        return F.lit(None).cast("string")
    return F.concat_ws(
        " || ",
        *[
            (
                F.concat(
                    F.lit(f"{column_name[0] if isinstance(column_name, tuple) else column_name}="),
                    F.coalesce(F.col(column_name[1] if isinstance(column_name, tuple) else column_name).cast("string"), F.lit("")),
                )
            )
            for column_name in column_names
        ],
    )

def _compared_values_from_alias_expr(alias_name: str, column_names: List[str]) -> F.Column:
    if not column_names:
        return F.lit(None).cast("string")
    return F.concat_ws(
        " || ",
        *[
            F.concat(
                F.lit(f"{column_name}="),
                F.coalesce(F.col(f"{alias_name}.{column_name}").cast("string"), F.lit("")),
            )
            for column_name in column_names
        ],
    )

def _score_to_percentage_expr(score_col: F.Column) -> F.Column:
    return F.when(score_col.isNull(), F.lit(None).cast("int")).otherwise(F.round(score_col * F.lit(100)).cast("int"))

def _method_condition(
    method_spec: Dict[str, Any],
    invalid_values: List[str],
    left_alias: str = "a",
    right_alias: str = "b",
) -> F.Column:
    col_name = method_spec["column"]
    method = method_spec.get("method", "exact_not_empty")
    min_length = int(method_spec.get("min_length", 1))
    left = F.col(f"{left_alias}.{col_name}")
    right = F.col(f"{right_alias}.{col_name}")

    if method == "exact_empty":
        return _exact_empty_condition(left, right)

    if method == "exact_not_empty":
        return _exact_not_empty_condition(left, right, invalid_values, min_length)

    if method == "exact_raw_not_empty":
        return _exact_not_empty_condition(
            _raw_exact_match_value(left),
            _raw_exact_match_value(right),
            invalid_values,
            min_length,
        )

    if method == "prefix":
        n = int(method_spec.get("length", 4))
        return (
            (F.substring(left, 1, n) == F.substring(right, 1, n))
            & _valid_value(left, invalid_values, min_length)
            & _valid_value(right, invalid_values, min_length)
        )

    if method == "levenshtein_similarity":
        threshold = float(method_spec["min"])
        return _levenshtein_similarity_expr(left, right, invalid_values, min_length) >= F.lit(threshold)

    if method == "token_jaccard":
        threshold = float(method_spec["min"])
        return _token_jaccard_similarity_expr(
            left,
            right,
            invalid_values,
            min_length,
            int(method_spec.get("min_token_length", 2)),
        ) >= F.lit(threshold)

    if method == "levenshtein_or_token_jaccard":
        threshold = float(method_spec["min"])
        return (
            _levenshtein_similarity_expr(left, right, invalid_values, min_length) >= F.lit(threshold)
        ) | (
            _token_jaccard_similarity_expr(
                left,
                right,
                invalid_values,
                min_length,
                int(method_spec.get("min_token_length", 2)),
            ) >= F.lit(threshold)
        )

    raise ValueError(f"Unsupported match method: {method}")

def _rule_conditions(rule: Dict[str, Any], invalid_values: List[str]) -> F.Column:
    specs = rule.get("conditions", rule.get("columns", []))
    conditions = [_method_condition(spec, invalid_values) for spec in specs]
    if not conditions:
        raise ValueError(f"Rule {rule['rule_name']} has no columns/conditions")

    decision = rule.get("decision", "all").lower()
    if decision == "all":
        return reduce(and_, conditions)
    if decision == "any":
        return reduce(or_, conditions)
    raise ValueError(f"Unsupported decision '{decision}' in rule {rule['rule_name']}")

def _evidence_condition(method_spec: Dict[str, Any], invalid_values: List[str]) -> F.Column:
    col_name = method_spec["column"]
    method = method_spec.get("method", "exact_not_empty")

    if col_name == "c_name" and method == "levenshtein_similarity":
        return F.col("NameLevenshteinSimilarity") >= F.lit(float(method_spec["min"]))

    if col_name == "c_address":
        if method == "levenshtein_similarity":
            return F.col("AddressLevenshteinSimilarity") >= F.lit(float(method_spec["min"]))
        if method == "token_jaccard":
            return F.col("AddressTokenJaccardSimilarity") >= F.lit(float(method_spec["min"]))
        if method == "levenshtein_or_token_jaccard":
            return F.col("AddressBestSimilarity") >= F.lit(float(method_spec["min"]))

    if col_name == "c_zip":
        if method == "exact_empty":
            return F.coalesce(F.col("ZipExactMatch"), F.lit(False))
        if method == "exact_not_empty":
            return F.coalesce(F.col("ZipExactMatch"), F.lit(False))

    if col_name == "c_city" and method == "levenshtein_similarity":
        return F.col("CityLevenshteinSimilarity") >= F.lit(float(method_spec["min"]))

    if col_name == "c_state" and method == "levenshtein_similarity":
        return F.col("StateLevenshteinSimilarity") >= F.lit(float(method_spec["min"]))

    return _method_condition(method_spec, invalid_values)

def _rule_conditions_from_evidence(rule: Dict[str, Any], invalid_values: List[str]) -> F.Column:
    specs = rule.get("conditions", rule.get("columns", []))
    conditions = [_evidence_condition(spec, invalid_values) for spec in specs]
    if not conditions:
        raise ValueError(f"Rule {rule['rule_name']} has no columns/conditions")

    decision = rule.get("decision", "all").lower()
    if decision == "all":
        return reduce(and_, conditions)
    if decision == "any":
        return reduce(or_, conditions)
    raise ValueError(f"Unsupported decision '{decision}' in rule {rule['rule_name']}")
