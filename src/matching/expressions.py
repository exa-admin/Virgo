"""Spark column expressions used by the match rules, plus small shared helpers.

Everything here is a Column/SQL expression — no Python UDFs, so the whole engine stays
executable inside Spark.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from functools import reduce
from operator import and_, or_
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Columns of a match link as persisted to MDMMatchLinks / MDMRuleResults.
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
# Evidence columns a link carries in flight (dropped when persisted to MDMMatchLinks).
MATCH_LINK_EVIDENCE_COLUMNS = [
    "NameLevenshteinSimilarity",
    "AddressLevenshteinSimilarity",
    "AddressTokenJaccardSimilarity",
    "AddressBestSimilarity",
    "ZipExactMatch",
]
MATCH_LINK_COLUMNS = MATCH_LINK_BASE_COLUMNS + MATCH_LINK_EVIDENCE_COLUMNS


def no_evidence() -> List[F.Column]:
    """Null evidence columns, so exact and group links share the fuzzy link schema.

    Built on call, never at import: a Column needs an active Spark session, and these
    modules get imported before one exists (e.g. by the ``mdm-match`` wheel task).
    """
    return [
        F.lit(None).cast("double").alias("NameLevenshteinSimilarity"),
        F.lit(None).cast("double").alias("AddressLevenshteinSimilarity"),
        F.lit(None).cast("double").alias("AddressTokenJaccardSimilarity"),
        F.lit(None).cast("double").alias("AddressBestSimilarity"),
        F.lit(None).cast("boolean").alias("ZipExactMatch"),
    ]


# Standardized columns the engine derives itself (see pipeline.standardize_input); a rule
# naming one of these does not need it read from the source.
DERIVED_COLUMNS = {
    "c_name",
    "c_name_exact",
    "c_city",
    "c_state",
    "c_zip",
    "c_zip_exact",
    "c_address",
    "c_name_prefix4",
    "c_name_soundex",
}


# --------------------------------------------------------------------------- helpers


@contextmanager
def timed(step_name: str):
    started = time.time()
    print(f"[{step_name}] STARTED")
    try:
        yield
    finally:
        print(f"[{step_name}] FINISHED in {time.time() - started:0.2f}s")


def sql_literal(value: str) -> str:
    """Escape a value for embedding in a SQL string literal."""
    return value.replace("'", "''")


def sql_or(filters: Optional[Iterable[str]]) -> Optional[str]:
    """OR a list of SQL predicates together, or None if there are none."""
    cleaned = [f"({f})" for f in (filters or []) if f and f.strip()]
    return " OR ".join(cleaned) if cleaned else None


def is_empty(df: DataFrame) -> bool:
    is_empty_method = getattr(df, "isEmpty", None)  # Spark >= 3.3
    return bool(is_empty_method()) if callable(is_empty_method) else df.limit(1).count() == 0


def require_dataframe_columns(df: DataFrame, required_columns: List[str], label: str) -> None:
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns {missing}")


def ensure_columns(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Null-fill any of ``columns`` the DataFrame does not have."""
    for column in columns:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast("string"))
    return df


def union_all(dfs: List[DataFrame]) -> DataFrame:
    if not dfs:
        raise ValueError("No DataFrames to union")
    return reduce(lambda a, b: a.unionByName(b), dfs)


def empty_match_links(spark: SparkSession) -> DataFrame:
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


def empty_record_ids(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], T.StructType([T.StructField("record_id", T.LongType(), False)]))


def matched_record_ids(match_links: DataFrame) -> DataFrame:
    """Distinct record ids appearing on either end of a link."""
    return (
        match_links.select(F.col("src").alias("record_id"))
        .unionByName(match_links.select(F.col("dst").alias("record_id")))
        .dropDuplicates(["record_id"])
    )


def dedupe_match_links(match_links: DataFrame) -> DataFrame:
    """Collapse repeated (src, dst, rule) edges, merging their blocks, keys and evidence."""
    return (
        match_links.groupBy("src", "dst", "match_rule", "rule_priority", "edge_type")
        .agg(
            F.concat_ws(",", F.sort_array(F.collect_set("block_name"))).alias("block_name"),
            F.concat_ws(",", F.sort_array(F.collect_set("match_key"))).alias("match_key"),
            F.max(F.when(F.col("src_is_rule_subject"), F.lit(1)).otherwise(F.lit(0))).alias("_src_subject"),
            F.max(F.when(F.col("dst_is_rule_subject"), F.lit(1)).otherwise(F.lit(0))).alias("_dst_subject"),
            F.max("NameLevenshteinSimilarity").alias("NameLevenshteinSimilarity"),
            F.max("AddressLevenshteinSimilarity").alias("AddressLevenshteinSimilarity"),
            F.max("AddressTokenJaccardSimilarity").alias("AddressTokenJaccardSimilarity"),
            F.max("AddressBestSimilarity").alias("AddressBestSimilarity"),
            F.max(F.when(F.col("ZipExactMatch"), F.lit(1)).otherwise(F.lit(0))).alias("_zip_match"),
            F.max(F.when(F.col("ZipExactMatch").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("_zip_known"),
        )
        .select(
            "src",
            "dst",
            "match_rule",
            "rule_priority",
            "edge_type",
            "block_name",
            "match_key",
            (F.col("_src_subject") == F.lit(1)).alias("src_is_rule_subject"),
            (F.col("_dst_subject") == F.lit(1)).alias("dst_is_rule_subject"),
            "NameLevenshteinSimilarity",
            "AddressLevenshteinSimilarity",
            "AddressTokenJaccardSimilarity",
            "AddressBestSimilarity",
            F.when(F.col("_zip_known") == F.lit(0), F.lit(None).cast("boolean"))
            .otherwise(F.col("_zip_match") == F.lit(1))
            .alias("ZipExactMatch"),
        )
    )


def apply_exclusion(df: DataFrame, filters: Optional[Iterable[str]], label: str) -> DataFrame:
    where_clause = sql_or(filters)
    if not where_clause:
        return df
    print(f"  -> Applying {label}: NOT ({where_clause})")
    return df.filter(f"NOT ({where_clause})")


def collect_required_columns(cfg: Dict[str, Any]) -> List[str]:
    """Source columns the config needs read: standardization inputs + non-derived rule columns."""
    columns = {cfg.get("rowRegistryKeyColumn", "OperatorConcatId")}
    std = cfg.get("standardization", {})
    columns.update(std[key] for key in ("name_column", "city_column", "state_column", "zip_column") if std.get(key))
    columns.update(std.get("address_columns", []))
    for rule in cfg.get("exact_match_rules", []):
        columns.update(c["column"] for c in rule.get("columns", []) if c["column"] not in DERIVED_COLUMNS)
    return sorted(columns)


# --------------------------------------------------------------------- text cleaning


def clean_text(column: F.Column) -> F.Column:
    """Lowercase, strip punctuation to spaces, collapse whitespace."""
    text = F.lower(F.coalesce(column.cast("string"), F.lit("")))
    text = F.regexp_replace(text, r"[^\p{L}0-9 ]", " ")
    return F.trim(F.regexp_replace(text, r"\s+", " "))


def compact_key(column: F.Column) -> F.Column:
    """Lowercase with every non-alphanumeric character removed."""
    return F.regexp_replace(F.lower(F.coalesce(column.cast("string"), F.lit(""))), r"[^\p{L}0-9]", "")


def standardize_address(column: F.Column) -> F.Column:
    """clean_text plus the common street-type abbreviations spelled out."""
    address = clean_text(column)
    for abbreviation, full in (("st", "street"), ("rd", "road"), ("ave", "avenue"), ("av", "avenue")):
        address = F.regexp_replace(address, rf"\b{abbreviation}\b", full)
    return F.trim(F.regexp_replace(address, r"\s+", " "))


def _raw_value(column: F.Column) -> F.Column:
    return F.trim(F.coalesce(column.cast("string"), F.lit("")))


def exact_match_key(spec: Dict[str, Any]) -> F.Column:
    """The value an exact rule blocks on: raw for ``exact_raw_not_empty``, else cleaned."""
    column = F.col(spec["column"])
    if spec.get("method", "exact_not_empty") == "exact_raw_not_empty":
        return _raw_value(column)
    return F.regexp_replace(clean_text(column), r"^0+", "")


# ----------------------------------------------------------------------- comparisons


def valid_value(column: F.Column, invalid_values: List[str], min_length: int = 1) -> F.Column:
    """True when the value is long enough and not one of the configured junk values."""
    lowered = F.lower(F.trim(F.coalesce(column.cast("string"), F.lit(""))))
    return (
        lowered.isNotNull()
        & (F.length(lowered) >= F.lit(min_length))
        & (~lowered.isin([v.lower() for v in invalid_values]))
    )


def levenshtein_similarity(left: F.Column, right: F.Column, invalid_values: List[str], min_length: int) -> F.Column:
    """1 - (edit distance / longer length), or 0.0 when either side is invalid."""
    max_len = F.greatest(F.length(left), F.length(right))
    return F.when(
        valid_value(left, invalid_values, min_length) & valid_value(right, invalid_values, min_length) & (max_len > 0),
        F.lit(1.0) - (F.levenshtein(left, right).cast("double") / max_len.cast("double")),
    ).otherwise(F.lit(0.0))


def token_jaccard_similarity(
    left: F.Column,
    right: F.Column,
    invalid_values: List[str],
    min_length: int,
    min_token_length: int,
) -> F.Column:
    """|shared tokens| / |all tokens|, ignoring tokens shorter than ``min_token_length``."""
    def tokens(column: F.Column) -> F.Column:
        split = F.array_distinct(F.split(F.trim(F.coalesce(column.cast("string"), F.lit(""))), r"\s+"))
        return F.filter(split, lambda token: F.length(token) >= F.lit(min_token_length))

    left_tokens, right_tokens = tokens(left), tokens(right)
    union_size = F.size(F.array_union(left_tokens, right_tokens))
    return F.when(
        valid_value(left, invalid_values, min_length) & valid_value(right, invalid_values, min_length) & (union_size > 0),
        F.size(F.array_intersect(left_tokens, right_tokens)).cast("double") / union_size.cast("double"),
    ).otherwise(F.lit(0.0))


def exact_empty_match(left: F.Column, right: F.Column) -> F.Column:
    """Equal, counting two NULLs as equal."""
    return left.eqNullSafe(right)


def exact_not_empty_match(left: F.Column, right: F.Column, invalid_values: List[str], min_length: int) -> F.Column:
    """Equal and both sides are real values."""
    return (left == right) & valid_value(left, invalid_values, min_length) & valid_value(right, invalid_values, min_length)


def method_condition(
    spec: Dict[str, Any],
    invalid_values: List[str],
    left_alias: str = "a",
    right_alias: str = "b",
) -> F.Column:
    """Build the comparison a single rule condition describes."""
    column_name = spec["column"]
    method = spec.get("method", "exact_not_empty")
    min_length = int(spec.get("min_length", 1))
    left = F.col(f"{left_alias}.{column_name}")
    right = F.col(f"{right_alias}.{column_name}")

    if method == "exact_empty":
        return exact_empty_match(left, right)
    if method == "exact_not_empty":
        return exact_not_empty_match(left, right, invalid_values, min_length)
    if method == "exact_raw_not_empty":
        return exact_not_empty_match(_raw_value(left), _raw_value(right), invalid_values, min_length)
    if method == "prefix":
        length = int(spec.get("length", 4))
        return (
            (F.substring(left, 1, length) == F.substring(right, 1, length))
            & valid_value(left, invalid_values, min_length)
            & valid_value(right, invalid_values, min_length)
        )

    if method in {"levenshtein_similarity", "token_jaccard", "levenshtein_or_token_jaccard"}:
        threshold = F.lit(float(spec["min"]))
        min_token_length = int(spec.get("min_token_length", 2))
        levenshtein = levenshtein_similarity(left, right, invalid_values, min_length) >= threshold
        jaccard = token_jaccard_similarity(left, right, invalid_values, min_length, min_token_length) >= threshold
        if method == "levenshtein_similarity":
            return levenshtein
        if method == "token_jaccard":
            return jaccard
        return levenshtein | jaccard

    raise ValueError(f"Unsupported match method: {method}")


# Similarity columns the fuzzy pass computes once per pair, so a rule condition can be
# re-checked against the stored evidence instead of recomputing the whole expression.
_EVIDENCE_COLUMNS = {
    ("c_name", "levenshtein_similarity"): "NameLevenshteinSimilarity",
    ("c_address", "levenshtein_similarity"): "AddressLevenshteinSimilarity",
    ("c_address", "token_jaccard"): "AddressTokenJaccardSimilarity",
    ("c_address", "levenshtein_or_token_jaccard"): "AddressBestSimilarity",
    ("c_city", "levenshtein_similarity"): "CityLevenshteinSimilarity",
    ("c_state", "levenshtein_similarity"): "StateLevenshteinSimilarity",
}


def evidence_condition(spec: Dict[str, Any], invalid_values: List[str]) -> F.Column:
    """A rule condition re-expressed against the already-computed evidence columns."""
    column_name = spec["column"]
    method = spec.get("method", "exact_not_empty")

    evidence_column = _EVIDENCE_COLUMNS.get((column_name, method))
    if evidence_column is not None:
        return F.col(evidence_column) >= F.lit(float(spec["min"]))
    if column_name == "c_zip" and method in {"exact_empty", "exact_not_empty"}:
        return F.coalesce(F.col("ZipExactMatch"), F.lit(False))
    return method_condition(spec, invalid_values)


def rule_matches(rule: Dict[str, Any], invalid_values: List[str]) -> F.Column:
    """Combine a fuzzy rule's conditions per its ``decision`` (all / any)."""
    specs = rule.get("conditions", rule.get("columns", []))
    if not specs:
        raise ValueError(f"Rule {rule['rule_name']} has no columns/conditions")

    conditions = [evidence_condition(spec, invalid_values) for spec in specs]
    decision = rule.get("decision", "all").lower()
    if decision == "all":
        return reduce(and_, conditions)
    if decision == "any":
        return reduce(or_, conditions)
    raise ValueError(f"Unsupported decision '{decision}' in rule {rule['rule_name']}")


def compared_values(alias_name: str, column_names: List[str]) -> F.Column:
    """``col=value || col=value`` audit string for one side of a candidate pair."""
    if not column_names:
        return F.lit(None).cast("string")
    return F.concat_ws(
        " || ",
        *[
            F.concat(F.lit(f"{name}="), F.coalesce(F.col(f"{alias_name}.{name}").cast("string"), F.lit("")))
            for name in column_names
        ],
    )


def score_to_percentage(score: F.Column) -> F.Column:
    """0.0-1.0 similarity as a 0-100 int for the stewardship tables."""
    return F.when(score.isNull(), F.lit(None).cast("int")).otherwise(F.round(score * F.lit(100)).cast("int"))
