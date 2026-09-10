"""Delta writes — every one a country slice.

``replaceWhere`` / ``DELETE WHERE CountryCode`` throughout, so re-running one country
never touches another. Datasets written here are addressed by table name, so they must
stay Delta tables.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from matching.helpers import score_to_percentage, sql_literal


def require_table(spark: SparkSession, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Required table {table_name} does not exist. Run sql/setup_tables.sql first.")


def require_columns(spark: SparkSession, table_name: str, required_columns: List[str]) -> None:
    actual = {field.name for field in spark.table(table_name).schema.fields}
    missing = [c for c in required_columns if c not in actual]
    if missing:
        raise ValueError(
            f"Table {table_name} is missing required columns {missing}. "
            "If it was created from an older setup SQL, re-run sql/setup_tables.sql."
        )


def require_tables(spark: SparkSession, tables: Dict[str, List[str]]) -> None:
    for table_name, columns in tables.items():
        require_table(spark, table_name)
        require_columns(spark, table_name, columns)


def overwrite_slice(
    df: DataFrame,
    table_name: str,
    replace_where: str,
    merge_schema: bool = False,
) -> DataFrame:
    """Replace one slice of a Delta table and return the persisted rows back."""
    writer = df.write.format("delta").mode("overwrite").option("replaceWhere", replace_where)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(table_name)
    return df.sparkSession.table(table_name).where(replace_where)


def clear_country_slice(spark: SparkSession, table_name: str, country_code: str) -> None:
    spark.sql(f"DELETE FROM {table_name} WHERE CountryCode = '{sql_literal(country_code)}'")


def save_matching_state(
    record_ids: DataFrame,
    table_name: str,
    country_code: str,
    state_name: str,
    state_type: str,
) -> DataFrame:
    """Checkpoint an intermediate record-id set to Delta and read it back (truncates the plan)."""
    slice_df = record_ids.select(
        F.lit(country_code).alias("CountryCode"),
        F.lit(state_name).alias("StateName"),
        F.lit(state_type).alias("StateType"),
        F.col("record_id").cast("long").alias("record_id"),
    )
    replace_where = (
        f"CountryCode = '{sql_literal(country_code)}' "
        f"AND StateName = '{sql_literal(state_name)}' "
        f"AND StateType = '{sql_literal(state_type)}'"
    )
    return overwrite_slice(slice_df, table_name, replace_where).select("record_id")


RULE_RESULT_COLUMNS = [
    "src",
    "dst",
    "SrcOperatorConcatId",
    "DstOperatorConcatId",
    "match_rule",
    "rule_priority",
    "edge_type",
    "block_name",
    "match_key",
    "src_is_rule_subject",
    "dst_is_rule_subject",
]


def save_rule_results(
    match_links: DataFrame,
    reference: DataFrame,
    cfg: Dict[str, Any],
    country_code: str,
    rule_type: str,
    rule_stage_name: str,
    rule_execution_order: int,
) -> DataFrame:
    """Persist one rule stage's accepted edges to mdm_rule_results, with operator keys attached."""
    key_column = cfg["rowRegistryKeyColumn"]
    src_keys = reference.select(F.col("record_id").alias("src"), F.col(key_column).cast("string").alias("SrcOperatorConcatId"))
    dst_keys = reference.select(F.col("record_id").alias("dst"), F.col(key_column).cast("string").alias("DstOperatorConcatId"))

    slice_df = (
        match_links.join(src_keys, "src", "left")
        .join(dst_keys, "dst", "left")
        .select(
            F.lit(country_code).alias("CountryCode"),
            F.lit(rule_type).alias("RuleType"),
            F.lit(rule_stage_name).alias("RuleStageName"),
            F.lit(int(rule_execution_order)).alias("RuleExecutionOrder"),
            *RULE_RESULT_COLUMNS,
        )
    )
    replace_where = (
        f"CountryCode = '{sql_literal(country_code)}' AND RuleStageName = '{sql_literal(rule_stage_name)}'"
    )
    return overwrite_slice(slice_df, cfg["ruleResultsTable"], replace_where, merge_schema=True).select(*RULE_RESULT_COLUMNS)


# Similarity scores are stored as 0-100 ints; everything else carries through as-is.
SIMILARITY_COLUMNS = [
    "NameLevenshteinSimilarity",
    "AddressLevenshteinSimilarity",
    "AddressTokenJaccardSimilarity",
    "AddressBestSimilarity",
    "CityLevenshteinSimilarity",
    "StateLevenshteinSimilarity",
]
EVALUATION_COLUMNS = [
    "src",
    "dst",
    "SrcOperatorConcatId",
    "DstOperatorConcatId",
    "SrcName",
    "DstName",
    "SrcAddress",
    "DstAddress",
    "SrcCity",
    "DstCity",
    "SrcState",
    "DstState",
    "SrcZip",
    "DstZip",
    "SrcComparedValues",
    "DstComparedValues",
    "match_rule",
    "rule_priority",
    "edge_type",
    "block_name",
    "match_key",
    "src_is_rule_subject",
    "dst_is_rule_subject",
    *SIMILARITY_COLUMNS,
    "ZipExactMatch",
    "NameConditionPassed",
    "AddressConditionPassed",
    "CityConditionPassed",
    "StateConditionPassed",
    "ZipConditionPassed",
    "IsMatched",
]


def save_rule_evaluations(
    evaluations: DataFrame,
    table_name: str,
    country_code: str,
    rule_stage_name: str,
    rule_execution_order: int,
) -> None:
    """Persist every fuzzy candidate pair and its evidence (matched or not) for stewardship."""
    scored = evaluations
    for column in SIMILARITY_COLUMNS:
        scored = scored.withColumn(column, score_to_percentage(F.col(column)))

    slice_df = scored.select(
        F.lit(country_code).alias("CountryCode"),
        F.lit("fuzzy").alias("RuleType"),
        F.lit(rule_stage_name).alias("RuleStageName"),
        F.lit(int(rule_execution_order)).alias("RuleExecutionOrder"),
        *EVALUATION_COLUMNS,
    )
    replace_where = (
        f"CountryCode = '{sql_literal(country_code)}' AND RuleStageName = '{sql_literal(rule_stage_name)}'"
    )
    overwrite_slice(slice_df, table_name, replace_where, merge_schema=True)


def save_component_labels(
    labels: DataFrame,
    table_name: str,
    country_code: str,
    label_stage_name: str,
    iteration_number: int,
) -> DataFrame:
    """Checkpoint one label-propagation iteration to Delta and read it back."""
    slice_df = labels.select(
        F.lit(country_code).alias("CountryCode"),
        F.lit(label_stage_name).alias("LabelStageName"),
        F.lit(int(iteration_number)).alias("IterationNumber"),
        F.col("record_id").cast("long").alias("record_id"),
        F.col("golden_id").cast("long").alias("golden_id"),
    )
    replace_where = (
        f"CountryCode = '{sql_literal(country_code)}' AND LabelStageName = '{sql_literal(label_stage_name)}'"
    )
    return overwrite_slice(slice_df, table_name, replace_where).select("record_id", "golden_id")
