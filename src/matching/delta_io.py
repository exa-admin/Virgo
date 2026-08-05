"""Delta Lake I/O helpers and materialization of stewardship/debug tables."""
from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from matching.utils import _score_to_percentage_expr, _sql_literal

def _require_table(spark: SparkSession, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(
            f"Required table {table_name} does not exist. Run sql/setup_tables.sql first."
        )

def _require_table_columns(spark: SparkSession, table_name: str, required_columns: List[str]) -> None:
    actual_columns = {field.name for field in spark.table(table_name).schema.fields}
    missing_columns = [column_name for column_name in required_columns if column_name not in actual_columns]
    if missing_columns:
        raise ValueError(
            f"Table {table_name} is missing required columns {missing_columns}. "
            f"If this table was created from an older version of the setup SQL, re-run sql/setup_tables.sql (or migrate schema)."
        )

def _overwrite_delta_slice(
    df: DataFrame,
    table_name: str,
    replace_where: str,
    read_where: Optional[str] = None,
    merge_schema: bool = False,
) -> DataFrame:
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
    )
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(table_name)
    return df.sparkSession.table(table_name).where(read_where or replace_where)

def _clear_country_slice(spark: SparkSession, table_name: str, country_code: str) -> None:
    spark.sql(
        f"DELETE FROM {table_name} WHERE CountryCode = '{_sql_literal(country_code)}'"
    )

def _materialize_matching_state(
    record_ids_df: DataFrame,
    table_name: str,
    country_code: str,
    state_name: str,
    state_type: str,
) -> DataFrame:
    country_code_sql = _sql_literal(country_code)
    state_name_sql = _sql_literal(state_name)
    state_type_sql = _sql_literal(state_type)
    state_slice = record_ids_df.select(
        F.lit(country_code).alias("CountryCode"),
        F.lit(state_name).alias("StateName"),
        F.lit(state_type).alias("StateType"),
        F.col("record_id").cast("long").alias("record_id"),
    )
    replace_where = (
        f"CountryCode = '{country_code_sql}' "
        f"AND StateName = '{state_name_sql}' "
        f"AND StateType = '{state_type_sql}'"
    )
    return _overwrite_delta_slice(
        state_slice,
        table_name,
        replace_where,
    ).select("record_id")

def _materialize_rule_results(
    match_links_df: DataFrame,
    reference_df: DataFrame,
    row_registry_key_column: str,
    table_name: str,
    country_code: str,
    rule_type: str,
    rule_stage_name: str,
    rule_execution_order: int,
) -> DataFrame:
    country_code_sql = _sql_literal(country_code)
    rule_stage_name_sql = _sql_literal(rule_stage_name)
    left_context = reference_df.select(
        F.col("record_id").alias("src"),
        F.col(row_registry_key_column).cast("string").alias("SrcOperatorConcatId"),
    )
    right_context = reference_df.select(
        F.col("record_id").alias("dst"),
        F.col(row_registry_key_column).cast("string").alias("DstOperatorConcatId"),
    )

    rule_results_slice = (
        match_links_df.join(left_context, "src", "left")
        .join(right_context, "dst", "left")
        .select(
        F.lit(country_code).alias("CountryCode"),
        F.lit(rule_type).alias("RuleType"),
        F.lit(rule_stage_name).alias("RuleStageName"),
        F.lit(int(rule_execution_order)).alias("RuleExecutionOrder"),
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
        )
    )
    replace_where = (
        f"CountryCode = '{country_code_sql}' "
        f"AND RuleStageName = '{rule_stage_name_sql}'"
    )
    return _overwrite_delta_slice(
        rule_results_slice,
        table_name,
        replace_where,
        merge_schema=True,
    ).select(
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
    )

def _materialize_rule_evaluations(
    evaluation_df: DataFrame,
    table_name: str,
    country_code: str,
    rule_type: str,
    rule_stage_name: str,
    rule_execution_order: int,
) -> DataFrame:
    country_code_sql = _sql_literal(country_code)
    rule_stage_name_sql = _sql_literal(rule_stage_name)
    evaluation_slice = evaluation_df.select(
        F.lit(country_code).alias("CountryCode"),
        F.lit(rule_type).alias("RuleType"),
        F.lit(rule_stage_name).alias("RuleStageName"),
        F.lit(int(rule_execution_order)).alias("RuleExecutionOrder"),
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
        _score_to_percentage_expr(F.col("NameLevenshteinSimilarity")).alias("NameLevenshteinSimilarity"),
        _score_to_percentage_expr(F.col("AddressLevenshteinSimilarity")).alias("AddressLevenshteinSimilarity"),
        _score_to_percentage_expr(F.col("AddressTokenJaccardSimilarity")).alias("AddressTokenJaccardSimilarity"),
        _score_to_percentage_expr(F.col("AddressBestSimilarity")).alias("AddressBestSimilarity"),
        _score_to_percentage_expr(F.col("CityLevenshteinSimilarity")).alias("CityLevenshteinSimilarity"),
        _score_to_percentage_expr(F.col("StateLevenshteinSimilarity")).alias("StateLevenshteinSimilarity"),
        "ZipExactMatch",
        "NameConditionPassed",
        "AddressConditionPassed",
        "CityConditionPassed",
        "StateConditionPassed",
        "ZipConditionPassed",
        "IsMatched",
    )
    replace_where = (
        f"CountryCode = '{country_code_sql}' "
        f"AND RuleStageName = '{rule_stage_name_sql}'"
    )
    return _overwrite_delta_slice(
        evaluation_slice,
        table_name,
        replace_where,
        merge_schema=True,
    )

def _materialize_component_labels(
    labels_df: DataFrame,
    table_name: str,
    country_code: str,
    label_stage_name: str,
    iteration_number: int,
) -> DataFrame:
    country_code_sql = _sql_literal(country_code)
    label_stage_name_sql = _sql_literal(label_stage_name)
    label_slice = labels_df.select(
        F.lit(country_code).alias("CountryCode"),
        F.lit(label_stage_name).alias("LabelStageName"),
        F.lit(int(iteration_number)).alias("IterationNumber"),
        F.col("record_id").cast("long").alias("record_id"),
        F.col("golden_id").cast("long").alias("golden_id"),
    )
    replace_where = (
        f"CountryCode = '{country_code_sql}' "
        f"AND LabelStageName = '{label_stage_name_sql}'"
    )
    return _overwrite_delta_slice(
        label_slice,
        table_name,
        replace_where,
    ).select("record_id", "golden_id")
