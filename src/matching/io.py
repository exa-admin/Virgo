"""Reading the source population and writing Delta slices.

Source reading is isolated here so a deployment can swap Delta tables for CSV/Parquet
files without touching engine code. Each spec in the config is declarative::

    {"format": "delta",   "table": "catalog.schema.view"}
    {"format": "csv",     "path": "/Volumes/.../operator/", "options": {"header": "true"}}
    {"format": "parquet", "path": "/Volumes/.../operator/"}

Every write is a country-scoped ``replaceWhere`` slice, so re-running one country never
touches another.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from matching.config import GOLDEN_MASTER_KEY_PREFIX
from matching.expressions import collect_required_columns, score_to_percentage, sql_literal

SUPPORTED_FORMATS = {"delta", "csv", "parquet"}

# Without these the registry MERGE / source-golden grouping cannot run. Match columns are
# only warned about, because the pipeline null-fills the ones a source does not have.
OPERATOR_REQUIRED_COLUMNS = ["OperatorConcatId", "CountryCode", "GoldenRecordId", "BDLLoadTimestamp"]
GOLDEN_REQUIRED_COLUMNS = ["CountryCode", "GoldenRecordId"]


# --------------------------------------------------------------------------- reading


def _read_spec(spark: SparkSession, spec: Dict[str, Any], label: str) -> DataFrame:
    if not spec:
        raise ValueError(f"{label}: source spec is empty")
    fmt = str(spec.get("format", "delta")).lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"{label}: unsupported source format '{fmt}' (use one of {sorted(SUPPORTED_FORMATS)})")

    table, path = spec.get("table"), spec.get("path")
    if fmt == "delta" and table:
        return spark.table(table)
    if not path:
        raise ValueError(f"{label}: spec needs 'table' (delta) or 'path' ({fmt})")

    reader = spark.read.format(fmt)
    if fmt == "csv":
        # Header row + string columns by default so downstream casts stay explicit.
        reader = reader.option("header", "true").option("inferSchema", "false")
    for key, value in dict(spec.get("options", {})).items():
        reader = reader.option(key, value)
    return reader.load(path)


def _validate_columns(df: DataFrame, cfg: Dict[str, Any], label: str, required: List[str]) -> None:
    present = set(df.columns)
    missing = [c for c in required if c not in present]
    if missing:
        raise ValueError(f"{label} is missing required columns {missing}. Available: {sorted(present)[:25]}...")

    missing_match = sorted(set(collect_required_columns(cfg)) - present)
    if missing_match:
        print(
            f"  -> WARNING: {label} is missing match/standardization columns {missing_match}; "
            "they will be treated as NULL (rules using them cannot match)."
        )


def read_source_population(spark: SparkSession, cfg: Dict[str, Any]) -> DataFrame:
    """The operator population fed to the match engine, for one country.

    Two inputs (``source`` / ``golden_source`` in the config):

    * **operator** — the population to match, one row per ``OperatorConcatId``, each
      carrying its Informatica ``GoldenRecordId`` when it has been matched before.
    * **golden** — the surviving master per ``GoldenRecordId`` (no ``OperatorConcatId``).

    Returns the operator feed plus any golden master whose ``GoldenRecordId`` is absent
    from it, so an Informatica group is never lost just because its master row is not in
    the current operator slice. Backfilled rows get a deterministic synthetic key
    (``GRID_<GoldenRecordId>``) and so stay stable across runs.
    """
    operator = _read_spec(spark, cfg["source"], "operator source")
    _validate_columns(operator, cfg, "operator source", OPERATOR_REQUIRED_COLUMNS)
    operator = operator.filter(cfg["filter_condition"])

    if not cfg.get("golden_source"):
        return operator

    golden = _read_spec(spark, cfg["golden_source"], "golden source")
    _validate_columns(golden, cfg, "golden source", GOLDEN_REQUIRED_COLUMNS)
    golden = golden.filter(cfg["filter_condition"])

    known_ids = (
        operator.select(F.trim(F.col("GoldenRecordId").cast("string")).alias("_grid"))
        .filter(F.col("_grid").isNotNull() & (F.col("_grid") != F.lit("")))
        .distinct()
    )
    missing_masters = (
        golden.withColumn("_grid", F.trim(F.col("GoldenRecordId").cast("string")))
        .filter(F.col("_grid").isNotNull() & (F.col("_grid") != F.lit("")))
        .join(known_ids, "_grid", "left_anti")
        .withColumn("OperatorConcatId", F.concat(F.lit(GOLDEN_MASTER_KEY_PREFIX), F.col("_grid")))
        .withColumn("IsGoldenRecordFlag", F.lit("TRUE"))
        .drop("_grid")
    )

    # Align to the operator schema (extra golden columns dropped, absent ones NULL) so
    # unionByName is exact.
    present = set(missing_masters.columns)
    aligned = missing_masters.select(
        *[F.col(c) if c in present else F.lit(None).cast("string").alias(c) for c in operator.columns]
    )
    return operator.unionByName(aligned)


# --------------------------------------------------------------------------- writing


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
    """Persist one rule stage's accepted edges to MDMRuleResults, with operator keys attached."""
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
