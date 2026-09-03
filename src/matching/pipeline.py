"""Country-level MDM match orchestration: enrich, registry, match, golden IDs."""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from matching.components import connected_components_native
from matching.config import (
    ENRICHMENT_COLUMN_MAPPINGS,
    _runtime_cfg,
    load_all_country_configs,
)
from matching.delta_io import (
    _clear_country_slice,
    _materialize_matching_state,
    _overwrite_delta_slice,
    _require_table,
    _require_table_columns,
)
from matching.golden_ids import (
    PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN,
    PREVIOUS_GOLDEN_ID_COLUMN,
    PREVIOUS_GOLDEN_ID_SOURCE_COLUMN,
    _build_final_golden_ids,
    append_golden_id_history,
    build_golden_id_history,
    collect_record_match_rules,
    persist_golden_id_assignments,
    validate_golden_id_space,
)
from matching.match_pipeline import run_match_pipeline
from matching.standardize import standardize_input
from matching.utils import (
    _ensure_columns,
    _is_empty,
    _matched_record_ids_from_links,
    _require_dataframe_columns,
    _sql_literal,
    _sql_or,
    _valid_value,
    collect_required_columns,
    timed,
)

ROW_REGISTRY_REQUIRED_COLUMNS = [
    "CountryCode",
    "MDMRowId",
    "OperatorConcatId",
    "SourceGoldenRecordId",
    "GoldenIDCreatedDate",
    "MDMGoldenId",
    "MDMGoldenIdSource",
    "MDMGoldenIdAssignedDate",
    "DateCreated",
    "DateUpdated",
]

def _ensure_row_registry(source: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """MERGE source keys into MDMRowRegistry and attach MDMRowId + golden id context.

    Informatica id policy: ``SourceGoldenRecordId`` is only written when the source
    carries a non-null value. A non-null id is never downgraded to NULL (Informatica
    stops populating it once a country migrates to this engine), but a non-null id may
    change to another non-null id (Informatica re-merged while it still owns the country).
    """
    spark = source.sparkSession
    row_registry_table = cfg["rowRegistryTable"]
    row_registry_key_column = cfg["rowRegistryKeyColumn"]
    invalid_values = cfg.get("invalid_values", [])
    _require_table(spark, row_registry_table)
    _require_table_columns(spark, row_registry_table, ROW_REGISTRY_REQUIRED_COLUMNS)

    _require_dataframe_columns(source, [row_registry_key_column, "CountryCode", "GoldenRecordId", "BDLLoadTimestamp"], "Source DataFrame")

    # Deterministic one-row-per-key: prefer a row that carries an Informatica id, then the
    # earliest load timestamp, then the smallest id (dropDuplicates would pick arbitrarily).
    key_window = Window.partitionBy("CountryCode", "OperatorConcatId").orderBy(
        F.col("SourceGoldenRecordId").asc_nulls_last(),
        F.col("GoldenIDCreatedDate").asc_nulls_last(),
    )
    registry_source = (
        source.select(
            F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))).alias("CountryCode"),
            F.trim(F.coalesce(F.col(row_registry_key_column).cast("string"), F.lit(""))).alias("OperatorConcatId"),
            F.col("GoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
            F.col("BDLLoadTimestamp").cast("timestamp").alias("GoldenIDCreatedDate"),
        )
        .filter(F.col("CountryCode") != "")
        .filter(_valid_value(F.col("OperatorConcatId"), invalid_values, 1))
        .withColumn("_key_rank", F.row_number().over(key_window))
        .filter(F.col("_key_rank") == F.lit(1))
        .drop("_key_rank")
    )

    merge_view = "TmpMDMRowRegistrySource"
    registry_source.createOrReplaceTempView(merge_view)
    spark.sql(
        f"""
        MERGE INTO {row_registry_table} AS target
        USING {merge_view} AS source
        ON target.CountryCode = source.CountryCode
           AND target.OperatorConcatId = source.OperatorConcatId
        WHEN MATCHED AND source.SourceGoldenRecordId IS NOT NULL
             AND (target.SourceGoldenRecordId IS NULL
                  OR target.SourceGoldenRecordId <> source.SourceGoldenRecordId) THEN UPDATE SET
          target.SourceGoldenRecordId = source.SourceGoldenRecordId,
          target.GoldenIDCreatedDate = source.GoldenIDCreatedDate,
          target.DateUpdated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          CountryCode,
          OperatorConcatId,
          SourceGoldenRecordId,
          GoldenIDCreatedDate,
          DateCreated,
          DateUpdated
        )
        VALUES (
          source.CountryCode,
          source.OperatorConcatId,
          source.SourceGoldenRecordId,
          source.GoldenIDCreatedDate,
          current_timestamp(),
          current_timestamp()
        )
        """
    )

    # Registry values win over the raw source: SourceGoldenRecordId keeps the last known
    # Informatica id even when the source has gone NULL for a migrated country.
    registry = spark.table(row_registry_table).select(
        "CountryCode",
        F.col("OperatorConcatId").alias(row_registry_key_column),
        "MDMRowId",
        "SourceGoldenRecordId",
        "GoldenIDCreatedDate",
        F.col("MDMGoldenId").alias(PREVIOUS_GOLDEN_ID_COLUMN),
        F.col("MDMGoldenIdSource").alias(PREVIOUS_GOLDEN_ID_SOURCE_COLUMN),
        F.col("MDMGoldenIdAssignedDate").alias(PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN),
    )
    enriched = (
        source.withColumn("CountryCode", F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))))
        .withColumn(row_registry_key_column, F.trim(F.coalesce(F.col(row_registry_key_column).cast("string"), F.lit(""))))
        .join(registry, ["CountryCode", row_registry_key_column], "left")
    )
    # Callers validate MDMRowId presence after persisting (avoids a second full source scan here).
    return enriched

def _load_match_exclusions(processed_df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    spark = processed_df.sparkSession
    match_exclusions_table = cfg["matchExclusionsTable"]
    row_registry_key_column = cfg["rowRegistryKeyColumn"]
    _require_table(spark, match_exclusions_table)
    _require_table_columns(
        spark,
        match_exclusions_table,
        ["CountryCode", "OperatorConcatId", "ExclusionDate"],
    )

    exclusions = (
        spark.table(match_exclusions_table)
        .select(
            F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))).alias("CountryCode"),
            F.trim(F.coalesce(F.col("OperatorConcatId").cast("string"), F.lit(""))).alias(row_registry_key_column),
        )
        .filter(F.col("CountryCode") != "")
        .filter(F.col(row_registry_key_column) != "")
        .dropDuplicates(["CountryCode", row_registry_key_column])
    )

    return (
        processed_df.select("record_id", "CountryCode", row_registry_key_column)
        .join(exclusions, ["CountryCode", row_registry_key_column], "inner")
        .select("record_id")
        .dropDuplicates(["record_id"])
        .withColumn("is_excluded_from_match", F.lit(True))
    )

def _enrich_source_data(source_df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not bool(cfg.get("EnrichDate", False)):
        return source_df

    spark = source_df.sparkSession
    enriched_operators_table = cfg["enrichedOperatorsTable"]
    row_registry_key_column = cfg["rowRegistryKeyColumn"]
    _require_table(spark, enriched_operators_table)
    _require_table_columns(
        spark,
        enriched_operators_table,
        [row_registry_key_column, *[enriched_column for _, enriched_column in ENRICHMENT_COLUMN_MAPPINGS]],
    )

    enrichment_df = spark.table(enriched_operators_table)
    if "match_found" in enrichment_df.columns:
        enrichment_df = enrichment_df.filter(
            F.lower(F.coalesce(F.col("match_found").cast("string"), F.lit("false"))) == F.lit("true")
        )

    source_types = {field.name: field.dataType for field in source_df.schema.fields}
    _require_dataframe_columns(
        source_df,
        [source_column for source_column, _ in ENRICHMENT_COLUMN_MAPPINGS],
        "Source DataFrame for enrichment",
    )

    enrichment_slice = (
        enrichment_df.select(
            F.trim(F.coalesce(F.col(row_registry_key_column).cast("string"), F.lit(""))).alias(row_registry_key_column),
            *[
                F.col(enriched_column).cast(source_types[source_column]).alias(f"enriched__{source_column}")
                for source_column, enriched_column in ENRICHMENT_COLUMN_MAPPINGS
            ],
        )
        .filter(F.col(row_registry_key_column) != "")
        .dropDuplicates([row_registry_key_column])
    )

    enriched_source = source_df.join(enrichment_slice, row_registry_key_column, "left")
    for source_column, _ in ENRICHMENT_COLUMN_MAPPINGS:
        enriched_source = (
            enriched_source.withColumn(
                source_column,
                F.coalesce(F.col(f"enriched__{source_column}"), F.col(source_column)),
            )
            .drop(f"enriched__{source_column}")
        )
    return enriched_source

def run_country(spark: SparkSession, country_code: str, cfg: Dict[str, Any]) -> DataFrame:
    cfg = _runtime_cfg(country_code, cfg)
    required_tables = {
        cfg["rowRegistryTable"]: ROW_REGISTRY_REQUIRED_COLUMNS,
        cfg["goldenIdHistoryTable"]: ["CountryCode", "OldGoldenId", "NewGoldenId", "Reason", "RecordCount", "RunTimestamp"],
        cfg["goldenIdSequenceTable"]: ["SequenceName", "NextValue", "DateUpdated"],
        cfg["ruleResultsTable"]: [
            "CountryCode",
            "RuleType",
            "RuleStageName",
            "RuleExecutionOrder",
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
        ],
        cfg["ruleEvaluationsTable"]: [
            "CountryCode",
            "RuleType",
            "RuleStageName",
            "RuleExecutionOrder",
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
        ],
        cfg["matchExclusionsTable"]: ["CountryCode", "OperatorConcatId", "ExclusionDate"],
        cfg["matchingStateTable"]: ["CountryCode", "StateName", "StateType", "record_id"],
        cfg["matchLinksTable"]: [
            "CountryCode",
            "src",
            "dst",
            "match_rule",
            "rule_priority",
            "edge_type",
            "block_name",
            "match_key",
            "src_is_rule_subject",
            "dst_is_rule_subject",
        ],
        cfg["componentLabelsTable"]: ["CountryCode", "LabelStageName", "IterationNumber", "record_id", "golden_id"],
        cfg["matchedResultsTable"]: ["CountryCode"],
    }
    for required_table, required_columns in required_tables.items():
        _require_table(spark, required_table)
        _require_table_columns(spark, required_table, required_columns)
    for country_scoped_table in [
        cfg["ruleResultsTable"],
        cfg["ruleEvaluationsTable"],
        cfg["matchingStateTable"],
        cfg["componentLabelsTable"],
    ]:
        _clear_country_slice(spark, country_scoped_table, country_code)

    with timed(f"Country {country_code}: load and standardize"):
        required_columns = collect_required_columns(cfg)
        source = spark.table(cfg["source_table"]).filter(cfg["filter_condition"])
        source = _enrich_source_data(source, cfg)
        source = _ensure_columns(source, required_columns)
        source = _ensure_row_registry(source, cfg)
        processed = standardize_input(source, cfg).persist(StorageLevel.MEMORY_AND_DISK)
        processed.count()
        if not _is_empty(processed.filter(F.col("MDMRowId").isNull())):
            raise ValueError(f"Some rows did not receive an MDMRowId from {cfg['rowRegistryTable']}")
        if _is_empty(processed.filter(_valid_value(F.col("record_id"), cfg.get("invalid_values", []), 1))):
            raise ValueError(
                f"No valid record_id values were generated from MDMRowRegistry table {cfg['rowRegistryTable']}."
            )
        validate_golden_id_space(spark, cfg)

        config_excluded_ids = (
            processed.filter(_sql_or(cfg.get("exclude_from_match_filters")) or "false")
            .select("record_id")
            .dropDuplicates(["record_id"])
            .withColumn("is_excluded_from_match", F.lit(True))
        )
        table_excluded_ids = _load_match_exclusions(processed, cfg)
        excluded_ids = (
            config_excluded_ids.unionByName(table_excluded_ids)
            .dropDuplicates(["record_id"])
        ).persist(StorageLevel.MEMORY_AND_DISK)
        excluded_ids.count()
        matchable = processed.join(excluded_ids.select("record_id"), "record_id", "left_anti").persist(StorageLevel.MEMORY_AND_DISK)
        matchable.count()

    all_match_links = run_match_pipeline(matchable, country_code, cfg)

    with timed(f"Country {country_code}: materialize match links"):
        all_match_links = _overwrite_delta_slice(
            all_match_links.select(
                F.lit(country_code).alias("CountryCode"),
                "src",
                "dst",
                "match_rule",
                "rule_priority",
                "edge_type",
                "block_name",
                "match_key",
                "src_is_rule_subject",
                "dst_is_rule_subject",
            ),
            cfg["matchLinksTable"],
            f"CountryCode = '{_sql_literal(country_code)}'",
        ).select(
            "src",
            "dst",
            "match_rule",
            "rule_priority",
            "edge_type",
            "block_name",
            "match_key",
            "src_is_rule_subject",
            "dst_is_rule_subject",
        ).persist(StorageLevel.MEMORY_AND_DISK)
        link_count = all_match_links.count()
        print(f"  -> Total match links: {link_count}")

    match_links_temp_view = cfg.get("matchLinksTempView") or cfg.get("edges_temp_view")
    if match_links_temp_view:
        all_match_links.createOrReplaceTempView(match_links_temp_view)
        print(f"Created temporary match link evidence view: {match_links_temp_view}")

    with timed(f"Country {country_code}: connected components"):
        matched_record_ids_for_grouping = _materialize_matching_state(
            _matched_record_ids_from_links(all_match_links),
            cfg["matchingStateTable"],
            country_code,
            "matched_record_ids_for_grouping",
            "MatchedRecordIdsForGrouping",
        ).persist(StorageLevel.MEMORY_AND_DISK)
        components = connected_components_native(
            country_code,
            matched_record_ids_for_grouping,
            all_match_links,
            cfg,
        )

    with timed(f"Country {country_code}: golden ids"):
        record_rules = collect_record_match_rules(all_match_links)
        # Persisted inside; one row per distinct record_id with golden_id / source / is_new.
        final_golden_ids = _build_final_golden_ids(processed, components, cfg)
        # Registry first: if the results write fails afterwards, a re-run reuses these ids
        # (tier 2) instead of minting again.
        persist_golden_id_assignments(final_golden_ids, cfg, country_code)

    with timed(f"Country {country_code}: final output"):
        match_group_sizes = final_golden_ids.groupBy("golden_id").agg(F.count("*").alias("match_group_size"))
        previous_golden_id = F.col(PREVIOUS_GOLDEN_ID_COLUMN).cast("long")
        source_golden_id = F.col("SourceGoldenRecordId").cast("long")
        final_df = (
            processed.join(final_golden_ids, "record_id", "left")
            .join(record_rules, "record_id", "left")
            .join(excluded_ids, "record_id", "left")
            .join(match_group_sizes, "golden_id", "left")
            .withColumn("match_group_size", F.coalesce(F.col("match_group_size"), F.lit(1)))
            .withColumn("is_excluded_from_match", F.coalesce(F.col("is_excluded_from_match"), F.lit(False)))
            .withColumn(
                "final_match_rule",
                F.when(F.col("is_excluded_from_match") == F.lit(True), F.lit("Excluded from Match"))
                .otherwise(F.coalesce(F.col("final_match_rule"), F.lit("Self/No Match"))),
            )
            .withColumn(
                "is_matched",
                F.when(
                    F.col("is_excluded_from_match"),
                    F.lit(False),
                ).otherwise(F.col("match_group_size") > F.lit(1)),
            )
            .withColumn("is_group_anchor", F.coalesce(F.col("record_id") == F.col("TempClusterId"), F.lit(False)))
            # Golden id continuity / migration validation columns.
            .withColumn("previous_golden_id", previous_golden_id)
            .withColumn("previous_golden_id_source", F.col(PREVIOUS_GOLDEN_ID_SOURCE_COLUMN).cast("string"))
            .withColumn("golden_id_is_new", F.coalesce(F.col("golden_id_is_new"), F.lit(False)))
            .withColumn(
                "golden_id_changed",
                previous_golden_id.isNotNull() & (previous_golden_id != F.col("golden_id")),
            )
            .withColumn(
                "golden_id_differs_from_source",
                source_golden_id.isNotNull() & (source_golden_id != F.col("golden_id")),
            )
            .drop(
                "is_excluded_from_match",
                "TempClusterId",
                PREVIOUS_GOLDEN_ID_COLUMN,
                PREVIOUS_GOLDEN_ID_SOURCE_COLUMN,
                PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN,
            )
        ).persist(StorageLevel.MEMORY_AND_DISK)
        final_df.count()

        history_count = append_golden_id_history(build_golden_id_history(final_df, country_code), cfg)
        print(f"  -> Golden id transitions recorded in {cfg['goldenIdHistoryTable']}: {history_count}")

        _overwrite_delta_slice(
            final_df,
            cfg["matchedResultsTable"],
            f"CountryCode = '{_sql_literal(country_code)}'",
            merge_schema=True,
        )

    components.unpersist()
    matched_record_ids_for_grouping.unpersist()
    all_match_links.unpersist()
    excluded_ids.unpersist()
    processed.unpersist()
    matchable.unpersist()
    final_golden_ids.unpersist()
    final_df.unpersist()
    print(f"Saved {country_code} matched output to {cfg['matchedResultsTable']}")
    # Return the persisted Delta slice rather than the lazy plan so callers (e.g. display())
    # do not silently recompute the pipeline.
    return spark.table(cfg["matchedResultsTable"]).where(f"CountryCode = '{_sql_literal(country_code)}'")

def run_all(
    spark: SparkSession,
    config_json: Optional[str] = None,
    conf_dir=None,
) -> None:
    """Run match for every country in config_json or conf/countries/*.json.

    If config_json is None, loads conf/countries/{CC}.json via load_all_country_configs
    (skips template.json / _*.json; raises if none found).
    If config_json is provided, parses that multi-country JSON object instead.
    """
    if config_json is None:
        configs = load_all_country_configs(conf_dir)
    else:
        configs = json.loads(config_json)
    for country_code, cfg in configs.items():
        run_country(spark, country_code, cfg)
