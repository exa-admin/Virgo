"""Country-level match orchestration — the entry point for the whole engine.

``run_country`` walks one country through:

    load -> enrich -> row registry -> standardize -> exclusions
      -> exact/fuzzy priority waterfall
      -> + Informatica group links (hard)
      -> connected components (-> drop Informatica-group bridges)
      -> golden ids -> MDMMatchedResults

Every stage writes its intermediate state to a country slice of a Delta table, so a run
can be inspected step by step and the Spark plan never grows unbounded.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from matching import graph, io
from matching.config import (
    ENRICHMENT_COLUMN_MAPPINGS,
    SOURCE_GOLDEN_GROUP_RULE_NAME,
    SOURCE_GOLDEN_GROUP_RULE_STAGE,
    conf_dir,
    load_all_country_configs,
    resolve_config,
)
from matching.expressions import (
    MATCH_LINK_BASE_COLUMNS,
    clean_text,
    collect_required_columns,
    compact_key,
    dedupe_match_links,
    empty_match_links,
    empty_record_ids,
    ensure_columns,
    is_empty,
    matched_record_ids,
    require_dataframe_columns,
    sql_literal,
    sql_or,
    standardize_address,
    timed,
    union_all,
    valid_value,
)
from matching.golden_ids import (
    PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN,
    PREVIOUS_GOLDEN_ID_COLUMN,
    PREVIOUS_GOLDEN_ID_SOURCE_COLUMN,
    assign_golden_ids,
    collect_record_match_rules,
    persist_assignments,
    save_history,
    validate_golden_id_space,
    validate_group_assignments,
)
from matching.rules import run_exact_rule, run_fuzzy_rule

ROW_REGISTRY_COLUMNS = [
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

_NUMERIC_ID = r"^[0-9]+$"


# ------------------------------------------------------------------- row registry


def _validate_source_golden_ids(source: DataFrame) -> None:
    """Reject a non-blank GoldenRecordId that is not a plain integer string.

    Informatica sends the id as a numeric STRING. A silent cast would turn a malformed
    value into NULL, and the record would lose its Informatica grouping (and could be
    minted a brand-new engine id).
    """
    trimmed = F.trim(F.col("GoldenRecordId").cast("string"))
    invalid = source.filter(trimmed.isNotNull() & (trimmed != F.lit("")) & ~trimmed.rlike(_NUMERIC_ID))
    if not is_empty(invalid):
        sample = [r["GoldenRecordId"] for r in invalid.select("GoldenRecordId").limit(5).collect()]
        raise ValueError(
            f"Source GoldenRecordId contains non-numeric values (sample: {sample}). Informatica golden ids must be "
            "integer strings; fix the source or clean the values before running the match."
        )


def _attach_row_registry(source: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """MERGE the source keys into MDMRowRegistry, then attach MDMRowId and golden id context.

    Informatica id policy: ``SourceGoldenRecordId`` is only written when the source carries
    a value. A known id is never downgraded to NULL (Informatica stops populating it once a
    country migrates to this engine), but it may change to another non-null id (Informatica
    re-merged while it still owns the country).
    """
    spark = source.sparkSession
    registry_table = cfg["rowRegistryTable"]
    key_column = cfg["rowRegistryKeyColumn"]

    require_dataframe_columns(source, [key_column, "CountryCode", "GoldenRecordId", "BDLLoadTimestamp"], "Source DataFrame")
    _validate_source_golden_ids(source)

    # Deterministic one row per key: prefer a row carrying an Informatica id, then the
    # earliest load timestamp (dropDuplicates would pick arbitrarily).
    key_order = Window.partitionBy("CountryCode", "OperatorConcatId").orderBy(
        F.col("SourceGoldenRecordId").asc_nulls_last(),
        F.col("GoldenIDCreatedDate").asc_nulls_last(),
    )
    trimmed_id = F.trim(F.col("GoldenRecordId").cast("string"))
    merge_source = (
        source.select(
            F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))).alias("CountryCode"),
            F.trim(F.coalesce(F.col(key_column).cast("string"), F.lit(""))).alias("OperatorConcatId"),
            F.when(trimmed_id.rlike(_NUMERIC_ID), trimmed_id.cast("long")).alias("SourceGoldenRecordId"),
            F.col("BDLLoadTimestamp").cast("timestamp").alias("GoldenIDCreatedDate"),
        )
        .filter(F.col("CountryCode") != "")
        .filter(valid_value(F.col("OperatorConcatId"), cfg.get("invalid_values", []), 1))
        .withColumn("_rank", F.row_number().over(key_order))
        .filter(F.col("_rank") == F.lit(1))
        .drop("_rank")
    )

    view = "TmpMDMRowRegistrySource"
    merge_source.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {registry_table} AS target
        USING {view} AS source
        ON target.CountryCode = source.CountryCode
           AND target.OperatorConcatId = source.OperatorConcatId
        WHEN MATCHED AND source.SourceGoldenRecordId IS NOT NULL
             AND (target.SourceGoldenRecordId IS NULL
                  OR target.SourceGoldenRecordId <> source.SourceGoldenRecordId) THEN UPDATE SET
          target.SourceGoldenRecordId = source.SourceGoldenRecordId,
          target.GoldenIDCreatedDate = source.GoldenIDCreatedDate,
          target.DateUpdated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          CountryCode, OperatorConcatId, SourceGoldenRecordId, GoldenIDCreatedDate, DateCreated, DateUpdated
        )
        VALUES (
          source.CountryCode, source.OperatorConcatId, source.SourceGoldenRecordId, source.GoldenIDCreatedDate,
          current_timestamp(), current_timestamp()
        )
        """
    )

    # Registry values win over the raw source, so SourceGoldenRecordId keeps the last known
    # Informatica id even when the source has since gone NULL for a migrated country.
    registry = spark.table(registry_table).select(
        "CountryCode",
        F.col("OperatorConcatId").alias(key_column),
        "MDMRowId",
        "SourceGoldenRecordId",
        "GoldenIDCreatedDate",
        F.col("MDMGoldenId").alias(PREVIOUS_GOLDEN_ID_COLUMN),
        F.col("MDMGoldenIdSource").alias(PREVIOUS_GOLDEN_ID_SOURCE_COLUMN),
        F.col("MDMGoldenIdAssignedDate").alias(PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN),
    )
    # MDMRowId presence is checked by the caller, after standardization (avoids a second
    # full scan of the source here).
    return (
        source.withColumn("CountryCode", F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))))
        .withColumn(key_column, F.trim(F.coalesce(F.col(key_column).cast("string"), F.lit(""))))
        .join(registry, ["CountryCode", key_column], "left")
    )


# ------------------------------------------------------------ enrich / standardize


def _apply_enrichment(source: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """Overlay reviewed address enrichment from mdmenrichedoperators, where present."""
    if not bool(cfg.get("EnrichDate", False)):
        return source

    spark = source.sparkSession
    key_column = cfg["rowRegistryKeyColumn"]
    source_columns = [source_column for source_column, _ in ENRICHMENT_COLUMN_MAPPINGS]
    io.require_table(spark, cfg["enrichedOperatorsTable"])
    io.require_columns(
        spark, cfg["enrichedOperatorsTable"], [key_column, *[enriched for _, enriched in ENRICHMENT_COLUMN_MAPPINGS]]
    )
    require_dataframe_columns(source, source_columns, "Source DataFrame for enrichment")

    enrichment = spark.table(cfg["enrichedOperatorsTable"])
    if "match_found" in enrichment.columns:
        enrichment = enrichment.filter(
            F.lower(F.coalesce(F.col("match_found").cast("string"), F.lit("false"))) == F.lit("true")
        )

    source_types = {field.name: field.dataType for field in source.schema.fields}
    enrichment = (
        enrichment.select(
            F.trim(F.coalesce(F.col(key_column).cast("string"), F.lit(""))).alias(key_column),
            *[
                F.col(enriched).cast(source_types[source_column]).alias(f"enriched__{source_column}")
                for source_column, enriched in ENRICHMENT_COLUMN_MAPPINGS
            ],
        )
        .filter(F.col(key_column) != "")
        .dropDuplicates([key_column])
    )

    enriched = source.join(enrichment, key_column, "left")
    for column in source_columns:
        enriched = enriched.withColumn(column, F.coalesce(F.col(f"enriched__{column}"), F.col(column))).drop(
            f"enriched__{column}"
        )
    return enriched


def standardize_input(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """Derive the ``c_*`` match and blocking attributes from the source columns."""
    std = cfg["standardization"]
    if "MDMRowId" not in df.columns:
        raise ValueError("MDMRowId column is missing. Merge source rows into MDMRowRegistry before standardization.")

    zip_digits = F.regexp_replace(F.coalesce(F.col(std["zip_column"]).cast("string"), F.lit("")), r"[^0-9]", "")
    state = clean_text(F.col(std["state_column"])) if std.get("state_column") else F.lit("").cast("string")

    return (
        df.withColumn("MDMRowId", F.col("MDMRowId").cast("long"))
        .withColumn("record_id", F.col("MDMRowId"))
        .withColumn("c_name", clean_text(F.col(std["name_column"])))
        .withColumn("c_name_exact", compact_key(F.col(std["name_column"])))
        .withColumn("c_city", clean_text(F.col(std["city_column"])))
        .withColumn("c_state", state)
        .withColumn("c_address", standardize_address(F.concat_ws(" ", *[F.col(c) for c in std.get("address_columns", [])])))
        .withColumn("c_zip", zip_digits)
        .withColumn("c_zip_exact", zip_digits)
        .withColumn("c_name_prefix4", F.substring(F.col("c_name"), 1, 4))
        # Soundex is empty for names that start with digits or non-Latin script; fall back
        # to the prefix so those records still land in a block.
        .withColumn("_soundex", F.soundex(F.col("c_name")))
        .withColumn(
            "c_name_soundex",
            F.when(F.col("_soundex").isNull() | (F.length(F.col("_soundex")) == 0), F.col("c_name_prefix4")).otherwise(
                F.col("_soundex")
            ),
        )
        .drop("_soundex")
    )


# ------------------------------------------------------------------- exclusions


def _excluded_record_ids(processed: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """Records held out of new matching: config filters plus the MDMMatchExclusions table."""
    spark = processed.sparkSession
    key_column = cfg["rowRegistryKeyColumn"]

    from_config = (
        processed.filter(sql_or(cfg.get("exclude_from_match_filters")) or "false")
        .select("record_id")
        .dropDuplicates(["record_id"])
    )

    exclusions = (
        spark.table(cfg["matchExclusionsTable"])
        .select(
            F.trim(F.coalesce(F.col("CountryCode").cast("string"), F.lit(""))).alias("CountryCode"),
            F.trim(F.coalesce(F.col("OperatorConcatId").cast("string"), F.lit(""))).alias(key_column),
        )
        .filter((F.col("CountryCode") != "") & (F.col(key_column) != ""))
        .dropDuplicates(["CountryCode", key_column])
    )
    from_table = (
        processed.select("record_id", "CountryCode", key_column)
        .join(exclusions, ["CountryCode", key_column], "inner")
        .select("record_id")
        .dropDuplicates(["record_id"])
    )

    return (
        from_config.unionByName(from_table)
        .dropDuplicates(["record_id"])
        .withColumn("is_excluded_from_match", F.lit(True))
    )


# ------------------------------------------------------------- exact/fuzzy waterfall


def run_match_waterfall(df: DataFrame, country_code: str, cfg: Dict[str, Any]) -> DataFrame:
    """Run every configured rule in priority order and return the union of their links.

    With ``priorityMatching`` (the default), a record matched by a higher-priority rule is
    no longer a *subject* of the later ones: it can still be matched *to* (so groups stay
    connected), but it will not pull in new members on a weaker rule. The first rule runs
    without a subject set, which lets it use the cheaper one-direction pair join.
    """
    spark = df.sparkSession
    priority_matching = bool(cfg.get("priorityMatching", cfg.get("waterfall_rules", True)))

    rules = [("exact", rule) for rule in cfg.get("exact_match_rules", [])]
    rules += [("fuzzy", rule) for rule in cfg.get("fuzzy_match_rules", [])]
    rules.sort(key=lambda item: (int(item[1].get("priority", 100)), item[1].get("rule_name", ""), item[0]))
    if not rules:
        return empty_match_links(spark)

    per_rule_links: List[DataFrame] = []
    already_matched = empty_record_ids(spark)

    for order, (rule_type, rule) in enumerate(rules, start=1):
        stage_name = f"{order:03d}_{rule['rule_name']}"

        # The first rule has nothing to defer to, so it runs over the whole population
        # without a subject set (which also lets fuzzy use the cheaper one-direction join).
        subject_ids = None
        if priority_matching and order > 1:
            subject_ids = io.save_matching_state(
                df.select("record_id").join(already_matched, "record_id", "left_anti").dropDuplicates(["record_id"]),
                cfg["matchingStateTable"],
                country_code,
                f"active_rule_subject_ids_{stage_name}",
                "ActiveRuleSubjectIds",
            )

        if rule_type == "exact":
            links = run_exact_rule(df, rule, cfg, subject_ids=subject_ids)
        else:
            links = run_fuzzy_rule(df, rule, cfg, country_code, stage_name, order, subject_ids=subject_ids)

        saved = io.save_rule_results(links, df, cfg, country_code, rule_type, stage_name, order)
        print(f"  -> Materialized {saved.count()} match links for {rule['rule_name']}")
        per_rule_links.append(links)

        if priority_matching:
            already_matched = io.save_matching_state(
                already_matched.unionByName(matched_record_ids(links)).dropDuplicates(["record_id"]),
                cfg["matchingStateTable"],
                country_code,
                f"matched_record_ids_after_{stage_name}",
                "MatchedRecordIds",
            )

    all_links = dedupe_match_links(union_all(per_rule_links)).persist(StorageLevel.MEMORY_AND_DISK)
    all_links.count()
    for links in per_rule_links:
        links.unpersist()
    return all_links


# ------------------------------------------------------------------------- run


def _required_tables(cfg: Dict[str, Any]) -> Dict[str, List[str]]:
    """Every table run_country needs, with the columns it reads or writes."""
    link_columns = MATCH_LINK_BASE_COLUMNS
    stage_columns = ["CountryCode", "RuleType", "RuleStageName", "RuleExecutionOrder"]
    return {
        cfg["rowRegistryTable"]: ROW_REGISTRY_COLUMNS,
        cfg["goldenIdHistoryTable"]: ["CountryCode", "OldGoldenId", "NewGoldenId", "Reason", "RecordCount", "RunTimestamp"],
        cfg["goldenIdSequenceTable"]: ["SequenceName", "NextValue", "DateUpdated"],
        cfg["ruleResultsTable"]: stage_columns + ["SrcOperatorConcatId", "DstOperatorConcatId"] + link_columns,
        cfg["ruleEvaluationsTable"]: stage_columns + io.EVALUATION_COLUMNS,
        cfg["matchExclusionsTable"]: ["CountryCode", "OperatorConcatId", "ExclusionDate"],
        cfg["matchingStateTable"]: ["CountryCode", "StateName", "StateType", "record_id"],
        cfg["matchLinksTable"]: ["CountryCode"] + link_columns,
        cfg["componentLabelsTable"]: ["CountryCode", "LabelStageName", "IterationNumber", "record_id", "golden_id"],
        cfg["matchedResultsTable"]: ["CountryCode"],
    }


def run_country(spark: SparkSession, country_code: str, cfg: Optional[Dict[str, Any]] = None) -> DataFrame:
    """Match one country end to end and return its slice of MDMMatchedResults.

    Config comes from the country code: global settings from ``conf/base.json``, country
    settings from ``conf/countries/{CC}.json``. A ``cfg`` passed in still gets base.json
    merged underneath it.
    """
    country_code = country_code.upper()
    cfg = resolve_config(country_code, cfg)
    where_country = f"CountryCode = '{sql_literal(country_code)}'"
    print(f"Config for {country_code}: base.json + countries/{country_code}.json under {conf_dir()}")

    io.require_tables(spark, _required_tables(cfg))
    for table in (cfg["ruleResultsTable"], cfg["ruleEvaluationsTable"], cfg["matchingStateTable"], cfg["componentLabelsTable"]):
        io.clear_country_slice(spark, table, country_code)

    # --- load, standardize, exclude ------------------------------------------------
    with timed(f"Country {country_code}: load and standardize"):
        source = io.read_source_population(spark, cfg)
        source = _apply_enrichment(source, cfg)
        source = ensure_columns(source, collect_required_columns(cfg))
        source = _attach_row_registry(source, cfg)

        processed = standardize_input(source, cfg).persist(StorageLevel.MEMORY_AND_DISK)
        processed.count()
        if not is_empty(processed.filter(F.col("MDMRowId").isNull())):
            raise ValueError(f"Some rows did not receive an MDMRowId from {cfg['rowRegistryTable']}")
        if is_empty(processed.filter(valid_value(F.col("record_id"), cfg.get("invalid_values", []), 1))):
            raise ValueError(f"No valid record_id values were generated from {cfg['rowRegistryTable']}.")
        validate_golden_id_space(spark, cfg)

        excluded = _excluded_record_ids(processed, cfg).persist(StorageLevel.MEMORY_AND_DISK)
        excluded.count()
        matchable = processed.join(excluded.select("record_id"), "record_id", "left_anti").persist(
            StorageLevel.MEMORY_AND_DISK
        )
        matchable.count()

    # --- engine rules ---------------------------------------------------------------
    match_links = run_match_waterfall(matchable, country_code, cfg)
    blocked_links = None

    # --- Informatica groups as hard links -------------------------------------------
    if bool(cfg["preserve_source_golden_groups"]):
        with timed(f"Country {country_code}: source golden group links"):
            # Built from the FULL processed population: Informatica groupings also hold for
            # records excluded from new matching. These edges sit outside the priority
            # waterfall (they must not change which rule other records match on) but are
            # part of the match graph, so they do show up in final_match_rule.
            group_links = graph.build_group_links(processed)
            saved = io.save_rule_results(
                group_links, processed, cfg, country_code, "source_golden", SOURCE_GOLDEN_GROUP_RULE_STAGE, 0
            )
            print(f"  -> Materialized {saved.count()} Source_GoldenRecordId links")

            if graph.blocking_enabled(cfg):
                match_links, blocked_links = graph.split_direct_bridges(match_links, processed)
                blocked_links = blocked_links.persist(StorageLevel.MEMORY_AND_DISK)
                print(f"  -> Blocked {blocked_links.count()} engine links that directly bridge two Informatica groups")
            match_links = dedupe_match_links(match_links.unionByName(group_links))

    def save_links(links: DataFrame) -> DataFrame:
        return io.overwrite_slice(
            links.select(F.lit(country_code).alias("CountryCode"), *MATCH_LINK_BASE_COLUMNS),
            cfg["matchLinksTable"],
            where_country,
        ).select(*MATCH_LINK_BASE_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)

    with timed(f"Country {country_code}: materialize match links"):
        match_links = save_links(match_links)
        print(f"  -> Total match links: {match_links.count()}")

    # --- components ------------------------------------------------------------------
    with timed(f"Country {country_code}: connected components"):
        linked_ids = io.save_matching_state(
            matched_record_ids(match_links),
            cfg["matchingStateTable"],
            country_code,
            "matched_record_ids_for_grouping",
            "MatchedRecordIdsForGrouping",
        ).persist(StorageLevel.MEMORY_AND_DISK)
        components = graph.connected_components(country_code, linked_ids, match_links, cfg)

    if graph.blocking_enabled(cfg):
        with timed(f"Country {country_code}: resolve transitive Informatica group bridges"):
            resolution = graph.resolve_transitive_bridges(country_code, components, match_links, processed, cfg)
            if resolution is not None:
                transitive_blocked, resolved_components = resolution
                print(f"  -> Blocked {transitive_blocked.count()} engine links that transitively bridge Informatica groups")
                blocked_links = blocked_links.unionByName(transitive_blocked).persist(StorageLevel.MEMORY_AND_DISK)
                blocked_links.count()
                transitive_blocked.unpersist()

                # The final graph must not contain the dropped edges: rewrite MDMMatchLinks.
                reduced = graph.remove_links(match_links, blocked_links)
                match_links.unpersist()
                match_links = save_links(reduced)
                print(f"  -> Total match links after resolution: {match_links.count()}")
                components.unpersist()
                components = resolved_components

            blocked_count = graph.save_blocked_links(blocked_links, processed, cfg, country_code)
            print(f"  -> Blocked Informatica group bridges recorded in {cfg['ruleResultsTable']}: {blocked_count}")

    if cfg.get("matchLinksTempView"):
        match_links.createOrReplaceTempView(cfg["matchLinksTempView"])
        print(f"Created temporary match link evidence view: {cfg['matchLinksTempView']}")

    # --- golden ids -------------------------------------------------------------------
    with timed(f"Country {country_code}: golden ids"):
        record_rules = collect_record_match_rules(match_links)
        golden_ids = assign_golden_ids(processed, components, cfg)
        # Migration guarantee: no Informatica group split / re-minted / renamed. Raises
        # before anything is written back.
        validate_group_assignments(golden_ids, processed, cfg, country_code)
        # Registry first: if the results write fails afterwards, a re-run reuses these ids
        # (tier 2) instead of minting again.
        persist_assignments(golden_ids, cfg, country_code)

    # --- output ------------------------------------------------------------------------
    with timed(f"Country {country_code}: final output"):
        final_df = _build_output(processed, golden_ids, record_rules, excluded).persist(StorageLevel.MEMORY_AND_DISK)
        final_df.count()

        history_count = save_history(final_df, cfg, country_code)
        print(f"  -> Golden id transitions recorded in {cfg['goldenIdHistoryTable']}: {history_count}")
        io.overwrite_slice(final_df, cfg["matchedResultsTable"], where_country, merge_schema=True)

    for cached in (components, linked_ids, match_links, blocked_links, excluded, processed, matchable, golden_ids, final_df):
        if cached is not None:
            cached.unpersist()

    print(f"Saved {country_code} matched output to {cfg['matchedResultsTable']}")
    # Return the persisted Delta slice, not the lazy plan, so display() does not silently
    # recompute the whole pipeline.
    return spark.table(cfg["matchedResultsTable"]).where(where_country)


def _build_output(
    processed: DataFrame,
    golden_ids: DataFrame,
    record_rules: DataFrame,
    excluded: DataFrame,
) -> DataFrame:
    """Join everything the run produced into the MDMMatchedResults row shape."""
    group_sizes = golden_ids.groupBy("golden_id").agg(F.count("*").alias("match_group_size"))
    previous_golden_id = F.col(PREVIOUS_GOLDEN_ID_COLUMN).cast("long")
    source_golden_id = F.col("SourceGoldenRecordId").cast("long")

    return (
        processed.join(golden_ids, "record_id", "left")
        .join(record_rules, "record_id", "left")
        .join(excluded, "record_id", "left")
        .join(group_sizes, "golden_id", "left")
        .withColumn("match_group_size", F.coalesce(F.col("match_group_size"), F.lit(1)))
        .withColumn("is_excluded_from_match", F.coalesce(F.col("is_excluded_from_match"), F.lit(False)))
        # An excluded record never takes part in engine rules, so the only link it can carry
        # is Source_GoldenRecordId: it stays in its Informatica group and is labelled as
        # such instead of a plain "Excluded from Match".
        .withColumn(
            "final_match_rule",
            F.when(F.col("is_excluded_from_match") & F.col("final_match_rule").isNull(), F.lit("Excluded from Match"))
            .when(F.col("is_excluded_from_match"), F.concat(F.lit("Excluded from Match, "), F.col("final_match_rule")))
            .otherwise(F.coalesce(F.col("final_match_rule"), F.lit("Self/No Match"))),
        )
        .withColumn(
            "is_matched",
            F.when(
                F.col("is_excluded_from_match") & ~F.col("final_match_rule").contains(SOURCE_GOLDEN_GROUP_RULE_NAME),
                F.lit(False),
            ).otherwise(F.col("match_group_size") > F.lit(1)),
        )
        .withColumn("is_group_anchor", F.coalesce(F.col("record_id") == F.col("TempClusterId"), F.lit(False)))
        # Golden id continuity / migration audit columns.
        .withColumn("previous_golden_id", previous_golden_id)
        .withColumn("previous_golden_id_source", F.col(PREVIOUS_GOLDEN_ID_SOURCE_COLUMN).cast("string"))
        .withColumn("golden_id_is_new", F.coalesce(F.col("golden_id_is_new"), F.lit(False)))
        .withColumn("golden_id_changed", previous_golden_id.isNotNull() & (previous_golden_id != F.col("golden_id")))
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
    )


def run_all(spark: SparkSession, country_codes: Optional[List[str]] = None) -> None:
    """Match several countries in sequence.

    Defaults to every ``conf/countries/{CC}.json``; pass ``country_codes`` to run a subset.
    """
    if country_codes:
        for code in country_codes:
            run_country(spark, code)
        return
    for country_code, cfg in load_all_country_configs().items():
        run_country(spark, country_code, cfg)
