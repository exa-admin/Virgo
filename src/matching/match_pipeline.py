"""Orchestration — one country, end to end.

``run_country`` calls each step in order; the steps themselves live in their own modules:

    read.py         load the operator + golden population, overlay enrichment
    registry.py     MERGE into mdm_row_registry, attach MDMRowId
    standardize.py  derive the c_* match attributes, work out exclusions
    rules.py        run one rule -> match links       (the waterfall below drives these)
    graph.py        connected components, Informatica group links
    golden_ids.py   assign the golden id, with Informatica continuity
    write.py        every Delta write, always a country slice

Each stage checkpoints to Delta, so a run can be inspected step by step and the Spark
plan never grows unbounded.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from matching import graph, read, registry, standardize, write
from matching.config import (
    SOURCE_GOLDEN_GROUP_RULE_NAME,
    SOURCE_GOLDEN_GROUP_RULE_STAGE,
    conf_dir,
    load_all_country_configs,
    resolve_config,
)
from matching.helpers import (
    MATCH_LINK_BASE_COLUMNS,
    collect_required_columns,
    dedupe_match_links,
    empty_match_links,
    empty_record_ids,
    ensure_columns,
    is_empty,
    matched_record_ids,
    no_evidence,
    sql_literal,
    valid_value,
    timed,
    union_all,
)
from matching.golden_ids import (
    PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN,
    PREVIOUS_GOLDEN_ID_COLUMN,
    PREVIOUS_GOLDEN_ID_SOURCE_COLUMN,
    assign_golden_ids,
    collect_record_match_rules,
    persist_assignments,
    save_change_log,
    save_history,
    validate_golden_id_space,
    validate_group_assignments,
)
from matching.rules import run_exact_rule, run_fuzzy_rule

# ------------------------------------------------------------------- row registry


# ------------------------------------------------------------ enrich / standardize


# ------------------------------------------------------------------- exclusions


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
            subject_ids = write.save_matching_state(
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

        saved = write.save_rule_results(links, df, cfg, country_code, rule_type, stage_name, order)
        print(f"  -> Materialized {saved.count()} match links for {rule['rule_name']}")
        # Use the checkpointed slice (no similarity columns), not the evidence-bearing plan.
        per_rule_links.append(saved.select(*MATCH_LINK_BASE_COLUMNS, *no_evidence()))

        if priority_matching:
            already_matched = write.save_matching_state(
                already_matched.unionByName(matched_record_ids(saved)).dropDuplicates(["record_id"]),
                cfg["matchingStateTable"],
                country_code,
                f"matched_record_ids_after_{stage_name}",
                "MatchedRecordIds",
            )

    return dedupe_match_links(union_all(per_rule_links))


# ------------------------------------------------------------------------- run


def _required_tables(cfg: Dict[str, Any]) -> Dict[str, List[str]]:
    """Every table run_country needs, with the columns it reads or writes."""
    link_columns = MATCH_LINK_BASE_COLUMNS
    stage_columns = ["CountryCode", "RuleType", "RuleStageName", "RuleExecutionOrder"]
    return {
        cfg["rowRegistryTable"]: registry.ROW_REGISTRY_COLUMNS,
        cfg["goldenIdHistoryTable"]: ["CountryCode", "OldGoldenId", "NewGoldenId", "Reason", "RecordCount", "RunTimestamp"],
        cfg["goldenIdSequenceTable"]: ["SequenceName", "NextValue", "DateUpdated"],
        cfg["ruleResultsTable"]: stage_columns + ["SrcOperatorConcatId", "DstOperatorConcatId"] + link_columns,
        cfg["ruleEvaluationsTable"]: stage_columns + write.EVALUATION_COLUMNS,
        cfg["matchExclusionsTable"]: ["CountryCode", "OperatorConcatId", "ExclusionDate"],
        cfg["matchingStateTable"]: ["CountryCode", "StateName", "StateType", "record_id"],
        cfg["matchLinksTable"]: ["CountryCode"] + link_columns,
        cfg["componentLabelsTable"]: ["CountryCode", "LabelStageName", "IterationNumber", "record_id", "golden_id"],
        cfg["matchedResultsTable"]: ["CountryCode"],
    }


def run_country(spark: SparkSession, country_code: str, cfg: Optional[Dict[str, Any]] = None) -> DataFrame:
    """Match one country end to end and return its slice of mdm_matched_results.

    Config comes from the country code: global settings from ``conf/base.json``, country
    settings from ``conf/countries/{CC}.json``. A ``cfg`` passed in still gets base.json
    merged underneath it.
    """
    country_code = country_code.upper()
    cfg = resolve_config(country_code, cfg)
    where_country = f"CountryCode = '{sql_literal(country_code)}'"
    # Printed so a run's output proves which build produced it — an old wheel left on a
    # cluster is otherwise indistinguishable from a new one in the logs.
    from matching import __version__

    print(f"mdm-engine {__version__} | country {country_code} | config {conf_dir()}")

    write.require_tables(spark, _required_tables(cfg))
    for table in (cfg["ruleResultsTable"], cfg["ruleEvaluationsTable"], cfg["matchingStateTable"], cfg["componentLabelsTable"]):
        write.clear_country_slice(spark, table, country_code)

    # --- load, standardize, exclude ------------------------------------------------
    with timed(f"Country {country_code}: load and standardize"):
        source = read.read_source_population(spark, cfg)
        source = read.apply_enrichment(source, cfg)
        source = ensure_columns(source, collect_required_columns(cfg))
        source = registry.attach_row_registry(source, cfg)

        processed = standardize.standardize_input(source, cfg).persist(StorageLevel.MEMORY_AND_DISK)
        processed.count()
        if not is_empty(processed.filter(F.col("MDMRowId").isNull())):
            raise ValueError(f"Some rows did not receive an MDMRowId from {cfg['rowRegistryTable']}")
        if is_empty(processed.filter(valid_value(F.col("record_id"), cfg.get("invalid_values", []), 1))):
            raise ValueError(f"No valid record_id values were generated from {cfg['rowRegistryTable']}.")
        validate_golden_id_space(spark, cfg)

        excluded = standardize.excluded_record_ids(processed, cfg).persist(StorageLevel.MEMORY_AND_DISK)
        excluded.count()
        matchable = processed.join(excluded.select("record_id"), "record_id", "left_anti").persist(
            StorageLevel.MEMORY_AND_DISK
        )
        matchable.count()

    # --- engine rules ---------------------------------------------------------------
    match_links = run_match_waterfall(matchable, country_code, cfg)
    blocked_links = None

    # What OUR rules alone say, before Informatica's groupings are forced into the graph.
    # Kept because the final golden_id can never disagree with Informatica once those
    # groupings are hard-linked, so this is the only column the over/undermatch views can
    # honestly compare against.
    with timed(f"Country {country_code}: engine-only components"):
        engine_components = graph.connected_components(
            country_code,
            matched_record_ids(match_links),
            match_links,
            cfg,
            stage_prefix="engine_labels",
        )

    # --- Informatica groups as hard links -------------------------------------------
    if bool(cfg["preserve_source_golden_groups"]):
        with timed(f"Country {country_code}: source golden group links"):
            # Built from the FULL processed population: Informatica groupings also hold for
            # records excluded from new matching. These edges sit outside the priority
            # waterfall (they must not change which rule other records match on) but are
            # part of the match graph, so they do show up in final_match_rule.
            group_links = graph.build_group_links(processed)
            saved = write.save_rule_results(
                group_links, processed, cfg, country_code, "source_golden", SOURCE_GOLDEN_GROUP_RULE_STAGE, 0
            )
            print(f"  -> Materialized {saved.count()} Source_GoldenRecordId links")

            if graph.blocking_enabled(cfg):
                match_links, blocked_links = graph.split_direct_bridges(match_links, processed)
                blocked_links = blocked_links.persist(StorageLevel.MEMORY_AND_DISK)
                print(f"  -> Blocked {blocked_links.count()} engine links that directly bridge two Informatica groups")
            match_links = dedupe_match_links(match_links.unionByName(group_links))

    def save_links(links: DataFrame) -> DataFrame:
        return write.overwrite_slice(
            links.select(F.lit(country_code).alias("CountryCode"), *MATCH_LINK_BASE_COLUMNS),
            cfg["matchLinksTable"],
            where_country,
        ).select(*MATCH_LINK_BASE_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)

    with timed(f"Country {country_code}: materialize match links"):
        match_links = save_links(match_links)
        print(f"  -> Total match links: {match_links.count()}")

    # --- components ------------------------------------------------------------------
    with timed(f"Country {country_code}: connected components"):
        linked_ids = write.save_matching_state(
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

                # The final graph must not contain the dropped edges: rewrite mdm_match_links.
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
        final_df = _build_output(processed, golden_ids, record_rules, excluded, engine_components).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        final_df.count()

        history_count = save_history(final_df, cfg, country_code)
        print(f"  -> Golden id transitions recorded in {cfg['goldenIdHistoryTable']}: {history_count}")
        # Reads the PREVIOUS run's slice, so it has to happen before the overwrite below.
        change_count = save_change_log(final_df, cfg, country_code)
        print(f"  -> Golden id changes traced in {cfg['changeLogTable']}: {change_count}")
        write.overwrite_slice(final_df, cfg["matchedResultsTable"], where_country, merge_schema=True)

    for cached in (
        components,
        engine_components,
        linked_ids,
        match_links,
        blocked_links,
        excluded,
        processed,
        matchable,
        golden_ids,
        final_df,
    ):
        if cached is not None:
            cached.unpersist()

    print(f"Saved {country_code} matched output to {cfg['matchedResultsTable']}")
    # Return the persisted Delta slice, not the lazy plan, so display() does not silently
    # recompute the whole pipeline.
    return read.read(spark, cfg, "matched_results").where(where_country)


def _build_output(
    processed: DataFrame,
    golden_ids: DataFrame,
    record_rules: DataFrame,
    excluded: DataFrame,
    engine_components: DataFrame,
) -> DataFrame:
    """Join everything the run produced into the mdm_matched_results row shape."""
    group_sizes = golden_ids.groupBy("golden_id").agg(F.count("*").alias("match_group_size"))
    engine_ids = engine_components.select("record_id", F.col("golden_id").alias("_engine_match_id"))
    previous_golden_id = F.col(PREVIOUS_GOLDEN_ID_COLUMN).cast("long")
    source_golden_id = F.col("SourceGoldenRecordId").cast("long")

    return (
        processed.join(golden_ids, "record_id", "left")
        .join(record_rules, "record_id", "left")
        .join(excluded, "record_id", "left")
        .join(group_sizes, "golden_id", "left")
        .join(engine_ids, "record_id", "left")
        .withColumn("match_group_size", F.coalesce(F.col("match_group_size"), F.lit(1)))
        # A record our rules never linked is its own engine group.
        .withColumn("engine_match_id", F.coalesce(F.col("_engine_match_id"), F.col("record_id")).cast("long"))
        .withColumn("MatchRunTimestamp", F.current_timestamp())
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
            "_engine_match_id",
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
