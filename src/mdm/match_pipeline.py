"""Match pipeline orchestration: exact/fuzzy waterfall with rule materialization."""
from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame

from mdm.delta_io import _materialize_matching_state, _materialize_rule_results
from mdm.exact_match import run_exact_match
from mdm.fuzzy_match import run_fuzzy_match
from mdm.utils import (
    _dedupe_match_links,
    _empty_ids,
    _empty_match_links,
    _matched_record_ids_from_links,
    _union_all,
)

def run_match_pipeline(df: DataFrame, country_code: str, cfg: Dict[str, Any]) -> DataFrame:
    spark = df.sparkSession
    priority_matching = bool(cfg.get("priorityMatching", cfg.get("waterfall_rules", True)))

    rules = [
        ("exact", rule)
        for rule in cfg.get("exact_match_rules", [])
    ] + [
        ("fuzzy", rule)
        for rule in cfg.get("fuzzy_match_rules", [])
    ]
    rules = sorted(rules, key=lambda item: (int(item[1].get("priority", 100)), item[1].get("rule_name", ""), item[0]))

    match_links = []
    matched_record_ids = _empty_ids(spark)
    single_rule_cfg = {**cfg, "priorityMatching": False, "waterfall_rules": False}

    for rule_index, (rule_type, rule) in enumerate(rules):
        rule_stage_name = f"{rule_index + 1:03d}_{rule['rule_name']}"
        active_rule_subject_ids = df.select("record_id").join(matched_record_ids, "record_id", "left_anti").dropDuplicates(["record_id"])
        if priority_matching and rule_index > 0:
            df_for_rule = df
            rule_subject_ids = active_rule_subject_ids
            rule_subject_ids = _materialize_matching_state(
                rule_subject_ids,
                cfg["matchingStateTable"],
                country_code,
                f"active_rule_subject_ids_{rule_stage_name}",
                "ActiveRuleSubjectIds",
            )
        elif priority_matching:
            df_for_rule = df.join(matched_record_ids, "record_id", "left_anti")
            rule_subject_ids = None
        else:
            df_for_rule = df
            rule_subject_ids = None

        if rule_type == "exact":
            matches = run_exact_match(df_for_rule, [rule], single_rule_cfg, rule_subject_ids=rule_subject_ids)
        elif rule_type == "fuzzy":
            matches = run_fuzzy_match(
                df_for_rule,
                [rule],
                single_rule_cfg,
                rule_subject_ids=rule_subject_ids,
                country_code=country_code,
                rule_stage_name=rule_stage_name,
                rule_execution_order=rule_index + 1,
            )
        else:
            raise ValueError(f"Unsupported rule type: {rule_type}")

        materialized_rule_results = _materialize_rule_results(
            matches,
            df,
            cfg["rowRegistryKeyColumn"],
            cfg["ruleResultsTable"],
            country_code,
            rule_type,
            rule_stage_name,
            rule_index + 1,
        )
        rule_match_link_count = materialized_rule_results.count()
        print(f"  -> Materialized {rule_match_link_count} match links for {rule['rule_name']}")

        match_links.append(matches)
        if priority_matching:
            matched_record_ids = matched_record_ids.unionByName(_matched_record_ids_from_links(matches)).dropDuplicates(["record_id"])
            matched_record_ids = _materialize_matching_state(
                matched_record_ids,
                cfg["matchingStateTable"],
                country_code,
                f"matched_record_ids_after_{rule_stage_name}",
                "MatchedRecordIds",
            )

    result = _dedupe_match_links(_union_all(match_links)) if match_links else _empty_match_links(spark)
    return result

def run_match_waterfall(df: DataFrame, cfg: Dict[str, Any], country_code: Optional[str] = None) -> DataFrame:
    if country_code is None:
        if "CountryCode" not in df.columns:
            raise ValueError("CountryCode is required when calling run_match_waterfall without run_country.")
        country_values = [row[0] for row in df.select("CountryCode").dropDuplicates().limit(2).collect()]
        if len(country_values) != 1:
            raise ValueError("run_match_waterfall expects exactly one CountryCode when country_code is not provided.")
        country_code = country_values[0]
    return run_match_pipeline(df, country_code, cfg)
