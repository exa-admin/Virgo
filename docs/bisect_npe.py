# Databricks notebook source
# MAGIC %md
# MAGIC # Bisect the fuzzy-rule NullPointerException
# MAGIC
# MAGIC The Python traceback tells us which *action* dies, never which *expression*. This
# MAGIC rebuilds the failing rule one scoring column at a time and counts after each, so the
# MAGIC last line printed names the expression that throws.
# MAGIC
# MAGIC Run on the same cluster. Read-only: it writes nothing.
# MAGIC
# MAGIC Note: token_jaccard was my earlier suspect and is **ruled out** — the 18-minute
# MAGIC working script contains that expression verbatim. No stage here is favoured; the
# MAGIC point is simply to find which one throws.

# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import spark

# COMMAND ----------

from pyspark import StorageLevel
from pyspark.sql import functions as F

from matching import config, read, registry, standardize
from matching.helpers import (
    collect_required_columns, ensure_columns, levenshtein_similarity,
    rule_matches, token_jaccard_similarity,
)
from matching.rules import _candidate_pairs, _score_pairs, _subject_flags

COUNTRY = "MY"
RULE_NAME = "Fuzzy_Name_Address_Zip"      # the first fuzzy rule that fails

cfg = config.resolve_config(COUNTRY)
rule = next(r for r in cfg["fuzzy_match_rules"] if r["rule_name"] == RULE_NAME)
invalid_values = cfg.get("invalid_values", [])
print(f"rule: {rule['rule_name']}  conditions: {[c['column'] + ':' + c.get('method','') for c in rule['conditions']]}")

# COMMAND ----------

# Rebuild `matchable` exactly as run_country does.
source = read.read_source_population(spark, cfg)
source = read.apply_enrichment(source, cfg)
source = ensure_columns(source, collect_required_columns(cfg))
source = registry.attach_row_registry(source, cfg)
processed = standardize.standardize_input(source, cfg).persist(StorageLevel.MEMORY_AND_DISK)
print("processed:", processed.count())

excluded = standardize.excluded_record_ids(processed, cfg)
matchable = processed.join(excluded.select("record_id"), "record_id", "left_anti").persist(
    StorageLevel.MEMORY_AND_DISK
)
print("matchable:", matchable.count())

# COMMAND ----------

# This rule runs first among the fuzzy rules, so subject_ids is None for it in the waterfall
# only if it is rule 1 overall. It is rule 4, so subjects are set — but for isolating the
# NPE the subject set is irrelevant, and None keeps the plan simpler.
population = matchable.filter(F.trim(F.coalesce(F.col("c_address"), F.lit(""))) != "").persist(
    StorageLevel.MEMORY_AND_DISK
)
print("population:", population.count())

pairs, blocks = _candidate_pairs(population, rule, cfg, None)
print("candidate pairs:", pairs.count())          # if this throws, the blocking join is the culprit
blocks.unpersist()

# COMMAND ----------

condition_columns = sorted({spec["column"] for spec in rule.get("conditions", [])})
trace_columns = sorted({cfg["rowRegistryKeyColumn"], "c_name", "c_address", "c_city", "c_state", "c_zip", *condition_columns})
left = population.select("record_id", *trace_columns).alias("a")
right = population.select("record_id", *trace_columns).alias("b")
candidates = pairs.join(left, F.col("src") == F.col("a.record_id"), "inner").join(
    right, F.col("dst") == F.col("b.record_id"), "inner"
)
print("joined candidates:", candidates.count())   # if this throws, the join is the culprit

# COMMAND ----------

# MAGIC %md
# MAGIC ## One expression at a time
# MAGIC
# MAGIC The last line that prints is the last expression that works. The one after it throws.

# COMMAND ----------

spec = {s["column"]: s for s in rule["conditions"]}
addr = spec.get("c_address", {})

probes = [
    ("name levenshtein",
     levenshtein_similarity(F.col("a.c_name"), F.col("b.c_name"), invalid_values,
                            int(spec.get("c_name", {}).get("min_length", 1)))),
    ("address levenshtein",
     levenshtein_similarity(F.col("a.c_address"), F.col("b.c_address"), invalid_values,
                            int(addr.get("min_length", 1)))),
    ("address token_jaccard",
     token_jaccard_similarity(F.col("a.c_address"), F.col("b.c_address"), invalid_values,
                              int(addr.get("min_length", 1)), int(addr.get("min_token_length", 2)))),
]

for label, expression in probes:
    n = candidates.select(expression.alias("v")).agg(F.count("v")).collect()[0][0]
    print(f"OK   {label}: {n}")

# COMMAND ----------

scored = _subject_flags(_score_pairs(candidates, rule, invalid_values), None)
print("OK   _score_pairs + _subject_flags:", scored.count())

accepted = scored.filter(rule_matches(rule, invalid_values))
print("OK   rule_matches filter:", accepted.count())

# COMMAND ----------

from matching.helpers import dedupe_match_links
from matching.rules import _rule_identity

links = dedupe_match_links(
    accepted.select(
        "src", "dst", *_rule_identity(rule, "fuzzy"), "block_name", "match_key",
        "src_is_rule_subject", "dst_is_rule_subject",
        "NameLevenshteinSimilarity", "AddressLevenshteinSimilarity",
        "AddressTokenJaccardSimilarity", "AddressBestSimilarity", "ZipExactMatch",
    )
)
print("OK   dedupe_match_links:", links.count())
print("\nNothing threw — the failure needs the waterfall's subject_ids to reproduce.")
