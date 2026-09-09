# Databricks notebook source
# MAGIC %md
# MAGIC # MDM Match — run one country or all countries
# MAGIC
# MAGIC Set the **countries** widget and run all cells.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `countries` | `MY`, or `MY,SG,TH` for several, or `ALL` for every configured country |
# MAGIC | `conf_dir` | Blank to use the configs bundled in the wheel; else a folder holding `base.json` + `countries/` |
# MAGIC
# MAGIC Prerequisites, once per environment:
# MAGIC 1. Run `sql/setup_tables.sql`.
# MAGIC 2. Install `mdm_engine-<version>-py3-none-any.whl` as a **cluster or job library**
# MAGIC    (Compute → Libraries → Install new → Python whl). Then skip the `%pip` cell below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the engine (only if it is not a cluster library)
# MAGIC
# MAGIC Uncomment and set the path to your uploaded wheel. `%pip` has to be the whole cell,
# MAGIC so the path cannot come from a widget.

# COMMAND ----------

# MAGIC %pip install --force-reinstall /Volumes/<catalog>/<schema>/<volume>/mdm_engine-0.1.0-py3-none-any.whl

# COMMAND ----------

# Databricks injects `spark`, `dbutils` and `display` into every notebook. This block is
# never executed (TYPE_CHECKING is False at runtime) — it only tells the IDE where those
# names come from, so editors stop reporting them as unresolved.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import dbutils, display, spark

# COMMAND ----------

dbutils.widgets.text("countries", "MY", "Countries (CC, CC,CC or ALL)")
dbutils.widgets.text("conf_dir", "", "Config folder (blank = bundled in wheel)")

# COMMAND ----------

import os

conf_dir = dbutils.widgets.get("conf_dir").strip()
if conf_dir:
    # Point the engine at configs outside the wheel (Workspace / Volume / DBFS).
    os.environ["MDM_CONF_DIR"] = conf_dir

from matching import available_countries, run_country

requested = dbutils.widgets.get("countries").strip().upper()
countries = available_countries() if requested == "ALL" else [c.strip() for c in requested.split(",") if c.strip()]

print(f"Configured countries: {available_countries()}")
print(f"Running: {countries}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run
# MAGIC
# MAGIC Each country is independent: it replaces only its own slice of the output tables, so
# MAGIC one country failing does not corrupt the others. Failures are collected and re-raised
# MAGIC at the end so a bad country does not stop the rest of the batch.

# COMMAND ----------

results, failures = {}, {}

for country_code in countries:
    print(f"\n{'=' * 70}\n{country_code}\n{'=' * 70}")
    try:
        results[country_code] = run_country(spark, country_code)
    except Exception as error:  # noqa: BLE001 - report every country before failing
        failures[country_code] = error
        print(f"!! {country_code} FAILED: {type(error).__name__}: {error}")

print(f"\nDone. Succeeded: {sorted(results)}  Failed: {sorted(failures)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary per country
# MAGIC
# MAGIC `golden_id_changed` should be **0** while a country is still being served by
# MAGIC Informatica — that column is the migration safety check.

# COMMAND ----------

from pyspark.sql import functions as F

if results:
    summary = None
    for country_code, df in results.items():
        row = df.agg(
            F.lit(country_code).alias("country"),
            F.count("*").alias("records"),
            F.countDistinct("golden_id").alias("golden_ids"),
            F.sum(F.col("is_matched").cast("int")).alias("matched"),
            F.sum(F.col("golden_id_is_new").cast("int")).alias("new_golden_ids"),
            F.sum(F.col("golden_id_changed").cast("int")).alias("golden_id_changed"),
        )
        summary = row if summary is None else summary.unionByName(row)
    display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect one country's results
# MAGIC
# MAGIC Stewardship detail lives in `MDMRuleResults` (accepted edges per rule),
# MAGIC `MDMRuleEvaluations` (every fuzzy candidate and its scores) and `MDMGoldenIdHistory`
# MAGIC (old → new id remaps).

# COMMAND ----------

if results:
    first_country = sorted(results)[0]
    display(
        results[first_country].select(
            "OperatorConcatId",
            "OperatorName",
            "CityText",
            "ZipCode",
            "golden_id",
            "golden_id_source",
            "match_group_size",
            "is_matched",
            "final_match_rule",
            "golden_id_changed",
        ).orderBy(F.col("match_group_size").desc(), "golden_id")
    )
