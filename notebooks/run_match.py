# Databricks notebook source
from pyspark.sql import functions as F

from matching import run_country
from matching.config import dataset_table, resolve_config

COUNTRY = "MY"

cfg = resolve_config(COUNTRY)
print(cfg["target_schema"])

# COMMAND ----------

df = run_country(spark, COUNTRY)

# COMMAND ----------

display(
    df.agg(
        F.count("*").alias("records"),
        F.countDistinct("golden_id").alias("golden_ids"),
        F.sum(F.col("is_matched").cast("int")).alias("matched"),
        F.sum(F.col("golden_id_is_new").cast("int")).alias("new_golden_ids"),
        F.sum(F.col("golden_id_changed").cast("int")).alias("golden_id_changed"),
    )
)

# COMMAND ----------

display(
    df.select(
        "OperatorConcatId",
        "OperatorName",
        "CityText",
        "ZipCode",
        "golden_id",
        "golden_id_source",
        "engine_match_id",
        "match_group_size",
        "is_matched",
        "final_match_rule",
        "golden_id_changed",
    ).orderBy(F.col("match_group_size").desc(), "golden_id")
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT ChangeReason, COUNT(*) AS records, COUNT(DISTINCT OperatorConcatId) AS operators
        FROM {dataset_table(cfg, "change_log")}
        WHERE CountryCode = '{COUNTRY}'
        GROUP BY ChangeReason
        ORDER BY records DESC
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT * FROM {dataset_table(cfg, "informatica_undermatch")}
        WHERE CountryCode = '{COUNTRY}'
        ORDER BY engine_group_size DESC, engine_match_id
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT * FROM {dataset_table(cfg, "informatica_overmatch")}
        WHERE CountryCode = '{COUNTRY}'
        ORDER BY informatica_group_size DESC, SourceGoldenRecordId
    """)
)
