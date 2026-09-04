# Databricks notebook source
# MAGIC %md
# MAGIC # DQ — enrich bad MY operator addresses (multi-candidate)
# MAGIC
# MAGIC Requires the **full Engine repo** in Workspace/Repos (with `src/dq/`), not
# MAGIC only this notebook. First cell bootstraps `sys.path` from the notebook location.

# COMMAND ----------

# MAGIC %run ./00_path_setup

# COMMAND ----------

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from dq.enrich_operators import enrich_operators_spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("country_code", "MY")
dbutils.widgets.text("limit_rows", "100")
dbutils.widgets.text("max_results", "10")
dbutils.widgets.text(
    "output_path",
    "/Volumes/pds_auroradsar_prod/schema_informatica/staging/operator_enriched_my_v5",
)

country_code = dbutils.widgets.get("country_code").strip().upper()
limit_rows = int(dbutils.widgets.get("limit_rows"))
max_results = int(dbutils.widgets.get("max_results"))
output_path = dbutils.widgets.get("output_path").strip()

# Prefer a secret scope in production:
# os.environ["GOOGLE_PLACES_API_KEY"] = dbutils.secrets.get(scope="…", key="…")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load operators needing enrichment

# COMMAND ----------

df_operators = spark.sql(f"""
    SELECT
        OperatorConcatId,
        OperatorName,
        StreetText,
        HouseNumberText,
        HouseNumberExtensionText,
        CityText,
        ZipCode,
        StateText,
        'Malaysia' AS CountryName
    FROM sl_bdl_processed_cd_prod.cd.vw_ufsoperator
    WHERE CountryCode = 'MY'
      AND StreetText IS NULL
      AND StateText IS NULL
      AND lower(OperatorName) NOT LIKE '%invalid%'
      -- AND OperatorConcatId NOT IN (SELECT OperatorConcatId FROM ufsoperator_enriched_MY)
      -- AND OperatorConcatId NOT IN (SELECT OperatorConcatId FROM ufsoperator_enriched_MY2)
      AND OperatorConcatId IN (
            SELECT operatorConcatID
            FROM sl_bdl_processed_cd_prod.cd.vw_ufsoperator
            WHERE CountryCode = 'MY'
              AND StreetText IS NULL
              AND StateText IS NULL
              AND lower(OperatorName) NOT LIKE '%invalid%'
            GROUP BY operatorConcatID
            HAVING COUNT(DISTINCT goldenrecordid) = 1
      )
    LIMIT {limit_rows}
""").cache()

print(f"Operators to enrich: {df_operators.count()}")
display(df_operators.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich — one row per Places candidate (`match_order` 1..N)

# COMMAND ----------

df_enriched = enrich_operators_spark(
    df_operators,
    country_code=country_code,
    max_results=max_results,
)

display(
    df_enriched[
        [
            "OperatorConcatId",
            "match_order",
            "match_found",
            "original_OperatorName",
            "OperatorName",
            "StreetText",
            "HouseNumberText",
            "CityText",
            "ZipCode",
            "StateText",
            "formatted_address",
            "place_id",
            "rating",
        ]
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write candidates for matching / review
# MAGIC
# MAGIC Drops `original_*` columns; keeps `match_order` so reviewers can choose.

# COMMAND ----------

df_for_matching = spark.createDataFrame(
    df_enriched.drop(
        columns=[c for c in df_enriched.columns if c.startswith("original_")]
    )
)
df_select = df_for_matching.select(
    [F.col(c).cast(StringType()) for c in df_for_matching.columns]
)

df_select.write.option("header", "true").mode("overwrite").csv(output_path)

print(f"Wrote {df_select.count()} candidate rows to {output_path}")
