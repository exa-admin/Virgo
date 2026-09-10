# Databricks notebook source
# MAGIC %md
# MAGIC # DQ — enrich operator addresses from Google Places
# MAGIC
# MAGIC Optional pre-step for the match run. For operators whose address fields are blank,
# MAGIC it fetches up to N Google Places candidates and writes them out **one row per
# MAGIC candidate** (`match_order` 1..N) so a steward can pick the right one.
# MAGIC
# MAGIC Nothing here feeds the match engine automatically: the reviewed rows have to land in
# MAGIC `mdmenrichedoperators`, which `EnrichDate: true` then overlays onto the source.
# MAGIC
# MAGIC Needs the `mdm_engine` wheel installed with its `dq` extra (`pandas`, `requests`).

# COMMAND ----------

# Databricks injects `spark`, `dbutils` and `display` into every notebook. This block is
# never executed (TYPE_CHECKING is False at runtime) — it only tells the IDE where those
# names come from, so editors stop reporting them as unresolved.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import dbutils, display, spark

# COMMAND ----------

from matching.config import dataset_table, resolve_config

country_code = "MY"
country_name = "Malaysia"
limit_rows = 100
max_results = 10
output_path = "/Volumes/catalog/schema/volume/address_candidates"

source_view = dataset_table(resolve_config(country_code), "operator")
print(source_view)

# COMMAND ----------

# MAGIC %md
# MAGIC ## API key
# MAGIC
# MAGIC Read it from a secret scope — never paste the key into the notebook.

# COMMAND ----------

import os

os.environ["GOOGLE_PLACES_API_KEY"] = dbutils.secrets.get(scope="mdm", key="google-places-api-key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the operators that need enrichment
# MAGIC
# MAGIC Only operators with no street *and* no state, excluding known-invalid names, and only
# MAGIC those whose `OperatorConcatId` maps to a single golden record (ambiguous ones need a
# MAGIC steward, not an API).

# COMMAND ----------

df_operators = spark.sql(f"""
    WITH unambiguous AS (
        SELECT OperatorConcatId
        FROM {source_view}
        WHERE CountryCode = '{country_code}'
          AND StreetText IS NULL
          AND StateText IS NULL
          AND lower(OperatorName) NOT LIKE '%invalid%'
        GROUP BY OperatorConcatId
        HAVING COUNT(DISTINCT GoldenRecordId) = 1
    )
    SELECT
        o.OperatorConcatId,
        o.OperatorName,
        o.StreetText,
        o.HouseNumberText,
        o.HouseNumberExtensionText,
        o.CityText,
        o.ZipCode,
        o.StateText,
        '{country_name}' AS CountryName
    FROM {source_view} o
    JOIN unambiguous u ON o.OperatorConcatId = u.OperatorConcatId
    WHERE o.CountryCode = '{country_code}'
      AND o.StreetText IS NULL
      AND o.StateText IS NULL
      AND lower(o.OperatorName) NOT LIKE '%invalid%'
    LIMIT {limit_rows}
""").cache()

print(f"Operators to enrich: {df_operators.count()}")
display(df_operators.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich
# MAGIC
# MAGIC Places calls run on the driver, one per operator — keep `limit_rows` modest.

# COMMAND ----------

from dq import enrich_operators_spark

df_enriched = enrich_operators_spark(df_operators, country_code=country_code, max_results=max_results)

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
# MAGIC ## Write the candidates for review
# MAGIC
# MAGIC Drops the `original_*` columns and keeps `match_order` so reviewers can choose.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

df_out = spark.createDataFrame(
    df_enriched.drop(columns=[c for c in df_enriched.columns if c.startswith("original_")])
)
df_out = df_out.select([F.col(c).cast(StringType()) for c in df_out.columns])
df_out.write.option("header", "true").mode("overwrite").csv(output_path)

print(f"Wrote {df_out.count()} candidate rows to {output_path}")
