"""Databricks / Spark-native Customer MDM match engine.

    from matching import run_country, run_all

    run_country(spark, "MY")   # one country
    run_all(spark)             # every country with a config
"""
from matching.config import available_countries, load_country_config
from matching.pipeline import run_all, run_country

__all__ = ["run_country", "run_all", "load_country_config", "available_countries"]
