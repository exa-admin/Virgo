"""Databricks / Spark-native Customer MDM Match engine.

Public API:
    from matching.pipeline import run_country, run_all
    from matching.config import load_country_config, CONFIG_JSON
"""
from matching.config import CONFIG_JSON, load_all_country_configs, load_country_config
from matching.pipeline import run_all, run_country

__all__ = [
    "CONFIG_JSON",
    "load_country_config",
    "load_all_country_configs",
    "run_country",
    "run_all",
]
