"""Databricks / Spark-native Customer MDM Match & Merge engine.

Public API:
    from mdm.pipeline import run_country, run_all
    from mdm.config import load_country_config, CONFIG_JSON
"""
from mdm.config import CONFIG_JSON, load_all_country_configs, load_country_config
from mdm.pipeline import run_all, run_country

__all__ = [
    "CONFIG_JSON",
    "load_country_config",
    "load_all_country_configs",
    "run_country",
    "run_all",
]
