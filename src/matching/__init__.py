"""Databricks / Spark-native Customer MDM match engine.

    from matching import run_country, run_all

    run_country(spark, "MY")   # one country
    run_all(spark)             # every country with a config
"""
from importlib.metadata import PackageNotFoundError, version as _version

from matching.config import available_countries, load_country_config
from matching.match_pipeline import run_all, run_country

try:
    __version__ = _version("mdm-engine")
except PackageNotFoundError:  # running from a source checkout, not an installed wheel
    __version__ = "0.0.0+source"

__all__ = ["run_country", "run_all", "load_country_config", "available_countries", "__version__"]
