"""Source ingestion — kept separate from the match engine on purpose.

The engine consumes a single operator "population" DataFrame. Where that data comes
from is isolated here so a deployment can switch between Delta tables today and CSV or
Parquet files tomorrow without touching pipeline / matching code.

Two inputs (see conf/base.json ``source`` / ``golden_source``):

* operator ("non-golden", e.g. ``sl_bdl_processed_cd_prod.cd.vw_ufsoperator``):
  the population to match. One row per ``OperatorConcatId``; each already carries its
  Informatica ``GoldenRecordId`` (its group) when it has been matched before.
* golden ("already matched", e.g. ``sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden``):
  the surviving master record per ``GoldenRecordId`` (no ``OperatorConcatId``).

``build_source_population`` returns the operator feed, plus any golden master whose
``GoldenRecordId`` is absent from the operator feed (so an existing Informatica group is
never lost just because its master row is not in the current operator slice). Backfilled
rows get a deterministic synthetic key (``GRID_<GoldenRecordId>``) so they are stable
across runs and keep their Informatica id via the normal source-golden-group path.

Each source spec is declarative::

    {"format": "delta",   "table": "catalog.schema.view"}
    {"format": "csv",     "path": "/mnt/landing/xx/operator/", "options": {"header": "true"}}
    {"format": "parquet", "path": "/mnt/landing/xx/operator/"}
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from matching.config import GOLDEN_MASTER_KEY_PREFIX
from matching.utils import collect_required_columns

# Columns the engine cannot synthesize: without these the registry MERGE / source-golden
# grouping cannot run. Match/standardization columns are validated as warnings because
# the pipeline null-fills missing ones via _ensure_columns.
REGISTRY_CRITICAL_COLUMNS = ["OperatorConcatId", "CountryCode", "GoldenRecordId", "BDLLoadTimestamp"]

# Columns needed to key/relate golden masters (they have no OperatorConcatId of their own).
GOLDEN_CRITICAL_COLUMNS = ["CountryCode", "GoldenRecordId"]

_SUPPORTED_FORMATS = {"delta", "csv", "parquet"}


def _read_spec(spark: SparkSession, spec: Dict[str, Any], label: str) -> DataFrame:
    """Read one declarative source spec into a DataFrame (delta table/path, csv, parquet)."""
    if not spec:
        raise ValueError(f"{label}: source spec is empty")
    fmt = str(spec.get("format", "delta")).lower()
    if fmt not in _SUPPORTED_FORMATS:
        raise ValueError(f"{label}: unsupported source format '{fmt}' (use one of {sorted(_SUPPORTED_FORMATS)})")
    options: Dict[str, Any] = dict(spec.get("options", {}))
    table = spec.get("table")
    path = spec.get("path")

    if fmt == "delta" and table:
        return spark.table(table)
    if not path:
        raise ValueError(f"{label}: spec needs 'table' (delta) or 'path' ({fmt})")

    reader = spark.read.format(fmt)
    if fmt == "csv":
        # Default to a header row and string columns so downstream casts stay explicit;
        # callers can override via options.
        reader = reader.option("header", "true").option("inferSchema", "false")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(path)


def validate_source_columns(df: DataFrame, cfg: Dict[str, Any], label: str, critical: List[str]) -> None:
    """Raise if a hard-required column is missing; print a warning for missing match columns."""
    present = set(df.columns)
    missing_critical = [c for c in critical if c not in present]
    if missing_critical:
        raise ValueError(
            f"{label} is missing required columns {missing_critical}. "
            f"Available columns include: {sorted(present)[:25]}..."
        )
    match_columns = set(collect_required_columns(cfg))
    std = cfg.get("standardization", {})
    for key in ("name_column", "city_column", "state_column", "zip_column"):
        if std.get(key):
            match_columns.add(std[key])
    match_columns.update(std.get("address_columns", []))
    missing_match = sorted(match_columns - present)
    if missing_match:
        print(
            f"  -> WARNING: {label} is missing match/standardization columns {missing_match}; "
            "they will be treated as NULL (rules using them cannot match)."
        )


def read_operator_source(spark: SparkSession, cfg: Dict[str, Any], country_code: str) -> DataFrame:
    """Read the operator (population) source and apply the country filter."""
    spec = cfg["source"]
    df = _read_spec(spark, spec, "operator source")
    validate_source_columns(df, cfg, "operator source", REGISTRY_CRITICAL_COLUMNS)
    return df.filter(cfg["filter_condition"])


def read_golden_source(spark: SparkSession, cfg: Dict[str, Any], country_code: str) -> Optional[DataFrame]:
    """Read the golden (already-matched masters) source, or None if not configured."""
    spec = cfg.get("golden_source")
    if not spec:
        return None
    df = _read_spec(spark, spec, "golden source")
    validate_source_columns(df, cfg, "golden source", GOLDEN_CRITICAL_COLUMNS)
    return df.filter(cfg["filter_condition"])


def _align_to_columns(df: DataFrame, target_columns: List[str]) -> DataFrame:
    """Project df onto target_columns, null-filling any it does not have (order preserved)."""
    present = set(df.columns)
    return df.select(
        *[
            F.col(c) if c in present else F.lit(None).cast("string").alias(c)
            for c in target_columns
        ]
    )


def build_source_population(spark: SparkSession, cfg: Dict[str, Any], country_code: str) -> DataFrame:
    """Return the operator population fed to the match engine.

    = operator feed  UNION  golden masters whose GoldenRecordId is absent from it.
    Backfilled golden rows receive a deterministic synthetic OperatorConcatId
    (``GRID_<GoldenRecordId>``) and IsGoldenRecordFlag = 'TRUE'.
    """
    operator_df = read_operator_source(spark, cfg, country_code)
    golden_df = read_golden_source(spark, cfg, country_code)
    if golden_df is None:
        return operator_df

    key_col = "GoldenRecordId"
    operator_ids = (
        operator_df.select(F.trim(F.col(key_col).cast("string")).alias(key_col))
        .filter(F.col(key_col).isNotNull() & (F.col(key_col) != F.lit("")))
        .distinct()
    )
    golden_keyed = golden_df.withColumn("_grid", F.trim(F.col(key_col).cast("string")))
    missing_masters = (
        golden_keyed.filter(F.col("_grid").isNotNull() & (F.col("_grid") != F.lit("")))
        .join(operator_ids.withColumnRenamed(key_col, "_grid"), "_grid", "left_anti")
        .withColumn("OperatorConcatId", F.concat(F.lit(GOLDEN_MASTER_KEY_PREFIX), F.col("_grid")))
        .withColumn("IsGoldenRecordFlag", F.lit("TRUE"))
        .drop("_grid")
    )

    # Align golden masters to the operator schema (extra golden columns are dropped;
    # operator columns absent from golden become NULL) so unionByName is exact.
    target_columns = operator_df.columns
    missing_aligned = _align_to_columns(missing_masters, target_columns)
    backfilled = operator_df.unionByName(missing_aligned)
    return backfilled
