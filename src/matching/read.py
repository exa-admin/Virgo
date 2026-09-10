"""Step 1 — read the source population.

Every read in the engine goes through :func:`read`, which resolves a dataset name from
``conf/storage.config``. Moving a dataset from a Delta table to Parquet is a config edit;
no engine code changes.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from matching.config import ENRICHMENT_COLUMN_MAPPINGS, GOLDEN_MASTER_KEY_PREFIX, dataset_spec
from matching.helpers import collect_required_columns, require_dataframe_columns

SUPPORTED_FORMATS = {"delta", "csv", "parquet"}

# The registry MERGE and Informatica grouping cannot run without these. Match columns are
# only warned about, since the pipeline null-fills whatever a source lacks.
OPERATOR_REQUIRED_COLUMNS = ["OperatorConcatId", "CountryCode", "GoldenRecordId", "BDLLoadTimestamp"]
GOLDEN_REQUIRED_COLUMNS = ["CountryCode", "GoldenRecordId"]


def read_spec(spark: SparkSession, spec: Dict[str, Any], label: str) -> DataFrame:
    """Load one declarative storage spec. The only place the engine touches a Spark reader.

    Teach the engine a new storage format here and every dataset can use it.
    """
    if not spec:
        raise ValueError(f"{label}: storage spec is empty")
    fmt = str(spec.get("format", "delta")).lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"{label}: unsupported storage format '{fmt}' (use one of {sorted(SUPPORTED_FORMATS)})")

    table, path = spec.get("table"), spec.get("path")
    if fmt == "delta" and table:
        return spark.table(table)
    if not path:
        raise ValueError(f"{label}: spec needs 'table' (delta) or 'path' ({fmt})")

    reader = spark.read.format(fmt)
    if fmt == "csv":
        # Header row + string columns by default so downstream casts stay explicit.
        reader = reader.option("header", "true").option("inferSchema", "false")
    for key, value in dict(spec.get("options", {})).items():
        reader = reader.option(key, value)
    return reader.load(path)


def read(spark: SparkSession, cfg: Dict[str, Any], dataset: str) -> DataFrame:
    """Read a dataset by its ``storage.config`` name — the engine's only read path.

    ``read.read(spark, cfg, "row_registry")`` rather than ``spark.table(...)``: the name
    stays the same when the underlying store changes from a table to files.
    """
    return read_spec(spark, dataset_spec(cfg, dataset), dataset)


def describe(cfg: Dict[str, Any], dataset: str) -> str:
    """Human-readable location of a dataset, for error messages and run logs."""
    spec = dataset_spec(cfg, dataset)
    return str(spec.get("table") or f"{spec.get('format')}:{spec.get('path')}")


def _validate_columns(
    df: DataFrame,
    cfg: Dict[str, Any],
    label: str,
    required: List[str],
    synthesized: Iterable[str] = (),
) -> None:
    present = set(df.columns)
    missing = [c for c in required if c not in present]
    if missing:
        raise ValueError(f"{label} is missing required columns {missing}. Available: {sorted(present)[:25]}...")

    missing_match = sorted(set(collect_required_columns(cfg)) - present - set(synthesized))
    if missing_match:
        print(
            f"  -> WARNING: {label} is missing match/standardization columns {missing_match}; "
            "they will be treated as NULL (rules using them cannot match)."
        )


def read_source_population(spark: SparkSession, cfg: Dict[str, Any]) -> DataFrame:
    """The operator population fed to the match engine, for one country.

    Two inputs (``source`` / ``golden_source`` in the config):

    * **operator** — the population to match, one row per ``OperatorConcatId``, each
      carrying its Informatica ``GoldenRecordId`` when it has been matched before.
    * **golden** — the surviving master per ``GoldenRecordId`` (no ``OperatorConcatId``).

    Returns the operator feed plus any golden master whose ``GoldenRecordId`` is absent
    from it, so an Informatica group is never lost just because its master row is not in
    the current operator slice. Backfilled rows get a deterministic synthetic key
    (``GRID_<GoldenRecordId>``) and so stay stable across runs.
    """
    operator = read(spark, cfg, "operator")
    _validate_columns(operator, cfg, f"operator source ({describe(cfg, 'operator')})", OPERATOR_REQUIRED_COLUMNS)
    operator = operator.filter(cfg["filter_condition"])

    if not cfg.get("golden_source"):
        return operator

    golden = read(spark, cfg, "golden")
    _validate_columns(
        golden,
        cfg,
        f"golden source ({describe(cfg, 'golden')})",
        GOLDEN_REQUIRED_COLUMNS,
        synthesized=[cfg.get("rowRegistryKeyColumn", "OperatorConcatId")],
    )
    golden = golden.filter(cfg["filter_condition"])

    known_ids = (
        operator.select(F.trim(F.col("GoldenRecordId").cast("string")).alias("_grid"))
        .filter(F.col("_grid").isNotNull() & (F.col("_grid") != F.lit("")))
        .distinct()
    )
    missing_masters = (
        golden.withColumn("_grid", F.trim(F.col("GoldenRecordId").cast("string")))
        .filter(F.col("_grid").isNotNull() & (F.col("_grid") != F.lit("")))
        .join(known_ids, "_grid", "left_anti")
        .withColumn("OperatorConcatId", F.concat(F.lit(GOLDEN_MASTER_KEY_PREFIX), F.col("_grid")))
        .withColumn("IsGoldenRecordFlag", F.lit("TRUE"))
        .drop("_grid")
    )

    # Align to the operator schema (extra golden columns dropped, absent ones NULL) so
    # unionByName is exact.
    present = set(missing_masters.columns)
    aligned = missing_masters.select(
        *[F.col(c) if c in present else F.lit(None).cast("string").alias(c) for c in operator.columns]
    )
    return operator.unionByName(aligned)


def require_dataset(
    spark: SparkSession,
    cfg: Dict[str, Any],
    dataset: str,
    required_columns: Optional[List[str]] = None,
) -> DataFrame:
    """Read a dataset by name and assert its columns, whatever it is stored as.

    Returns the DataFrame so the caller reads once. Unlike :func:`require_table` this
    works for file-backed datasets too, so a source can move to Parquet without the
    validation needing a table name.
    """
    spec = dataset_spec(cfg, dataset)
    location = describe(cfg, dataset)
    table = spec.get("table")
    if table and str(spec.get("format", "delta")).lower() == "delta" and not spark.catalog.tableExists(table):
        raise ValueError(
            f"Dataset '{dataset}' points at {table}, which does not exist. Either create it, point "
            f"'{dataset}' somewhere else in conf/storage.config, or turn off the step that reads it."
        )

    df = read(spark, cfg, dataset)
    missing = [c for c in (required_columns or []) if c not in set(df.columns)]
    if missing:
        raise ValueError(
            f"Dataset '{dataset}' at {location} is missing required columns {missing}. "
            "If it was created from an older setup SQL, re-run sql/setup_tables.sql; "
            "if it is file-backed, check the path in storage.config."
        )
    return df


def apply_enrichment(source: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """Overlay reviewed address enrichment from the 'enriched_operators' dataset, where present."""
    if not bool(cfg.get("EnrichDate", False)):
        return source

    spark = source.sparkSession
    key_column = cfg["rowRegistryKeyColumn"]
    source_columns = [source_column for source_column, _ in ENRICHMENT_COLUMN_MAPPINGS]
    enrichment = require_dataset(
        spark, cfg, "enriched_operators", [key_column, *[enriched for _, enriched in ENRICHMENT_COLUMN_MAPPINGS]]
    )
    require_dataframe_columns(source, source_columns, "Source DataFrame for enrichment")
    if "match_found" in enrichment.columns:
        enrichment = enrichment.filter(
            F.lower(F.coalesce(F.col("match_found").cast("string"), F.lit("false"))) == F.lit("true")
        )

    source_types = {field.name: field.dataType for field in source.schema.fields}
    enrichment = (
        enrichment.select(
            F.trim(F.coalesce(F.col(key_column).cast("string"), F.lit(""))).alias(key_column),
            *[
                F.col(enriched).cast(source_types[source_column]).alias(f"enriched__{source_column}")
                for source_column, enriched in ENRICHMENT_COLUMN_MAPPINGS
            ],
        )
        .filter(F.col(key_column) != "")
        .dropDuplicates([key_column])
    )

    enriched = source.join(enrichment, key_column, "left")
    for column in source_columns:
        enriched = enriched.withColumn(column, F.coalesce(F.col(f"enriched__{column}"), F.col(column))).drop(
            f"enriched__{column}"
        )
    return enriched
