"""Config loading and runtime table names.

Two files, separate concerns:

* ``conf/storage.config`` — **where the data lives**: every source, table, view and file
  path the engine touches. Nothing storage-related is hardcoded in Python; to move a
  dataset from a Delta table to Parquet/CSV files, edit that file only.
* ``conf/base.json`` + ``conf/countries/{CC}.json`` — **how matching behaves** (rules,
  thresholds, exclusions). Layered, country wins.

``conf/`` ships inside the wheel. ``MDM_CONF_DIR`` relocates the whole folder;
``MDM_STORAGE_CONFIG`` points at ``storage.config`` alone. Either way the packaged copy
is the fallback, so a partial override folder never loses the storage defaults.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

STORAGE_CONFIG_FILENAME = "storage.config"
SCHEMA_PLACEHOLDER = "${schema}"
# Datasets the engine only reads (any format) vs. the Delta tables it also writes.
STORAGE_SOURCES_KEY = "sources"
STORAGE_TABLES_KEY = "tables"

ROW_REGISTRY_KEY_COLUMN = "OperatorConcatId"
# Synthetic key for golden masters missing from the operator feed (see io.read_source_population).
GOLDEN_MASTER_KEY_PREFIX = "GRID_"

DEFAULT_EXACT_MAX_BLOCK_SIZE = 50000
DEFAULT_FUZZY_MAX_BLOCK_SIZE = 500
DEFAULT_COMPONENTS_MAX_ITERATIONS = 30

# Engine-minted golden ids are always >= this floor and above every id the registry knows,
# keeping them disjoint from the range Informatica can still reach for countries it serves.
# Must be identical in every country config and may only ever be raised.
DEFAULT_GOLDEN_ID_FLOOR = 100_000_000
DEFAULT_GOLDEN_ID_SEQUENCE_NAME = "MDMGoldenId"
GOLDEN_ID_SOURCE_INFORMATICA = "INFORMATICA"
GOLDEN_ID_SOURCE_ENGINE = "ENGINE"

# Informatica groupings replayed as hard match links (docs/ARCHITECTURE.md).
DEFAULT_PRESERVE_SOURCE_GOLDEN_GROUPS = True
DEFAULT_ALLOW_SOURCE_GOLDEN_GROUP_MERGE = False
SOURCE_GOLDEN_GROUP_RULE_NAME = "Source_GoldenRecordId"
SOURCE_GOLDEN_GROUP_RULE_PRIORITY = 0
SOURCE_GOLDEN_GROUP_EDGE_TYPE = "source_golden"
SOURCE_GOLDEN_GROUP_BLOCK_NAME = "source_golden_group"
SOURCE_GOLDEN_GROUP_RULE_STAGE = "000_Source_GoldenRecordId"
BLOCKED_SOURCE_GROUP_MERGE_RULE_STAGE = "999_Blocked_Source_GoldenRecordId_Merge"

# (source column, enriched column) pairs coalesced in from the enrichment table.
ENRICHMENT_COLUMN_MAPPINGS = [
    ("OperatorName", "OperatorName"),
    ("HouseNumberText", "HouseNumberText"),
    ("StreetText", "StreetText"),
    ("CityText", "CityText"),
    ("StateText", "StateText"),
    ("CountryName", "CountryName"),
    ("LatitudeText", "latitude"),
    ("LongitudeText", "longitude"),
    ("ZipCode", "ZipCode"),
]


def conf_dir() -> Path:
    """Root of the config tree: ``MDM_CONF_DIR`` if set, else the packaged ``conf/``."""
    override = os.environ.get("MDM_CONF_DIR")
    return Path(override).resolve() if override else Path(__file__).resolve().parent / "conf"


def storage_config_path() -> Path:
    """Where ``storage.config`` lives.

    ``MDM_STORAGE_CONFIG`` (a file) wins, then a copy inside ``MDM_CONF_DIR``, then the
    one packaged in the wheel. The fallback matters: an override folder holding only
    ``base.json`` still gets the packaged storage defaults.
    """
    override = os.environ.get("MDM_STORAGE_CONFIG")
    if override:
        return Path(override).resolve()
    candidate = conf_dir() / STORAGE_CONFIG_FILENAME
    return candidate if candidate.is_file() else Path(__file__).resolve().parent / "conf" / STORAGE_CONFIG_FILENAME


def _without_comments(node: Dict[str, Any]) -> Dict[str, Any]:
    """Drop ``_comment`` documentation keys so they never reach a dataset spec."""
    return {key: value for key, value in node.items() if not key.startswith("_")}


def _expand_spec(spec: Any, schema: str, name: str) -> Dict[str, Any]:
    """One dataset spec with ``${schema}`` expanded. A bare string means a Delta table."""
    if isinstance(spec, str):
        spec = {"format": "delta", "table": spec}
    if not isinstance(spec, dict):
        raise ValueError(f"storage.config: dataset '{name}' must be an object or a table name, got {type(spec).__name__}")

    resolved = dict(spec)
    for key in ("table", "path"):
        if isinstance(resolved.get(key), str):
            resolved[key] = resolved[key].replace(SCHEMA_PLACEHOLDER, schema)
    resolved.setdefault("format", "delta")
    if not resolved.get("table") and not resolved.get("path"):
        raise ValueError(f"storage.config: dataset '{name}' needs a 'table' (delta) or a 'path' (file formats)")
    return resolved


def load_storage_config(path: Optional[Path] = None, schema_override: Optional[str] = None) -> Dict[str, Any]:
    """Parse ``storage.config`` into ``{"schema": str, "datasets": {name: spec}}``.

    Every dataset is returned fully expanded, so callers never see ``${schema}``.
    ``schema_override`` (the ``target_schema`` of a base/country config) replaces the
    file's own schema before expansion, which is how the tests retarget every engine
    table at a scratch schema in one move.
    """
    path = Path(path) if path is not None else storage_config_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"Storage config not found: {path}. It ships in the wheel at matching/conf/{STORAGE_CONFIG_FILENAME}; "
            "set MDM_STORAGE_CONFIG or put a copy in MDM_CONF_DIR to override it."
        )
    raw = json.loads(path.read_text())
    schema = str(schema_override or raw.get("schema") or "").strip()
    if not schema:
        raise ValueError(f"storage.config: 'schema' is required ({path})")

    datasets, writable = {}, set()
    for group in (STORAGE_SOURCES_KEY, STORAGE_TABLES_KEY):
        for name, spec in _without_comments(dict(raw.get(group) or {})).items():
            datasets[name] = _expand_spec(spec, schema, name)
            if group == STORAGE_TABLES_KEY:
                writable.add(name)
    if not datasets:
        raise ValueError(f"storage.config: no datasets defined under '{STORAGE_SOURCES_KEY}' / '{STORAGE_TABLES_KEY}' ({path})")

    return {"schema": schema, "datasets": datasets, "writable": writable, "path": str(path)}


def _is_country_file(path: Path) -> bool:
    """Loadable country config? Skips template.json and _*.json."""
    return path.suffix.lower() == ".json" and not path.stem.startswith("_") and path.stem.lower() != "template"


def _load_base(root: Path) -> Dict[str, Any]:
    path = root / "base.json"
    return json.loads(path.read_text()) if path.is_file() else {}


def _merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow merge: a top-level key in the country file replaces the base value entirely."""
    return {**base, **override}


def load_country_config(country_code: str, root: Optional[Path] = None) -> Dict[str, Any]:
    """Load base.json merged with countries/{CC}.json. Raises if the country file is missing."""
    root = root or conf_dir()
    code = country_code.upper()
    path = root / "countries" / f"{code}.json"
    if not _is_country_file(path) or not path.is_file():
        raise FileNotFoundError(
            f"Missing country config: {path}. Copy countries/template.json to countries/{code}.json and edit it."
        )
    return _merge(_load_base(root), json.loads(path.read_text()))


def load_all_country_configs(root: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Load every countries/*.json (template.json and _*.json are skipped)."""
    root = root or conf_dir()
    directory = root / "countries"
    if not directory.is_dir():
        raise FileNotFoundError(f"Country config directory not found: {directory}")
    configs = {
        path.stem.upper(): _merge(_load_base(root), json.loads(path.read_text()))
        for path in sorted(directory.glob("*.json"))
        if _is_country_file(path)
    }
    if not configs:
        raise FileNotFoundError(f"No country configs found in {directory} (copy template.json to {{CC}}.json).")
    return configs


def available_countries(root: Optional[Path] = None) -> list[str]:
    """Country codes that have a config file, in load order."""
    directory = (root or conf_dir()) / "countries"
    if not directory.is_dir():
        return []
    return [p.stem.upper() for p in sorted(directory.glob("*.json")) if _is_country_file(p)]


def _source_spec(cfg: Dict[str, Any], key: str, default: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Normalize a source spec; a bare string is treated as a Delta table name."""
    spec = cfg.get(key, default)
    if spec is None:
        return None
    return {"format": "delta", "table": spec} if isinstance(spec, str) else dict(spec)


# storage.config dataset -> the config key the engine reads it under.
# Specs (any format, read through io.read); the two the base/country config may override.
SOURCE_SPEC_KEYS = {"operator": "source", "golden": "golden_source"}
# Delta tables the engine names directly in SQL / saveAsTable, so these need a table name.
TABLE_NAME_KEYS = {
    "row_registry": "rowRegistryTable",
    "golden_id_sequence": "goldenIdSequenceTable",
    "golden_id_history": "goldenIdHistoryTable",
    "change_log": "changeLogTable",
    "rule_results": "ruleResultsTable",
    "rule_evaluations": "ruleEvaluationsTable",
    "match_exclusions": "matchExclusionsTable",
    "matching_state": "matchingStateTable",
    "match_links": "matchLinksTable",
    "component_labels": "componentLabelsTable",
    "matched_results": "matchedResultsTable",
}


def dataset_spec(cfg: Dict[str, Any], name: str) -> Dict[str, Any]:
    """The storage spec for one dataset of a resolved config. Raises if it is not defined."""
    datasets = (cfg.get("storage") or {}).get("datasets") or {}
    if name not in datasets:
        raise KeyError(f"Unknown dataset '{name}'. Defined in storage.config: {sorted(datasets)}")
    return datasets[name]


def dataset_table(cfg: Dict[str, Any], name: str) -> str:
    """The table name of a dataset — for SQL and ``saveAsTable``, which cannot take a path."""
    spec = dataset_spec(cfg, name)
    table = spec.get("table")
    if not table:
        raise ValueError(
            f"Dataset '{name}' is configured as {spec.get('format')} at '{spec.get('path')}', but this use "
            "needs a table name. Point it at a Delta table in storage.config, or read it with io.read()."
        )
    return str(table)


def resolve_config(country_code: str, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Effective config for a country: base defaults + country file (or ``cfg``) + storage.

    A caller-supplied ``cfg`` still gets base.json merged underneath it, so a partial
    override never loses the global settings.

    Storage names all come from ``storage.config``. ``target_schema`` / ``source`` /
    ``golden_source`` in base.json or a country file still override it, so an
    environment can be retargeted without editing the storage file.
    """
    cfg = load_country_config(country_code) if cfg is None else _merge(_load_base(conf_dir()), cfg)

    storage = load_storage_config(schema_override=cfg.get("target_schema"))
    schema = storage["schema"]
    datasets = dict(storage["datasets"])

    # A source/golden_source override in base.json or a country file wins over storage.config
    # (source_table is the legacy spelling of source).
    for dataset_name, config_key in SOURCE_SPEC_KEYS.items():
        override = _source_spec(cfg, config_key, None)
        if override is None and dataset_name == "operator":
            override = _source_spec(cfg, "source_table", None)
        if override is not None:
            datasets[dataset_name] = override
        elif cfg.get(config_key, "") is None:
            # Explicit null disables the dataset (golden_source: null skips the backfill).
            datasets.pop(dataset_name, None)

    missing = [name for name in TABLE_NAME_KEYS if name not in datasets]
    if missing:
        raise ValueError(f"storage.config is missing required tables {missing} (see {storage['path']})")

    resolved_storage = {**storage, "datasets": datasets}
    storage_only = {"storage": resolved_storage}
    table_names = {
        config_key: dataset_table(storage_only, dataset_name)
        for dataset_name, config_key in TABLE_NAME_KEYS.items()
    }

    return {
        **cfg,
        "storage": resolved_storage,
        "target_schema": schema,
        "source": datasets.get("operator"),
        "golden_source": datasets.get("golden"),
        "filter_condition": cfg.get("filter_condition") or f"CountryCode = '{country_code.upper()}'",
        "rowRegistryKeyColumn": ROW_REGISTRY_KEY_COLUMN,
        "matchLinksTempView": f"tmp_match_links_{country_code.lower()}",
        **table_names,
        "exact_max_block_size": DEFAULT_EXACT_MAX_BLOCK_SIZE,
        "fuzzy_max_block_size": DEFAULT_FUZZY_MAX_BLOCK_SIZE,
        "golden_id_floor": int(cfg.get("golden_id_floor", DEFAULT_GOLDEN_ID_FLOOR)),
        "golden_id_sequence_name": str(cfg.get("golden_id_sequence_name", DEFAULT_GOLDEN_ID_SEQUENCE_NAME)),
        "components_max_iterations": int(cfg.get("components_max_iterations", DEFAULT_COMPONENTS_MAX_ITERATIONS)),
        "preserve_source_golden_groups": bool(
            cfg.get("preserve_source_golden_groups", DEFAULT_PRESERVE_SOURCE_GOLDEN_GROUPS)
        ),
        "allow_source_golden_group_merge": bool(
            cfg.get("allow_source_golden_group_merge", DEFAULT_ALLOW_SOURCE_GOLDEN_GROUP_MERGE)
        ),
    }
