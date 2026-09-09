"""MDM country/runtime configuration.

Country match rules live under conf/countries/{CC}.json.
For a new country, copy conf/countries/template.json (reference only; not loaded)
to conf/countries/{CC}.json and edit it.

Table defaults target Databricks Unity Catalog / Hive metastore names
used by the original notebook; override per environment as needed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

# Reference shape for new countries: conf/countries/template.json (not loaded at runtime).

DEFAULT_SOURCE_TABLE = "sl_bdl_processed_cd_prod.cd.vw_ufsoperator"
DEFAULT_TARGET_SCHEMA = "pds_auroradsar_prod.schema_informatica"

# Source inputs (see matching.sources). Two Databricks views feed the engine:
#   - operator ("non-golden"): the population to match; one row per OperatorConcatId,
#     each carrying its Informatica GoldenRecordId (its group) when already matched.
#   - golden ("already matched"): the surviving master record per GoldenRecordId
#     (no OperatorConcatId). Used to preserve groups whose master is absent from the
#     operator feed (see matching.sources.build_source_population).
# Each spec is {"format": "delta"|"csv"|"parquet", "table"|"path": ..., "options": {...}}.
# Keeping them declarative lets a deployment swap Delta tables for CSV/Parquet files
# without touching engine code.
DEFAULT_SOURCE = {"format": "delta", "table": DEFAULT_SOURCE_TABLE}
DEFAULT_GOLDEN_SOURCE = {"format": "delta", "table": "sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden"}
# Prefix for the synthetic OperatorConcatId given to golden masters that are missing
# from the operator feed (so their Informatica group is still preserved).
GOLDEN_MASTER_KEY_PREFIX = "GRID_"
DEFAULT_ROW_REGISTRY_TABLE = "MDMRowRegistry"
DEFAULT_ROW_REGISTRY_KEY_COLUMN = "OperatorConcatId"
DEFAULT_EXACT_MAX_BLOCK_SIZE = 50000
DEFAULT_FUZZY_MAX_BLOCK_SIZE = 500
DEFAULT_COMPONENTS_MAX_ITERATIONS = 30

# Golden ID continuity (see docs/ARCHITECTURE.md "Golden IDs").
# Engine-minted golden IDs are allocated from a Delta-backed sequence and are
# always >= golden_id_floor AND > every golden id already known to the registry
# (Informatica SourceGoldenRecordId or engine MDMGoldenId). The floor keeps the
# engine range disjoint from the range Informatica can still reach for
# countries it continues to serve. Override per country via "golden_id_floor";
# the value must be identical in every country config and may only ever be raised.
# Informatica GoldenRecordId was confirmed at ~9 999 993 (Sep 2026) and still grows
# slowly for non-migrated countries; 1e8 gives 10x headroom with 9-digit engine ids.
DEFAULT_GOLDEN_ID_FLOOR = 100_000_000
DEFAULT_GOLDEN_ID_SEQUENCE_NAME = "MDMGoldenId"
GOLDEN_ID_SOURCE_INFORMATICA = "INFORMATICA"
GOLDEN_ID_SOURCE_ENGINE = "ENGINE"

# Informatica groupings as hard links (see docs/ARCHITECTURE.md "Source golden groups").
# preserve_source_golden_groups: records sharing a non-null SourceGoldenRecordId are
#   linked by star edges before connected components, so an Informatica group can never
#   be split and always keeps its id (also for records excluded from new matching).
# allow_source_golden_group_merge: when False, engine edges that would put two different
#   Informatica ids into one component are dropped (and recorded) so no Informatica id
#   ever changes; when True the component keeps one id and logs the other as MERGE.
DEFAULT_PRESERVE_SOURCE_GOLDEN_GROUPS = True
DEFAULT_ALLOW_SOURCE_GOLDEN_GROUP_MERGE = False
SOURCE_GOLDEN_GROUP_RULE_NAME = "Source_GoldenRecordId"
SOURCE_GOLDEN_GROUP_RULE_PRIORITY = 0
SOURCE_GOLDEN_GROUP_EDGE_TYPE = "source_golden"
SOURCE_GOLDEN_GROUP_BLOCK_NAME = "source_golden_group"
SOURCE_GOLDEN_GROUP_RULE_STAGE = "000_Source_GoldenRecordId"
BLOCKED_SOURCE_GROUP_MERGE_RULE_STAGE = "999_Blocked_Source_GoldenRecordId_Merge"

# DEPRECATED: pre-registry runs derived synthetic ids as OFFSET + min(MDMRowId).
# No longer used by the engine (ids were unstable and could collide with
# Informatica ids). Kept only so older notebooks importing it do not break.
SYNTHETIC_GOLDEN_ID_OFFSET = 100000000
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

# conf/ location. Defaults to the repo layout (repo_root/conf) but can be pointed
# elsewhere via the MDM_CONF_DIR env var — useful on Databricks when the code is
# installed as a wheel and configs live in a Workspace / DBFS / Volume path.
_CONF_ROOT = (
    Path(os.environ["MDM_CONF_DIR"]).resolve()
    if os.environ.get("MDM_CONF_DIR")
    else Path(__file__).resolve().parents[2] / "conf"
)
_CONF_DIR = _CONF_ROOT / "countries"
# Master defaults shared by every country (EnrichDate, standardization, match rules,
# invalid_values, source specs, ...). A country file only carries its overrides.
_BASE_CONFIG_PATH = _CONF_ROOT / "base.json"


def _is_country_config_file(path: Path) -> bool:
    """True if path is a loadable country config (skips template.json and _*.json)."""
    stem = path.stem
    if stem.startswith("_"):
        return False
    if stem.lower() == "template":
        return False
    return path.suffix.lower() == ".json"


def load_base_config(base_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the shared master config (conf/base.json). Returns {} if absent."""
    path = base_path or _BASE_CONFIG_PATH
    if not path.is_file():
        return {}
    return json.loads(path.read_text())


def _merge_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow-merge a country override onto the base master config.

    Top-level keys in the country file replace those from base (a country that sets
    e.g. its own ``standardization`` or ``exact_match_rules`` fully overrides the base
    value; keys it omits are inherited). Kept shallow so behaviour stays predictable.
    """
    merged = dict(base)
    merged.update(override)
    return merged


def load_country_config(
    country_code: str,
    conf_dir: Optional[Path] = None,
    base_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Load conf/base.json merged with conf/countries/{CC}.json (country wins).

    Raises FileNotFoundError if the country file is missing.
    """
    directory = conf_dir or _CONF_DIR
    cc = country_code.upper()
    path = directory / f"{cc}.json"
    if not _is_country_config_file(path) or not path.is_file():
        raise FileNotFoundError(
            f"Missing country config: {path}. "
            f"Copy conf/countries/template.json to conf/countries/{cc}.json and edit it."
        )
    base = load_base_config(base_path)
    return _merge_config(base, json.loads(path.read_text()))


def load_all_country_configs(
    conf_dir: Optional[Path] = None,
    base_path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load every conf/countries/*.json (except template.json / _*.json), each merged
    onto conf/base.json.

    Raises FileNotFoundError if the directory is missing or contains no country configs.
    """
    directory = conf_dir or _CONF_DIR
    if not directory.is_dir():
        raise FileNotFoundError(
            f"Country config directory not found: {directory}. "
            "Add conf/countries/{CC}.json files (copy from template.json)."
        )
    base = load_base_config(base_path)
    configs: Dict[str, Dict[str, Any]] = {}
    for path in sorted(directory.glob("*.json")):
        if not _is_country_config_file(path):
            continue
        configs[path.stem.upper()] = _merge_config(base, json.loads(path.read_text()))
    if not configs:
        raise FileNotFoundError(
            f"No country configs found in {directory}. "
            "Add conf/countries/{CC}.json files (copy from template.json; "
            "template.json and _*.json are skipped)."
        )
    return configs


def _resolve_source_spec(cfg: Dict[str, Any], key: str, default: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a source spec dict for the given key.

    Accepts a declarative dict ({"format","table"/"path","options"}) or a bare string
    (treated as a Delta table name for backward compatibility). Falls back to default.
    """
    spec = cfg.get(key, default)
    if spec is None:
        return None
    if isinstance(spec, str):
        return {"format": "delta", "table": spec}
    return dict(spec)


def _runtime_cfg(country_code: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    country_path = country_code.lower()
    target_schema = cfg.get("target_schema", DEFAULT_TARGET_SCHEMA)
    # Backward compat: a legacy "source_table" string still works as the operator source.
    source_spec = _resolve_source_spec(cfg, "source", cfg.get("source_table", DEFAULT_SOURCE))
    golden_source_spec = _resolve_source_spec(cfg, "golden_source", DEFAULT_GOLDEN_SOURCE)
    source_table = source_spec.get("table") if source_spec else DEFAULT_SOURCE_TABLE
    filter_condition = cfg.get("filter_condition") or f"CountryCode = '{country_code.upper()}'"
    return {
        **cfg,
        "target_schema": target_schema,
        "source": source_spec,
        "golden_source": golden_source_spec,
        "source_table": source_table,
        "filter_condition": filter_condition,
        "rowRegistryTable": f"{target_schema}.{DEFAULT_ROW_REGISTRY_TABLE}",
        "rowRegistryKeyColumn": DEFAULT_ROW_REGISTRY_KEY_COLUMN,
        "matchLinksTempView": f"tmp_match_links_{country_path}",
        "ruleResultsTable": f"{target_schema}.MDMRuleResults",
        "ruleEvaluationsTable": f"{target_schema}.MDMRuleEvaluations",
        "matchExclusionsTable": f"{target_schema}.MDMMatchExclusions",
        "enrichedOperatorsTable": f"{target_schema}.mdmenrichedoperators",
        "matchingStateTable": f"{target_schema}.MDMMatchingState",
        "matchLinksTable": f"{target_schema}.MDMMatchLinks",
        "componentLabelsTable": f"{target_schema}.MDMComponentLabels",
        "matchedResultsTable": f"{target_schema}.MDMMatchedResults",
        "goldenIdHistoryTable": f"{target_schema}.MDMGoldenIdHistory",
        "goldenIdSequenceTable": f"{target_schema}.MDMGoldenIdSequence",
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
