"""Config loading and runtime table names.

Layered: ``conf/base.json`` (cross-country defaults) + ``conf/countries/{CC}.json``
(that country's match rules and overrides; country wins).

``conf/`` ships inside the wheel. Set ``MDM_CONF_DIR`` to load it from elsewhere
(Workspace / Volume / DBFS) without rebuilding.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_TARGET_SCHEMA = "pds_auroradsar_prod.schema_informatica"
DEFAULT_SOURCE = {"format": "delta", "table": "sl_bdl_processed_cd_prod.cd.vw_ufsoperator"}
DEFAULT_GOLDEN_SOURCE = {"format": "delta", "table": "sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden"}

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


def resolve_config(country_code: str, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Effective config for a country: base defaults + country file (or ``cfg``) + table names.

    A caller-supplied ``cfg`` still gets base.json merged underneath it, so a partial
    override never loses the global settings.
    """
    cfg = load_country_config(country_code) if cfg is None else _merge(_load_base(conf_dir()), cfg)

    schema = cfg.get("target_schema", DEFAULT_TARGET_SCHEMA)
    source = _source_spec(cfg, "source", cfg.get("source_table", DEFAULT_SOURCE))
    return {
        **cfg,
        "target_schema": schema,
        "source": source,
        "golden_source": _source_spec(cfg, "golden_source", DEFAULT_GOLDEN_SOURCE),
        "filter_condition": cfg.get("filter_condition") or f"CountryCode = '{country_code.upper()}'",
        "rowRegistryTable": f"{schema}.MDMRowRegistry",
        "rowRegistryKeyColumn": ROW_REGISTRY_KEY_COLUMN,
        "matchLinksTempView": f"tmp_match_links_{country_code.lower()}",
        "ruleResultsTable": f"{schema}.MDMRuleResults",
        "ruleEvaluationsTable": f"{schema}.MDMRuleEvaluations",
        "matchExclusionsTable": f"{schema}.MDMMatchExclusions",
        "enrichedOperatorsTable": f"{schema}.mdmenrichedoperators",
        "matchingStateTable": f"{schema}.MDMMatchingState",
        "matchLinksTable": f"{schema}.MDMMatchLinks",
        "componentLabelsTable": f"{schema}.MDMComponentLabels",
        "matchedResultsTable": f"{schema}.MDMMatchedResults",
        "goldenIdHistoryTable": f"{schema}.MDMGoldenIdHistory",
        "goldenIdSequenceTable": f"{schema}.MDMGoldenIdSequence",
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
