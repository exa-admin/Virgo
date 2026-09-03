"""MDM country/runtime configuration.

Country match rules live under conf/countries/{CC}.json.
For a new country, copy conf/countries/template.json (reference only; not loaded)
to conf/countries/{CC}.json and edit it.

Table defaults target Databricks Unity Catalog / Hive metastore names
used by the original notebook; override per environment as needed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

# Reference shape for new countries: conf/countries/template.json (not loaded at runtime).

DEFAULT_SOURCE_TABLE = "sources_informatica.ufsoperator"
DEFAULT_TARGET_SCHEMA = "pds_auroradsar_prod.schema_informatica"
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
# countries it continues to serve. Override per country via "golden_id_floor".
DEFAULT_GOLDEN_ID_FLOOR = 1_000_000_000
DEFAULT_GOLDEN_ID_SEQUENCE_NAME = "MDMGoldenId"
GOLDEN_ID_SOURCE_INFORMATICA = "INFORMATICA"
GOLDEN_ID_SOURCE_ENGINE = "ENGINE"

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

_CONF_DIR = Path(__file__).resolve().parents[2] / "conf" / "countries"


def _is_country_config_file(path: Path) -> bool:
    """True if path is a loadable country config (skips template.json and _*.json)."""
    stem = path.stem
    if stem.startswith("_"):
        return False
    if stem.lower() == "template":
        return False
    return path.suffix.lower() == ".json"


def load_country_config(country_code: str, conf_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Load conf/countries/{CC}.json. Raises FileNotFoundError if missing."""
    directory = conf_dir or _CONF_DIR
    cc = country_code.upper()
    path = directory / f"{cc}.json"
    if not _is_country_config_file(path) or not path.is_file():
        raise FileNotFoundError(
            f"Missing country config: {path}. "
            f"Copy conf/countries/template.json to conf/countries/{cc}.json and edit it."
        )
    return json.loads(path.read_text())


def load_all_country_configs(conf_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Load all conf/countries/*.json files except template.json and _*.json.

    Raises FileNotFoundError if the directory is missing or contains no country configs.
    """
    directory = conf_dir or _CONF_DIR
    if not directory.is_dir():
        raise FileNotFoundError(
            f"Country config directory not found: {directory}. "
            "Add conf/countries/{CC}.json files (copy from template.json)."
        )
    configs: Dict[str, Dict[str, Any]] = {}
    for path in sorted(directory.glob("*.json")):
        if not _is_country_config_file(path):
            continue
        configs[path.stem.upper()] = json.loads(path.read_text())
    if not configs:
        raise FileNotFoundError(
            f"No country configs found in {directory}. "
            "Add conf/countries/{CC}.json files (copy from template.json; "
            "template.json and _*.json are skipped)."
        )
    return configs


def _runtime_cfg(country_code: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    country_path = country_code.lower()
    return {
        **cfg,
        "source_table": DEFAULT_SOURCE_TABLE,
        "rowRegistryTable": f"{DEFAULT_TARGET_SCHEMA}.{DEFAULT_ROW_REGISTRY_TABLE}",
        "rowRegistryKeyColumn": DEFAULT_ROW_REGISTRY_KEY_COLUMN,
        "matchLinksTempView": f"tmp_match_links_{country_path}",
        "ruleResultsTable": f"{DEFAULT_TARGET_SCHEMA}.MDMRuleResults",
        "ruleEvaluationsTable": f"{DEFAULT_TARGET_SCHEMA}.MDMRuleEvaluations",
        "matchExclusionsTable": f"{DEFAULT_TARGET_SCHEMA}.MDMMatchExclusions",
        "enrichedOperatorsTable": f"{DEFAULT_TARGET_SCHEMA}.mdmenrichedoperators",
        "matchingStateTable": f"{DEFAULT_TARGET_SCHEMA}.MDMMatchingState",
        "matchLinksTable": f"{DEFAULT_TARGET_SCHEMA}.MDMMatchLinks",
        "componentLabelsTable": f"{DEFAULT_TARGET_SCHEMA}.MDMComponentLabels",
        "matchedResultsTable": f"{DEFAULT_TARGET_SCHEMA}.MDMMatchedResults",
        "goldenIdHistoryTable": f"{DEFAULT_TARGET_SCHEMA}.MDMGoldenIdHistory",
        "goldenIdSequenceTable": f"{DEFAULT_TARGET_SCHEMA}.MDMGoldenIdSequence",
        "exact_max_block_size": DEFAULT_EXACT_MAX_BLOCK_SIZE,
        "fuzzy_max_block_size": DEFAULT_FUZZY_MAX_BLOCK_SIZE,
        "golden_id_floor": int(cfg.get("golden_id_floor", DEFAULT_GOLDEN_ID_FLOOR)),
        "golden_id_sequence_name": str(cfg.get("golden_id_sequence_name", DEFAULT_GOLDEN_ID_SEQUENCE_NAME)),
        "components_max_iterations": int(cfg.get("components_max_iterations", DEFAULT_COMPONENTS_MAX_ITERATIONS)),
    }
