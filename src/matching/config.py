"""MDM country/runtime configuration.

Country match rules live under conf/countries/*.json.
Table defaults target Databricks Unity Catalog / Hive metastore names
used by the original notebook; override per environment as needed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

# Embedded fallback: same MY config as conf/countries/MY.json (for notebook paste / offline).
CONFIG_JSON = """
{
  "MY": {
    "filter_condition": "CountryCode = 'MY'",
    "EnrichDate": true,
    "priorityMatching": true,
    "invalid_values": [
      "",
      "null",
      "nan",
      "na",
      "n/a",
      "0",
      "00",
      "000",
      "0000",
      "dummy",
      "test"
    ],
    "exclude_from_match_filters": [
      "lower(OperatorName) LIKE '%invalid%'",
      "lower(OperatorName) LIKE '%dummy%'",
      "ZipCode IN ('00000', '99999')"
    ],
    "standardization": {
      "name_column": "OperatorName",
      "city_column": "CityText",
      "state_column": "StateText",
      "zip_column": "ZipCode",
      "address_columns": [
        "HouseNumberText",
        "HouseNumberExtensionText",
        "StreetText"
      ]
    },
    "exact_match_rules": [
      {
        "rule_name": "Exact_OperatorName_Zip",
        "priority": 10,
        "columns": [
          {
            "column": "c_name_exact",
            "method": "exact_not_empty",
            "min_length": 3
          },
          {
            "column": "c_zip_exact",
            "method": "exact_not_empty",
            "min_length": 3
          }
        ],
        "match_exclusion_filter": [
          "c_zip_exact IN ('00000', '99999')"
        ]
      },
      {
        "rule_name": "Exact_SAP_Customer_ID",
        "priority": 20,
        "columns": [
          {
            "column": "SAPCustomerID",
            "method": "exact_not_empty",
            "min_length": 3
          }
        ],
        "match_exclusion_filter": [
          "OTMText IN ('A++', 'DUMMY')"
        ]
      },
      {
        "rule_name": "Exact_Latitude_Longitude",
        "priority": 30,
        "columns": [
          {
            "column": "LatitudeText",
            "method": "exact_raw_not_empty",
            "min_length": 3
          },
          {
            "column": "LongitudeText",
            "method": "exact_raw_not_empty",
            "min_length": 3
          }
        ]
      }
    ],
    "default_fuzzy_blocking": [
      {
        "block_name": "zip_name_prefix",
        "columns": [
          "c_zip",
          "c_name_prefix4"
        ],
        "max_block_size": 1000
      },
      {
        "block_name": "city_name_soundex",
        "columns": [
          "c_city",
          "c_name_soundex"
        ],
        "max_block_size": 300
      },
      {
        "block_name": "city_name_prefix",
        "columns": [
          "c_city",
          "c_name_prefix4"
        ],
        "max_block_size": 300
      },
      {
        "block_name": "state_name_soundex",
        "columns": [
          "c_state",
          "c_name_soundex"
        ],
        "max_block_size": 300
      },
      {
        "block_name": "state_name_prefix",
        "columns": [
          "c_state",
          "c_name_prefix4"
        ],
        "max_block_size": 300
      }
    ],
    "fuzzy_match_rules": [
      {
        "rule_name": "Fuzzy_Name_Address_Zip",
        "priority": 100,
        "blocking_names": [
          "zip_name_prefix",
          "city_name_soundex",
          "city_name_prefix"
        ],
        "conditions": [
          {
            "column": "c_name",
            "method": "levenshtein_similarity",
            "min": 0.86,
            "min_length": 4
          },
          {
            "column": "c_address",
            "method": "levenshtein_or_token_jaccard",
            "min": 0.7,
            "min_length": 3,
            "min_token_length": 2
          },
          {
            "column": "c_zip",
            "method": "exact_not_empty",
            "min_length": 3
          }
        ],
        "decision": "all",
        "match_exclusion_filter": [
          "c_address = 'test'"
        ]
      },
      {
        "rule_name": "Fuzzy_Name_Address_City",
        "priority": 110,
        "blocking_names": [
          "city_name_soundex",
          "city_name_prefix"
        ],
        "conditions": [
          {
            "column": "c_name",
            "method": "levenshtein_similarity",
            "min": 0.86,
            "min_length": 4
          },
          {
            "column": "c_address",
            "method": "levenshtein_or_token_jaccard",
            "min": 0.7,
            "min_length": 3,
            "min_token_length": 2
          },
          {
            "column": "c_city",
            "method": "exact_not_empty",
            "min_length": 2
          }
        ],
        "decision": "all",
        "match_exclusion_filter": [
          "c_address = 'test'"
        ]
      },
      {
        "rule_name": "Fuzzy_Name_Address_State",
        "priority": 120,
        "blocking_names": [
          "state_name_soundex",
          "state_name_prefix"
        ],
        "conditions": [
          {
            "column": "c_name",
            "method": "levenshtein_similarity",
            "min": 0.86,
            "min_length": 4
          },
          {
            "column": "c_address",
            "method": "levenshtein_or_token_jaccard",
            "min": 0.7,
            "min_length": 3,
            "min_token_length": 2
          },
          {
            "column": "c_state",
            "method": "exact_not_empty",
            "min_length": 2
          }
        ],
        "decision": "all",
        "match_exclusion_filter": [
          "c_address = 'test'"
        ]
      }
    ]
  }
}
"""

DEFAULT_SOURCE_TABLE = "sources_informatica.ufsoperator"
DEFAULT_TARGET_SCHEMA = "pds_auroradsar_prod.schema_informatica"
DEFAULT_ROW_REGISTRY_TABLE = "MDMRowRegistry"
DEFAULT_ROW_REGISTRY_KEY_COLUMN = "OperatorConcatId"
DEFAULT_EXACT_MAX_BLOCK_SIZE = 50000
DEFAULT_FUZZY_MAX_BLOCK_SIZE = 500
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


def load_country_config(country_code: str, conf_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Load a single country config from conf/countries/{CC}.json, else CONFIG_JSON fallback."""
    directory = conf_dir or _CONF_DIR
    path = directory / f"{country_code.upper()}.json"
    if path.is_file():
        return json.loads(path.read_text())
    all_cfg = json.loads(CONFIG_JSON)
    if country_code.upper() not in all_cfg and country_code not in all_cfg:
        raise KeyError(f"No config for country {country_code!r} in {path} or embedded CONFIG_JSON")
    return all_cfg.get(country_code.upper()) or all_cfg[country_code]


def load_all_country_configs(conf_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Load all conf/countries/*.json files; fall back to embedded CONFIG_JSON if none found."""
    directory = conf_dir or _CONF_DIR
    configs: Dict[str, Dict[str, Any]] = {}
    if directory.is_dir():
        for path in sorted(directory.glob("*.json")):
            configs[path.stem.upper()] = json.loads(path.read_text())
    if configs:
        return configs
    return {k.upper(): v for k, v in json.loads(CONFIG_JSON).items()}


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
        "exact_max_block_size": DEFAULT_EXACT_MAX_BLOCK_SIZE,
        "fuzzy_max_block_size": DEFAULT_FUZZY_MAX_BLOCK_SIZE,
    }
