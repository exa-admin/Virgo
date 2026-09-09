# Customer MDM Match & Merge Engine

Databricks / Spark-native **Customer Master Data Management** Match engine (similar in role to Informatica MDM, Customer 360, and Adobe identity resolution).

This repository holds the **Python PySpark** match pipeline that assigns stable row IDs, runs exact and fuzzy rules in a priority waterfall, builds connected components without GraphFrames, and writes golden IDs plus stewardship tables to Delta Lake.

> **Base code decision:** Use this Python engine — not the Scala “improved” notebook prototype. Useful Scala ideas (match confidence score, dual-pass enriched vs original reporting, cache-before-count) may be ported into Python later; see [AGENTS.md](AGENTS.md).

## Architecture (summary)

```
Source → (optional enrichment) → Row registry → Standardize → Exclusions
  → Exact/Fuzzy waterfall → + Informatica group links (hard) → Match links
  → Connected components (→ drop Informatica-group bridges)
  → Golden IDs (Informatica id > prior engine id > mint ≥ 1e8) → MDMMatchedResults
```

Details and a mermaid flowchart: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Golden ID continuity (Informatica migration)

Informatica MDM is being replaced country by country. Policy: **existing Informatica groupings
keep their golden id; only genuinely new clusters get a new id.**

- **Informatica groups are hard links.** Records already grouped under one `GoldenRecordId` are
  linked by the engine (`match_rule = Source_GoldenRecordId`) before connected components, so a
  group can never be split and always keeps its id — even for records excluded from new matching
  (`preserve_source_golden_groups: true`).
- A **new record** (no Informatica id) that matches a group member **inherits the group's id**.
- A **brand-new cluster** is minted a BIGINT id from `MDMGoldenIdSequence`: always
  `>= golden_id_floor` and greater than every id already known. `golden_id_floor` is
  **`100000000` (1e8)**: Informatica is at ~1e7 (max `9999993` in Sep 2026) and still grows
  slowly for non-migrated countries; 1e8 gives 10x headroom with 9-digit ids. Use the same value
  in every country config and only ever raise it (the run fails if Informatica reaches the floor).
- **Two Informatica groups bridged by engine rules**: with `allow_source_golden_group_merge:
  false` (MY default) the bridging edges are dropped and recorded in `MDMRuleResults` stage
  `999_Blocked_Source_GoldenRecordId_Merge` for stewardship — no Informatica id ever changes.
  With `true`, one id survives and the other is logged as `MERGE` in `MDMGoldenIdHistory`.
- Otherwise a cluster keeps the id the engine assigned on a previous run (`MDMRowRegistry.MDMGoldenId`).
- Known Informatica ids are never overwritten with NULL once Informatica stops feeding a migrated
  country; a non-numeric `GoldenRecordId` string fails the run instead of silently becoming NULL.
- The run fails fast (before any write-back) if an Informatica id would end up split across
  components, on an engine-minted id, or — when merges are disallowed — changed at all.
- Every old → new remap is appended to `MDMGoldenIdHistory`; per-record detail is in
  `MDMMatchedResults` (`previous_golden_id`, `golden_id_changed`, `golden_id_differs_from_source`, `golden_id_source`).

Full policy, tie-breaking and safety checks: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#golden-ids-informatica-continuity).
Existing deployments: run the `ALTER TABLE … ADD COLUMNS …` and new `CREATE TABLE` statements in `sql/setup_tables.sql`.

## Repository layout

| Path | Role |
|------|------|
| `src/matching/` | Match pipeline package (current) |
| `src/matching/sources.py` | Source ingestion (delta/csv/parquet); operator + golden views |
| `src/merging/` | Survivorship / merge (future; not present yet) |
| `src/dq/` | Data quality utilities (e.g. address enrichment) |
| `conf/base.json` | Cross-country defaults (source specs, standardization, invalid_values, target_schema, …) |
| `conf/countries/{CC}.json` | Per-country match rules (exact/fuzzy/blocking) + any base overrides (e.g. `MY.json`) |
| `conf/countries/template.json` | Reference-only shape for new countries (not loaded) |
| `sql/setup_tables.sql` | Delta DDL for MDM tables |
| `scripts/build_databricks_bundle.sh` | Build the deployable zip + wheel (see `docs/DEPLOYMENT.md`) |
| `notebooks/01_run_match_country.py` | Thin Databricks entry notebook |
| `AGENTS.md` | Instructions for AI/coding sessions |
| `docs/ARCHITECTURE.md` | Pipeline deep-dive |

## Prerequisites

- Databricks workspace with Delta Lake / Unity Catalog (or compatible metastore)
- Source views `sl_bdl_processed_cd_prod.cd.vw_ufsoperator` (operators to match) and
  `…vw_ufsoperatorgoden` (golden masters) — configurable as delta/csv/parquet via
  `source` / `golden_source` in `conf/base.json` (see `src/matching/sources.py`)
- Optional enrichment table `…mdmenrichedoperators` when `EnrichDate` is true
- PySpark is provided by **Databricks Runtime** — local `pip install` is optional (for editing / type-checking only)

## Setup

1. Parameterize schema names in `sql/setup_tables.sql` if not using  
   `pds_auroradsar_prod.schema_informatica`.
2. Run `sql/setup_tables.sql` in a Databricks SQL / notebook cell.
3. Attach a cluster and add this repo’s `src/` to `sys.path` (or install the package on the cluster).

## Run

### Single country (recommended)

```python
import sys
sys.path.append("/Workspace/Repos/Engine/src")  # adjust path

from pyspark.sql import SparkSession
from matching.config import load_country_config
from matching.pipeline import run_country

spark = SparkSession.builder.getOrCreate()
cfg = load_country_config("MY")
run_country(spark, "MY", cfg)
```

Or open `notebooks/01_run_match_country.py` and run all cells.

### All countries in conf/

```python
from matching.pipeline import run_all
run_all(spark)  # loads conf/countries/*.json
```

## Configuration overview

Config is **layered**: `conf/base.json` holds the cross-country defaults (source/golden specs, `EnrichDate`, `standardization`, `invalid_values`, `exclude_from_match_filters`, `golden_id_floor`, `target_schema`, …), and each `conf/countries/{CC}.json` holds that country's **match rules** (`exact_match_rules`, `default_fuzzy_blocking`, `fuzzy_match_rules`) plus any base overrides, merged on top of base (country wins). `filter_condition` defaults to `CountryCode = '<CC>'`. Loaders raise `FileNotFoundError` if the country file is missing. To add a country, copy `conf/countries/template.json` → e.g. `SG.json` — `template.json` and `_*.json` are skipped by `load_all_country_configs`.

To deploy to Databricks (zip bundle or wheel), see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

Country JSON (e.g. `conf/countries/MY.json`) controls:

- `filter_condition` — source filter (`CountryCode = 'MY'`)
- `EnrichDate` — join Google Places / enriched operators when true
- `priorityMatching` — waterfall (already-matched records leave later rules as subjects only)
- `golden_id_floor` — lower bound for engine-minted golden ids (`100000000`; keep identical across countries, only ever raise)
- `preserve_source_golden_groups` — hard-link Informatica groupings so they never split or change id (default `true`)
- `allow_source_golden_group_merge` — let engine rules merge two Informatica groups (`false` for MY: bridging edges are dropped and recorded)
- `components_max_iterations` — connected-components iteration budget (run fails if not converged)
- `invalid_values`, `exclude_from_match_filters`
- `standardization` — name/city/state/zip/address columns
- `exact_match_rules` / `fuzzy_match_rules` — priorities, methods, exclusions
- `default_fuzzy_blocking` — named blocks referenced by fuzzy rules

Table names default via `matching.config._runtime_cfg` to  
`pds_auroradsar_prod.schema_informatica.*`.

## Conventions

- No Python UDFs; Spark SQL expressions only
- No GraphFrames / GraphX — connected components use min-label propagation with Delta materialization
- Country-partitioned Delta slices (`replaceWhere` / `DELETE … WHERE CountryCode = …`)

## Address enrichment (Google Places)

`src/dq/address_enrichment.py` calls the Google Places Text Search (New) API and returns a ranked candidate table for manual review. It requires env var **`GOOGLE_PLACES_API_KEY`** (or pass `api_key=` to the function).

### Set the API key

**Databricks (recommended for production — secret scope):**

1. Store the key in a Databricks secret scope (create scope + put secret via CLI/UI).
2. Inject into the process environment before calling enrichment:

```python
import os
os.environ["GOOGLE_PLACES_API_KEY"] = dbutils.secrets.get(
    scope="mdm",  # your scope name
    key="google-places-api-key",  # your secret key name
)
```

**Databricks (cluster / job env):** set `GOOGLE_PLACES_API_KEY` on the cluster environment, job `spark_env_vars`, or equivalent. Prefer secrets over plaintext env vars in shared workspaces.

**Local / dev:**

```bash
export GOOGLE_PLACES_API_KEY='your-key-here'
```

Or put the same variable in a `.env` file loaded by your shell/tooling. Do **not** commit secrets — `.env` / `.env.*` are already in `.gitignore`.

### Example

```python
from dq.address_enrichment import enrich_restaurant_candidates

df = enrich_restaurant_candidates(
    "McDonalds",
    "Chineham, United Kingdom",
    country_code="GB",
    max_results=10,
)
display(df)  # Databricks notebook; locally print(df) is fine
```

## Not built yet

Merge/survivorship, incremental match, stewardship UI, automated tests. (Golden id remap history exists in `MDMGoldenIdHistory`; match-evidence history does not.)

## License

Proprietary — internal use.
