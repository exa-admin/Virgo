# Customer MDM Match & Merge Engine

Databricks / Spark-native **Customer Master Data Management** Match engine (similar in role to Informatica MDM, Customer 360, and Adobe identity resolution).

This repository holds the **Python PySpark** match pipeline that assigns stable row IDs, runs exact and fuzzy rules in a priority waterfall, builds connected components without GraphFrames, and writes golden IDs plus stewardship tables to Delta Lake.

> **Base code decision:** Use this Python engine — not the Scala “improved” notebook prototype. Useful Scala ideas (match confidence score, dual-pass enriched vs original reporting, cache-before-count) may be ported into Python later; see [AGENTS.md](AGENTS.md).

## Architecture (summary)

```
Source → (optional enrichment) → Row registry → Standardize → Exclusions
  → Exact/Fuzzy waterfall → Match links → Connected components
  → Golden IDs → MDMMatchedResults
```

Details and a mermaid flowchart: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Repository layout

| Path | Role |
|------|------|
| `src/matching/` | Match pipeline package (current) |
| `src/merging/` | Survivorship / merge (future; not present yet) |
| `src/dq/` | Data quality utilities (e.g. address enrichment) |
| `conf/countries/*.json` | Per-country match config (MY extracted) |
| `sql/setup_tables.sql` | Delta DDL for MDM tables |
| `notebooks/01_run_match_country.py` | Thin Databricks entry notebook |
| `AGENTS.md` | Instructions for AI/coding sessions |
| `docs/ARCHITECTURE.md` | Pipeline deep-dive |

## Prerequisites

- Databricks workspace with Delta Lake / Unity Catalog (or compatible metastore)
- Source table `sources_informatica.ufsoperator` (or override in config / `_runtime_cfg` defaults)
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

Country JSON (e.g. `conf/countries/MY.json`) controls:

- `filter_condition` — source filter (`CountryCode = 'MY'`)
- `EnrichDate` — join Google Places / enriched operators when true
- `priorityMatching` — waterfall (already-matched records leave later rules as subjects only)
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

Merge/survivorship, incremental match, stewardship UI, XREF history, automated tests.

## License

Proprietary — internal use.
