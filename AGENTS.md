# AGENTS.md — Customer MDM Match & Merge Engine

Instructions for AI coding agents and humans starting a new session in this repo.

## What this project is

Customer **Master Data Management (MDM) Match & Merge** on **Databricks / Spark**, analogous to Informatica MDM, Customer 360, and Adobe identity management. Today this repo implements the **Match** half: resolve duplicate operators into golden groups via exact + fuzzy rules, connected components, and stable golden IDs written to Delta.

## Base code (do not reverse)

- **Canonical engine:** Python **PySpark** under `src/mdm/`.
- **Not base:** A Scala “improved” notebook existed as a simpler prototype. Do **not** rewrite the engine in Scala.
- **Port later (Python):** match confidence score, dual-pass enriched vs original reporting, more aggressive cache-before-count patterns from the Scala notes.

## High-level pipeline

1. **Load** source (`sources_informatica.ufsoperator` by default) filtered by country.
2. **Enrich** (optional) from `mdmenrichedoperators` when `EnrichDate` is true.
3. **Row registry** MERGE into `MDMRowRegistry` → assign `MDMRowId` / `record_id`.
4. **Standardize** match attributes (`c_name`, `c_zip`, `c_address`, soundex/prefix, …).
5. **Exclude** via config filters + `MDMMatchExclusions`.
6. **Exact / fuzzy waterfall** (`priorityMatching`) → per-rule links in `MDMRuleResults` / evaluations in `MDMRuleEvaluations`.
7. **Match links** country slice → `MDMMatchLinks`.
8. **Connected components** (native min-label iteration) → `MDMComponentLabels`.
9. **Golden IDs** (prefer earliest `SourceGoldenRecordId`, else `SYNTHETIC_GOLDEN_ID_OFFSET + TempClusterId`).
10. **Matched results** → `MDMMatchedResults` (`replaceWhere` country).

Public API:

```python
from mdm.pipeline import run_country, run_all
from mdm.config import load_country_config, CONFIG_JSON
```

## Key tables

| Table | Role |
|-------|------|
| `MDMRowRegistry` | Stable `MDMRowId` IDENTITY keys per `CountryCode` + `OperatorConcatId` |
| `MDMRuleResults` | Accepted match edges per rule stage |
| `MDMRuleEvaluations` | Fuzzy candidate evidence (similarities as % ints) |
| `MDMMatchExclusions` | Stewardship “do not match” keys |
| `MDMMatchingState` | Intermediate ID sets for waterfall / grouping |
| `MDMMatchLinks` | Final country match graph |
| `MDMComponentLabels` | Component labels per iteration |
| `MDMMatchedResults` | Output with `golden_id`, `final_match_rule`, flags |
| `mdmenrichedoperators` | External enrichment (not created by setup SQL) |

Defaults live in `mdm.config.DEFAULT_TARGET_SCHEMA` = `pds_auroradsar_prod.schema_informatica`.

## How to run (Databricks)

1. Run `sql/setup_tables.sql` (adjust catalog/schema per env).
2. Ensure `src/` is on `sys.path` (see `notebooks/01_run_match_country.py`).
3. Call:

```python
from mdm.config import load_country_config
from mdm.pipeline import run_country

run_country(spark, "MY", load_country_config("MY"))
# or
from mdm.pipeline import run_all
run_all(spark)
```

## Conventions

- Prefer **no Python UDFs** — use Column / SQL expressions.
- Prefer **no GraphFrames / GraphX**.
- Writes are **country-partitioned Delta slices** (`replaceWhere` / delete-by-country).
- Match methods are Spark-native (exact keys, Levenshtein, token Jaccard, blocking).
- Keep config in `conf/countries/*.json`; `CONFIG_JSON` remains as embedded fallback.

## Package map

| Module | Responsibility |
|--------|----------------|
| `config.py` | Constants, JSON loaders, `_runtime_cfg` |
| `utils.py` | Timing, cleaning, similarity, empty schemas, rule conditions |
| `delta_io.py` | Table requires, overwrite slice, materialize helpers |
| `standardize.py` | `standardize_input` |
| `exact_match.py` | Star edges, block caps, priority |
| `fuzzy_match.py` | Blocking + fuzzy scores + evaluations |
| `match_pipeline.py` | Waterfall orchestration |
| `components.py` | `connected_components_native` |
| `golden_ids.py` | Final golden IDs + match rule aggregate |
| `pipeline.py` | Enrich, registry, exclusions, `run_country` / `run_all` |

## What is NOT built yet

- Merge / survivorship of golden attributes
- Incremental / CDC match
- Stewardship UI
- XREF / match history beyond current Delta tables
- Automated unit/integration tests
- Local runnable Spark without Databricks (package is Databricks-oriented)

## Editing guidance for agents

- Preserve match semantics (priorities, exclusions, block size caps, synthetic offset `100000000`).
- Prefer small, focused changes; do not “simplify away” stewardship tables or waterfall.
- When adding countries, add `conf/countries/<CC>.json` rather than hardcoding rules in Python.
- Identity column DDL for `MDMRowId` may need env-specific adjustment — see comments in `sql/setup_tables.sql`.

## Pointers

- Human overview: `README.md`
- Pipeline diagram: `docs/ARCHITECTURE.md`
- DDL: `sql/setup_tables.sql`
- Entry notebook: `notebooks/01_run_match_country.py`
