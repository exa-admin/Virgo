# Customer MDM Match & Merge Engine

Databricks / Spark-native **Customer Master Data Management** match engine — the replacement
for Informatica MDM, country by country.

It assigns a stable row id to every operator, runs exact and fuzzy rules in a priority
waterfall, builds connected components without GraphFrames, and writes golden ids plus
stewardship tables to Delta Lake.

```
Source → (optional enrichment) → Row registry → Standardize → Exclusions
  → Exact/Fuzzy waterfall → + Informatica group links (hard) → Match links
  → Connected components (→ drop Informatica-group bridges)
  → Golden IDs (Informatica id > prior engine id > mint ≥ 1e8) → MDMMatchedResults
```

Details and a flowchart: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Deployment: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Quick start

```bash
./scripts/build_wheel.sh          # -> dist/mdm_engine-0.1.0-py3-none-any.whl
```

1. Run [`sql/setup_tables.sql`](sql/setup_tables.sql) once per environment.
2. Install the wheel on the cluster (Compute → Libraries → Install new → Python whl).
3. Open [`notebooks/run_match.py`](notebooks/run_match.py), set the `countries` widget
   (`MY`, `MY,SG`, or `ALL`), run all cells.

From any notebook cell:

```python
from matching import run_country, run_all

run_country(spark, "MY")   # one country
run_all(spark)             # every country that has a config
```

As a Databricks **Python wheel task** (no notebook):

```
entry point: mdm-match      parameters: ["--country", "MY"]   (or ["--all"])
```

## Repository layout

| Path | Role |
|------|------|
| `src/matching/` | The match engine — this is what the wheel ships |
| `src/matching/conf/base.json` | Cross-country defaults (sources, standardization, thresholds, target schema) |
| `src/matching/conf/countries/{CC}.json` | Per-country match rules + any base overrides |
| `src/dq/` | Optional Google Places address enrichment (needs the `dq` extra) |
| `sql/setup_tables.sql` | Delta DDL for the MDM tables |
| `scripts/build_wheel.sh` | Build the deployable wheel |
| `notebooks/run_match.py` | Run one country or all countries |
| `notebooks/enrich_addresses.py` | Optional DQ pre-step |
| `AGENTS.md` | Conventions for coding sessions |

### `src/matching` modules

| Module | Responsibility |
|--------|----------------|
| `config.py` | Load `base.json` + `countries/{CC}.json`, resolve table names |
| `io.py` | Read the source population (delta/csv/parquet); write country Delta slices |
| `expressions.py` | Spark column expressions: text cleaning, similarity, rule conditions |
| `rules.py` | One rule → match links (exact star edges, fuzzy blocking + scoring) |
| `graph.py` | Connected components; Informatica groups as hard links and bridge blocking |
| `golden_ids.py` | Golden id selection, minting, registry write-back, history |
| `pipeline.py` | `run_country` / `run_all` and the priority waterfall |
| `cli.py` | `mdm-match` entry point for Databricks wheel tasks |

## Golden ID continuity (the Informatica migration)

Policy: **existing Informatica groupings keep their golden id; only genuinely new clusters
get a new one.**

- **Informatica groups are hard links.** Records already grouped under one `GoldenRecordId`
  are star-linked (`match_rule = Source_GoldenRecordId`) before connected components, so a
  group can never be split and always keeps its id — including records excluded from new
  matching (`preserve_source_golden_groups: true`).
- A **new record** with no Informatica id that matches a group member **inherits the
  group's id**.
- A **brand-new cluster** is minted a BIGINT from `MDMGoldenIdSequence`: always
  `>= golden_id_floor` and above every id already known. The floor is **`100000000`**
  because Informatica is at ~1e7 and still grows for non-migrated countries. Keep it
  identical in every country config and only ever raise it — the run fails if Informatica
  reaches it.
- **Two Informatica groups bridged by an engine rule**: with
  `allow_source_golden_group_merge: false` (the MY default) the bridging edges are dropped
  and recorded in `MDMRuleResults` stage `999_Blocked_Source_GoldenRecordId_Merge`, so no
  Informatica id ever changes. With `true`, one id survives and the other is logged as
  `MERGE` in `MDMGoldenIdHistory`.
- Otherwise a cluster keeps the id the engine gave it on a previous run.
- A known Informatica id is never overwritten with NULL once Informatica stops feeding a
  migrated country; a non-numeric `GoldenRecordId` fails the run rather than silently
  becoming NULL.
- The run **fails before any write-back** if an Informatica id would end up split across
  components, on an engine-minted id, or — when merges are disallowed — changed at all.
- Every old → new remap goes to `MDMGoldenIdHistory`; per-record detail is in
  `MDMMatchedResults` (`previous_golden_id`, `golden_id_changed`,
  `golden_id_differs_from_source`, `golden_id_source`).

Full tie-breaking and safety checks:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#golden-ids-informatica-continuity).

## Configuration

Layered, and both layers ship inside the wheel:

- **`conf/base.json`** — cross-country defaults: `source` / `golden_source` specs,
  `EnrichDate`, `standardization`, `invalid_values`, `exclude_from_match_filters`,
  `golden_id_floor`, `target_schema`.
- **`conf/countries/{CC}.json`** — that country's match rules (`exact_match_rules`,
  `default_fuzzy_blocking`, `fuzzy_match_rules`) plus any base override. Merged on top of
  base, country wins. `filter_condition` defaults to `CountryCode = '<CC>'`.

To add a country, copy `conf/countries/template.json` to `{CC}.json` and edit the rules
(`template.json` and `_*.json` are never loaded as countries). To change configs without
rebuilding the wheel, put the folder somewhere readable and set `MDM_CONF_DIR` (or the
notebook's `conf_dir` widget / `mdm-match --conf-dir`).

Source format is declarative, so moving from Delta views to files needs no code change:

```json
"source":        { "format": "parquet", "path": "/Volumes/cat/sch/vol/operator/" },
"golden_source": { "format": "csv", "path": "/Volumes/cat/sch/vol/golden/",
                   "options": { "header": "true" } }
```

## Prerequisites

- Databricks workspace with Delta Lake / Unity Catalog
- Source views `sl_bdl_processed_cd_prod.cd.vw_ufsoperator` (operators to match) and
  `…vw_ufsoperatorgoden` (golden masters)
- Optional enrichment table `…mdmenrichedoperators` when `EnrichDate` is true
- PySpark and Delta come from the Databricks Runtime — the engine adds no dependencies

## Conventions

- No Python UDFs; Spark SQL / Column expressions only
- No GraphFrames / GraphX — connected components use min-label propagation with Delta
  checkpoints
- Country-partitioned Delta slices (`replaceWhere` / `DELETE … WHERE CountryCode = …`), so
  one country's run never touches another's data

## Address enrichment (Google Places)

`src/dq/` calls the Google Places Text Search API and returns a ranked candidate table for
manual review. Install the wheel with its extra (`pip install "mdm-engine[dq]"`) and supply
the key from a secret scope — **never hardcode it**:

```python
import os
os.environ["GOOGLE_PLACES_API_KEY"] = dbutils.secrets.get(scope="mdm", key="google-places-api-key")
```

Reviewed candidates have to land in `mdmenrichedoperators` for `EnrichDate: true` to pick
them up; nothing flows into the match automatically.

## Not built yet

Merge / survivorship, incremental (CDC) match, stewardship UI, automated tests.

## License

Proprietary — internal use.
