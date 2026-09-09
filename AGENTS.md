# AGENTS.md — Customer MDM Match & Merge Engine

Instructions for AI coding agents and humans starting a new session in this repo.

## What this project is

Customer **Master Data Management (MDM) Match & Merge** on **Databricks / Spark**, analogous to Informatica MDM, Customer 360, and Adobe identity management. Today this repo implements the **Match** half: resolve duplicate operators into golden groups via exact + fuzzy rules, connected components, and stable golden IDs written to Delta.

## Base code (do not reverse)

- **Canonical engine:** Python **PySpark** under `src/matching/` (match phase).
- **Future:** `src/merging/` for survivorship / merge (not present yet).
- **Not base:** A Scala “improved” notebook existed as a simpler prototype. Do **not** rewrite the engine in Scala.
- **Port later (Python):** match confidence score, dual-pass enriched vs original reporting, more aggressive cache-before-count patterns from the Scala notes.

## High-level pipeline

1. **Load** source via `matching.sources.build_source_population` (kept separate so the
   input can be Delta/CSV/Parquet): the operator view (`sl_bdl_processed_cd_prod.cd.vw_ufsoperator`,
   the population to match) plus the golden view (`…vw_ufsoperatorgoden`, already-matched
   masters) — golden masters absent from the operator feed are backfilled with a synthetic
   `OperatorConcatId` (`GRID_<GoldenRecordId>`) so their group is never lost. Filtered by country.
2. **Enrich** (optional) from `mdmenrichedoperators` when `EnrichDate` is true.
3. **Row registry** MERGE into `MDMRowRegistry` → assign `MDMRowId` / `record_id`.
4. **Standardize** match attributes (`c_name`, `c_zip`, `c_address`, soundex/prefix, …).
5. **Exclude** via config filters + `MDMMatchExclusions`.
6. **Exact / fuzzy waterfall** (`priorityMatching`) → per-rule links in `MDMRuleResults` / evaluations in `MDMRuleEvaluations`.
7. **Source golden group links** (`preserve_source_golden_groups`): star edges over every Informatica `SourceGoldenRecordId` shared by ≥ 2 records (all records, excluded ones included) → `MDMRuleResults` stage `000_Source_GoldenRecordId`; engine edges that directly bridge two Informatica groups are dropped when `allow_source_golden_group_merge` is false.
8. **Match links** country slice → `MDMMatchLinks`.
9. **Connected components** (native min-label iteration) → `MDMComponentLabels`; then, if merges are disallowed, components still holding several Informatica ids are re-labelled along group lines and the bridging edges are dropped (`MDMRuleResults` stage `999_Blocked_Source_GoldenRecordId_Merge`, `MDMMatchLinks` rewritten).
10. **Golden IDs** with Informatica continuity: per component prefer an Informatica `SourceGoldenRecordId`, else the engine id assigned on a previous run (`MDMRowRegistry.MDMGoldenId`), else mint from `MDMGoldenIdSequence`. `validate_source_golden_group_assignments` fails fast if an Informatica group was split/renamed. Assignments are written back to `MDMRowRegistry`; remaps go to `MDMGoldenIdHistory`.
11. **Matched results** → `MDMMatchedResults` (`replaceWhere` country).

## Golden ID continuity policy (read before touching `golden_ids.py` / `source_golden_groups.py` / `_ensure_row_registry`)

The company replaces Informatica MDM **country by country**. Hard requirements:

1. **Informatica groupings are preserved as hard links.** Records that Informatica grouped
 under one `GoldenRecordId` are star-linked (`match_rule = Source_GoldenRecordId`, stage
 `000_Source_GoldenRecordId`, `edge_type = source_golden`) before connected components, so
 the engine can never split such a group and the group always keeps its Informatica id
 (`preserve_source_golden_groups`, default true). This also applies to records excluded from
 new matching: exclusions stop *new* matches, they never undo Informatica's grouping.
2. New records (no Informatica id) that match a group member **inherit the group's id**;
 records forming a **brand-new cluster** get an engine id: same shape (BIGINT), globally
 unique, **disjoint from Informatica's range** (`golden_id_floor` = **100 000 000** (1e8),
 plus `> max` known id) and **stable across runs** (previous assignment is preferred over
 minting). Informatica's max id was 9 999 993 in Sep 2026 and still grows slowly for
 non-migrated countries; `validate_golden_id_space` fails the run if it ever reaches the floor.
 The floor must be **identical in every country config** and may **only ever be raised**.
3. **Two Informatica groups bridged by engine rules**: with `allow_source_golden_group_merge =
 false` (MY default) the bridging edges are dropped (direct bridges before components,
 transitive bridges via a seeded re-labelling after components) and written to
 `MDMRuleResults` stage `999_Blocked_Source_GoldenRecordId_Merge` for stewardship — no
 Informatica id ever changes. With `true`, the component keeps one id (earliest
 `GoldenIDCreatedDate`, then smallest) and the other is logged as `MERGE`.
4. For a migrated country the source `GoldenRecordId` may go NULL: the registry MERGE never
 downgrades a non-null `SourceGoldenRecordId` to NULL. The source column is a numeric
 STRING; a non-blank non-integer value fails the run instead of silently casting to NULL.
5. `validate_source_golden_group_assignments` fails the run (before any write-back) if an
 Informatica id would end up on more than one component, on an engine-minted id, or (when
 merges are disallowed) on a different id.

Selection order per component: Informatica id (earliest `GoldenIDCreatedDate`, then smallest) →
prior `ENGINE` id (earliest `MDMGoldenIdAssignedDate`, then smallest) → mint. An id shared by
several components is claimed by the one with most incumbent records (then most records,
earliest date, smallest `TempClusterId`); the others get their own prior id or a new one
(with preserved groups this only ever concerns engine ids).
Merges/splits are logged to `MDMGoldenIdHistory` (`Reason` = `MERGE` | `SPLIT`).
Full description: `docs/ARCHITECTURE.md` → "Golden IDs (Informatica continuity)".
`SYNTHETIC_GOLDEN_ID_OFFSET` is deprecated and unused.

Public API:

```python
from matching.pipeline import run_country, run_all
from matching.config import load_country_config
```

## Key tables

| Table | Role |
|-------|------|
| `MDMRowRegistry` | Stable `MDMRowId` IDENTITY keys per `CountryCode` + `OperatorConcatId`; golden id crosswalk (`SourceGoldenRecordId`, `MDMGoldenId`, `MDMGoldenIdSource`, `MDMGoldenIdAssignedDate`) |
| `MDMGoldenIdSequence` | High-water mark for engine-minted golden ids (single global row) |
| `MDMGoldenIdHistory` | Append-only old → new golden id remaps per run (`MERGE` / `SPLIT`) |
| `MDMRuleResults` | Accepted match edges per rule stage (incl. `000_Source_GoldenRecordId`); dropped Informatica-group bridges in stage `999_Blocked_Source_GoldenRecordId_Merge` (`blocked_source_group_merge`) |
| `MDMRuleEvaluations` | Fuzzy candidate evidence (similarities as % ints) |
| `MDMMatchExclusions` | Stewardship “do not match” keys |
| `MDMMatchingState` | Intermediate ID sets for waterfall / grouping |
| `MDMMatchLinks` | Final country match graph |
| `MDMComponentLabels` | Component labels per iteration |
| `MDMMatchedResults` | Output with `golden_id`, `golden_id_source`, `previous_golden_id`, `golden_id_changed`, `golden_id_differs_from_source`, `final_match_rule`, flags |
| `mdmenrichedoperators` | External enrichment (not created by setup SQL) |

Defaults live in `matching.config.DEFAULT_TARGET_SCHEMA` = `pds_auroradsar_prod.schema_informatica`
(override per env via `target_schema` in `conf/base.json`). Source views default to
`sl_bdl_processed_cd_prod.cd.vw_ufsoperator` / `…vw_ufsoperatorgoden` (override via
`source` / `golden_source` specs; each is `{format: delta|csv|parquet, table|path, options}`).

## How to run (Databricks)

1. Run `sql/setup_tables.sql` (adjust catalog/schema per env).
2. Ensure `src/` is on `sys.path` (see `notebooks/01_run_match_country.py`).
3. Call:

```python
from matching.config import load_country_config
from matching.pipeline import run_country

run_country(spark, "MY", load_country_config("MY"))
# or
from matching.pipeline import run_all
run_all(spark)
```

## Conventions

- Prefer **no Python UDFs** — use Column / SQL expressions.
- Prefer **no GraphFrames / GraphX**.
- Writes are **country-partitioned Delta slices** (`replaceWhere` / delete-by-country).
- Match methods are Spark-native (exact keys, Levenshtein, token Jaccard, blocking).
- Config is layered: `conf/base.json` (cross-country defaults — source specs, `EnrichDate`, `standardization`, `invalid_values`, `exclude_from_match_filters`, `golden_id_floor`, `target_schema`, …) merged with `conf/countries/{CC}.json` (per-country settings: the **match rules** — `exact_match_rules`, `default_fuzzy_blocking`, `fuzzy_match_rules` — plus any base overrides; country wins). `filter_condition` defaults to `CountryCode = '<CC>'`. An empty `{}` country file inherits all base defaults (see `MY.json`). Copy `conf/countries/template.json` when adding a country — that file is reference-only and is not loaded. `MDM_CONF_DIR` env var relocates `conf/` (e.g. wheel deploys).

## Package map

Intended layout:

- `src/matching/` — match pipeline (current)
- `src/merging/` — survivorship / merge (future; not created yet)
- `src/dq/` — data quality utilities

### `src/matching/` — match engine

| Module | Responsibility |
|--------|----------------|
| `config.py` | Constants, JSON loaders (`conf/base.json` master + per-country override merge), `_runtime_cfg` |
| `sources.py` | Source ingestion (delta/csv/parquet) for operator + golden views; `build_source_population` |
| `utils.py` | Timing, cleaning, similarity, empty schemas, rule conditions |
| `delta_io.py` | Table requires, overwrite slice, materialize helpers |
| `standardize.py` | `standardize_input` |
| `exact_match.py` | Star edges, block caps, priority |
| `fuzzy_match.py` | Blocking + fuzzy scores + evaluations |
| `match_pipeline.py` | Waterfall orchestration |
| `components.py` | `connected_components_native` (raises on non-convergence) |
| `source_golden_groups.py` | Informatica groupings as hard links; direct/transitive bridge blocking; blocked-edge stewardship output |
| `golden_ids.py` | Golden id continuity: candidate claim/choice, allocator, registry write-back, history, group-preservation validation |
| `pipeline.py` | Enrich, registry, exclusions, `run_country` / `run_all` |

### `src/dq/` — data quality

| Module | Responsibility |
|--------|----------------|
| `address_enrichment.py` | Google Places top-N address candidates for manual review |

### Google Places API key (`GOOGLE_PLACES_API_KEY`)

`src/dq/address_enrichment.py` reads the key from **`os.environ["GOOGLE_PLACES_API_KEY"]`** (or an explicit `api_key=` argument). Exact variable name: **`GOOGLE_PLACES_API_KEY`**.

**Databricks (prefer secrets for production):**

1. Create a secret scope and store the key, e.g. scope `mdm`, key `google-places-api-key`.
2. In a notebook or job init, inject into the process env before calling enrichment:

```python
import os
os.environ["GOOGLE_PLACES_API_KEY"] = dbutils.secrets.get(
    scope="mdm", key="google-places-api-key"
)
```

Alternatively, set `GOOGLE_PLACES_API_KEY` as a cluster / job environment variable (Spark env or job `spark_env_vars`). Prefer secrets over plain env vars in shared workspaces.

**Local / dev:**

```bash
export GOOGLE_PLACES_API_KEY='your-key-here'
# or put it in a .env file (already gitignored via .env / .env.*)
```

Do not commit secrets. `.gitignore` already covers `.env` and `.env.*`.

## What is NOT built yet

- Merge / survivorship of golden attributes
- Incremental / CDC match
- Stewardship UI
- Match history beyond current Delta tables (golden id remaps are in `MDMGoldenIdHistory`)
- Automated unit/integration tests
- Local runnable Spark without Databricks (package is Databricks-oriented)

## Editing guidance for agents

- Preserve match semantics (priorities, exclusions, block size caps) and the golden id continuity policy above (never re-mint an id the registry already knows; never split or rename an Informatica group; keep `golden_id_floor` identical across countries and only raise it).
- Source golden group edges live **outside** the priority waterfall on purpose: they must not change which rule other records match on. Do not fold them into `run_match_pipeline`.
- Prefer small, focused changes; do not “simplify away” stewardship tables or waterfall.
- When adding countries, copy `conf/countries/template.json` → `conf/countries/<CC>.json` and edit; do not hardcode rules in Python. `template.json` / `_*.json` are never loaded as countries.
- Identity column DDL for `MDMRowId` may need env-specific adjustment — see comments in `sql/setup_tables.sql`.

## Pointers

- Human overview: `README.md`
- Pipeline diagram: `docs/ARCHITECTURE.md`
- DDL: `sql/setup_tables.sql`
- Entry notebook: `notebooks/01_run_match_country.py`
