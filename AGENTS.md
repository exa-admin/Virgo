# AGENTS.md — Customer MDM Match & Merge Engine

Instructions for AI coding agents and humans starting a new session in this repo.

## What this project is

Customer **Master Data Management (MDM) Match & Merge** on **Databricks / Spark**, analogous to Informatica MDM, Customer 360, and Adobe identity management. Today this repo implements the **Match** half: resolve duplicate operators into golden groups via exact + fuzzy rules, connected components, and stable golden IDs written to Delta.

## Starting on a new machine

Nothing in this repo runs meaningfully outside Databricks — the local setup exists only so
an editor can resolve imports and so you can lint. There is no local database, no local
Spark workflow to learn, and no build step beyond the wheel.

```bash
git clone <repo> && cd Virgo
python3.11 -m venv .venv                 # 3.10-3.12; match your DBR's Python
.venv/bin/pip install -r requirements.txt
```

Point your IDE's interpreter at `.venv/bin/python`. That is the whole setup.

- `requirements.txt` is **editing only** — never installed on a cluster. It pins `pyspark`
  (for completions; match your DBR's Spark version) and `databricks-sdk` (so the IDE can
  resolve `spark` / `dbutils` / `display`, which notebooks import under `TYPE_CHECKING`).
- The wheel itself has **zero** dependencies: `./scripts/build_wheel.sh`.
- To verify a change without a cluster: `python -m pyflakes src/matching/*.py` and
  `python -c "import matching"`. Real verification means running on Databricks.
- `.venv/` should not live in iCloud Drive if the repo does — Spark's jars and sync do not
  get along. Prefer a local checkout.

## Base code (do not reverse)

- **Canonical engine:** Python **PySpark** under `src/matching/` (match phase).
- **Future:** `src/merging/` for survivorship / merge (not present yet).
- **Not base:** A Scala “improved” notebook existed as a simpler prototype. Do **not** rewrite the engine in Scala.
- **Port later (Python):** match confidence score, dual-pass enriched vs original reporting, more aggressive cache-before-count patterns from the Scala notes.

## High-level pipeline

1. **Load** source via `matching.read.read_source_population` (kept separate so the
   input can be Delta/CSV/Parquet): the operator view (`sl_bdl_processed_cd_prod.cd.vw_ufsoperator`,
   the population to match) plus the golden view (`…vw_ufsoperatorgolden`, already-matched
   masters) — golden masters absent from the operator feed are backfilled with a synthetic
   `OperatorConcatId` (`GRID_<GoldenRecordId>`) so their group is never lost. Filtered by country.
2. **Enrich** (optional) from `mdmenrichedoperators` when `EnrichDate` is true.
3. **Row registry** MERGE into `mdm_row_registry` → assign `MDMRowId` / `record_id`.
4. **Standardize** match attributes (`c_name`, `c_zip`, `c_address`, soundex/prefix, …).
5. **Exclude** via config filters + `mdm_match_exclusions`.
6. **Exact / fuzzy waterfall** (`priorityMatching`) → per-rule links in `mdm_rule_results` / evaluations in `mdm_rule_evaluations`.
7. **Source golden group links** (`preserve_source_golden_groups`): star edges over every Informatica `SourceGoldenRecordId` shared by ≥ 2 records (all records, excluded ones included) → `mdm_rule_results` stage `000_Source_GoldenRecordId`; engine edges that directly bridge two Informatica groups are dropped when `allow_source_golden_group_merge` is false.
8. **Match links** country slice → `mdm_match_links`.
9. **Connected components** (native min-label iteration) → `mdm_component_labels`; then, if merges are disallowed, components still holding several Informatica ids are re-labelled along group lines and the bridging edges are dropped (`mdm_rule_results` stage `999_Blocked_Source_GoldenRecordId_Merge`, `mdm_match_links` rewritten).
10. **Golden IDs** with Informatica continuity: per component prefer an Informatica `SourceGoldenRecordId`, else the engine id assigned on a previous run (`mdm_row_registry.MDMGoldenId`), else mint from `mdm_golden_id_sequence`. `validate_group_assignments` fails fast if an Informatica group was split/renamed. Assignments are written back to `mdm_row_registry`; remaps go to `mdm_golden_id_history`.
11. **Matched results** → `mdm_matched_results` (`replaceWhere` country), after appending
 record-level golden id changes to `operator_golden_changelog` (that step reads the
 PREVIOUS slice, so it must stay before the overwrite).

A second, engine-only components pass (`stage_prefix="engine_labels"`) runs on the
waterfall links **before** the Informatica group links are added, and lands in
`mdm_matched_results.engine_match_id`. It exists solely so the over/undermatch views have
something to compare Informatica against — `golden_id` cannot disagree with Informatica
once `preserve_source_golden_groups` hard-links those groups. Do not "optimise" it away.

## Golden ID continuity policy (read before touching `golden_ids.py` / `graph.py` / `pipeline._attach_row_registry`)

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
 `mdm_rule_results` stage `999_Blocked_Source_GoldenRecordId_Merge` for stewardship — no
 Informatica id ever changes. With `true`, the component keeps one id (earliest
 `GoldenIDCreatedDate`, then smallest) and the other is logged as `MERGE`.
4. For a migrated country the source `GoldenRecordId` may go NULL: the registry MERGE never
 downgrades a non-null `SourceGoldenRecordId` to NULL. The source column is a numeric
 STRING; a non-blank non-integer value fails the run instead of silently casting to NULL.
5. `validate_group_assignments` fails the run (before any write-back) if an
 Informatica id would end up on more than one component, on an engine-minted id, or (when
 merges are disallowed) on a different id.

Selection order per component: Informatica id (earliest `GoldenIDCreatedDate`, then smallest) →
prior `ENGINE` id (earliest `MDMGoldenIdAssignedDate`, then smallest) → mint. An id shared by
several components is claimed by the one with most incumbent records (then most records,
earliest date, smallest `TempClusterId`); the others get their own prior id or a new one
(with preserved groups this only ever concerns engine ids).
Merges/splits are logged to `mdm_golden_id_history` (`Reason` = `MERGE` | `SPLIT`).
Full description: `docs/ARCHITECTURE.md` → "Golden IDs (Informatica continuity)".

Public API:

```python
from matching import run_country, run_all, load_country_config, available_countries
```

## Key tables

| Table | Role |
|-------|------|
| `mdm_row_registry` | Stable `MDMRowId` IDENTITY keys per `CountryCode` + `OperatorConcatId`; golden id crosswalk (`SourceGoldenRecordId`, `MDMGoldenId`, `MDMGoldenIdSource`, `MDMGoldenIdAssignedDate`) |
| `mdm_golden_id_sequence` | High-water mark for engine-minted golden ids (single global row) |
| `mdm_golden_id_history` | Append-only old → new golden id remaps per run (`MERGE` / `SPLIT`) |
| `mdm_rule_results` | Accepted match edges per rule stage (incl. `000_Source_GoldenRecordId`); dropped Informatica-group bridges in stage `999_Blocked_Source_GoldenRecordId_Merge` (`blocked_source_group_merge`) |
| `mdm_rule_evaluations` | Fuzzy candidate evidence (similarities as % ints) |
| `mdm_match_exclusions` | Stewardship “do not match” keys |
| `mdm_matching_state` | Intermediate ID sets for waterfall / grouping |
| `mdm_match_links` | Final country match graph |
| `mdm_component_labels` | Component labels per iteration |
| `operator_golden_changelog` | Append-only record-level trace of golden id changes per `OperatorConcatId` with previous/current matching data and `ChangeReason` |
| `vw_informatica_undermatch` / `vw_informatica_overmatch` | Views comparing `engine_match_id` (our rules alone) against Informatica's `SourceGoldenRecordId` |
| `mdm_matched_results` | Output with `golden_id`, `golden_id_source`, `previous_golden_id`, `golden_id_changed`, `golden_id_differs_from_source`, `final_match_rule`, flags |
| `mdmenrichedoperators` | External enrichment (not created by setup SQL) |

### Where the data lives — `conf/storage.config`

**Every table, view and file path the engine touches is declared in
`src/matching/conf/storage.config`, and nowhere else.** No storage name is hardcoded in
Python. The file has two groups:

- `sources` — read-only inputs (`operator`, `golden`, `enriched_operators`, and the two
  `informatica_*` comparison views). Any supported format.
- `tables` — the Delta tables the engine reads *and* writes. These stay `delta` + `table`,
  because the writes are `replaceWhere` / `DELETE WHERE CountryCode` by table name.

Each entry is a spec — `{format: delta|csv|parquet, table|path, options}` — and `${schema}`
expands to the file's `schema` value, so one edit repoints every engine table.

**All reads go through `io.read(spark, cfg, "<dataset>")`.** Nothing else calls
`spark.table` / `spark.read`. To move a source from a table to Parquet, edit
`storage.config` — the engine does not change. New formats are taught to `io.read_spec`
once and every dataset gains them.

Overrides, highest first: `target_schema` / `source` / `golden_source` in `conf/base.json`
or a country file → `MDM_STORAGE_CONFIG` (that one file) → `MDM_CONF_DIR`'s copy → the
packaged copy. The first is how `tests/conftest.py` retargets everything at a scratch
schema; the packaged fallback is why a conf dir holding only `base.json` still works.

## How to run (Databricks)

1. Build the wheel: `./scripts/build_wheel.sh` → `dist/mdm_engine-<ver>-py3-none-any.whl`.
2. Install it as a cluster/job library (or `%pip install <path to whl>` in a notebook).
3. Run `sql/setup_tables.sql` once per environment.

```python
from matching import run_country, run_all

run_country(spark, "MY")     # one country
run_all(spark)               # every country with a config
run_all(spark, ["MY", "SG"]) # a subset
```

Notebook: `notebooks/run_match.py` (widget-driven). Job: Python wheel task, entry point
`mdm-match`, parameters `["--country","MY"]` or `["--all"]`.

Configs ship **inside** the wheel at `src/matching/conf/`. `MDM_CONF_DIR` (or
`--conf-dir`) points the loader at an external folder instead.

## Conventions

- Prefer **no Python UDFs** — use Column / SQL expressions.
- Prefer **no GraphFrames / GraphX**.
- Writes are **country-partitioned Delta slices** (`replaceWhere` / delete-by-country).
- Match methods are Spark-native (exact keys, Levenshtein, token Jaccard, blocking).
- Storage names live **only** in `src/matching/conf/storage.config` (see above); never hardcode a table, view or path in Python, and read through `io.read`.
- Config is layered: `src/matching/conf/base.json` (cross-country matching defaults — `EnrichDate`, `standardization`, `invalid_values`, `exclude_from_match_filters`, `golden_id_floor`, …) merged with `src/matching/conf/countries/{CC}.json` (per-country settings: the **match rules** — `exact_match_rules`, `default_fuzzy_blocking`, `fuzzy_match_rules` — plus any base overrides; country wins). `filter_condition` defaults to `CountryCode = '<CC>'`. An empty `{}` country file inherits all base defaults (see `MY.json`). Copy `conf/countries/template.json` when adding a country — that file is reference-only and is not loaded. `MDM_CONF_DIR` env var relocates `conf/` without rebuilding the wheel.

## Package map

Intended layout:

- `src/matching/` — match pipeline (current)
- `src/merging/` — survivorship / merge (future; not created yet)
- `src/dq/` — data quality utilities

### `src/matching/` — match engine

| Module | Step |
|--------|------|
| `config.py` | Config + `storage.config` loading; `resolve_config` |
| `read.py` | 1. Load the operator + golden population, overlay enrichment |
| `registry.py` | 2. MERGE into `mdm_row_registry`, attach `MDMRowId` |
| `standardize.py` | 3. Derive the `c_*` match attributes; work out exclusions |
| `rules.py` | 4. One rule → links: `run_exact_rule`, `run_fuzzy_rule` |
| `graph.py` | 5. Connected components; Informatica group links and bridge blocking |
| `golden_ids.py` | 6. Golden id assignment with Informatica continuity |
| `write.py` | Every Delta write, always a country slice |
| `helpers.py` | Shared column expressions: cleaning, similarity, filters, timing |
| `match_pipeline.py` | Orchestration: `run_country` / `run_all`, the priority waterfall |
| `cli.py` | `mdm-match` console entry point |

Rules of thumb when adding code: reads go in `read.py` and writes in `write.py`, anything
that returns a `Column` goes in `helpers.py`, label propagation goes in `graph.py`, and
`match_pipeline.py` only orchestrates — it should read as the list of steps.

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

## Current state — 2026-09-10

The engine was restructured and extended; treat the following as recent and worth reading
before changing anything nearby.

- `src/matching/` was consolidated from 13 modules to 9 (`config`, `io`, `expressions`,
  `rules`, `graph`, `golden_ids`, `pipeline`, `cli`, `__init__`) with dead code removed.
  Behaviour was preserved deliberately; if something looks odd, check git history before
  "fixing" it.
- `conf/` moved **inside the package** (`src/matching/conf/`) so the wheel is runnable as
  installed. `MDM_CONF_DIR` still overrides it at runtime.
- Deployment is the wheel only. The old zip-bundle and `sys.path` bootstrapping are gone,
  along with `notebooks/00_path_setup.py`.
- New: `operator_golden_changelog` (record-level golden id trace with cause),
  `engine_match_id` on `mdm_matched_results`, and the two Informatica comparison views.
- New: `tests/` + `notebooks/run_tests.py`, run on a cluster (`docs/TESTING.md`).

### Open items

1. **Rotate the Google Places API key.** A live key was hardcoded in
   `src/dq/address_enrichment.py` and is still in git history (commit `bc394a8`). The code
   now reads `GOOGLE_PLACES_API_KEY` properly, but the leaked key must be rotated in the
   Google Cloud console — editing the file does not undo the exposure.
2. **`MY.json` SAP rule silently skips NULL `OTMText`.** Its
   `match_exclusion_filter` is `["OTMText IN ('A++','DUMMY')"]`, which becomes
   `NOT (OTMText IN (...))` — NULL, not TRUE, when `OTMText` is NULL, so those records
   never reach the rule. Decide whether to wrap it in `coalesce(OTMText, '')`. The current
   behaviour is pinned by
   `test_null_in_an_exclusion_column_silently_drops_the_record_from_that_rule`, which will
   fail (correctly) once fixed. Check other exclusion filters on nullable columns too.
3. **The test suite has never been observed passing.** It was written and statically
   checked but not yet run green on a cluster. The first cluster run is the real shakedown;
   a failure is more likely a wrong expectation about the MY rules (blocking keys, soundex)
   than an engine defect.
4. **No run has been executed against real data since the restructure.** Run MY end to end
   and diff `mdm_matched_results` against a pre-change run before trusting it.

## What is NOT built yet

- Merge / survivorship of golden attributes
- Incremental / CDC match
- Stewardship UI
- Match history beyond current Delta tables (golden id remaps are in `mdm_golden_id_history`)
- Any supported local workflow — the engine, the tests and the deployment are all
  Databricks-only by design

## Editing guidance for agents

- Preserve match semantics (priorities, exclusions, block size caps) and the golden id continuity policy above (never re-mint an id the registry already knows; never split or rename an Informatica group; keep `golden_id_floor` identical across countries and only raise it).
- Source golden group edges live **outside** the priority waterfall on purpose: they must not change which rule other records match on. Do not fold them into `run_match_waterfall`.
- Prefer small, focused changes; do not “simplify away” stewardship tables or waterfall.
- Run the suite on a cluster (`notebooks/run_tests.py`) after any change to matching semantics. `tests/` asserts the golden id
  continuity policy above; a failure there is a migration risk, not a flaky test.
- When adding countries, copy `conf/countries/template.json` → `conf/countries/<CC>.json` and edit; do not hardcode rules in Python. `template.json` / `_*.json` are never loaded as countries.
- Identity column DDL for `MDMRowId` may need env-specific adjustment — see comments in `sql/setup_tables.sql`.

## Pointers

- Tests: `docs/TESTING.md` — they run on a Databricks cluster (`notebooks/run_tests.py`), not locally
- Human overview: `README.md`
- Table inventory (internal vs audit): `docs/TABLES.md`
- Pipeline diagram: `docs/ARCHITECTURE.md`
- DDL: `sql/setup_tables.sql`
- Entry notebook: `notebooks/run_match.py` (imports the installed wheel — the deployed path)
- Debug/learning notebook: `notebooks/run_match_syspath.py` (same run, but imports the engine
  from `src/` in a Workspace checkout via `sys.path`; edit a file and re-run, no rebuild.
  Development only — jobs use the wheel)
- Deployment: `docs/DEPLOYMENT.md`
