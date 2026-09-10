# Deploying the MDM match engine to Databricks

One artifact: a wheel. PySpark and Delta come from the Databricks Runtime, so the wheel has
no dependencies of its own, and the country configs ship inside it.

```bash
./scripts/build_wheel.sh
# -> dist/mdm_engine-0.1.0-py3-none-any.whl
```

## Where the data lives — `conf/storage.config`

Every table, view and file path the engine touches is declared in one file,
[`src/matching/conf/storage.config`](../src/matching/conf/storage.config). Nothing is
hardcoded in Python, so this is the only file to edit when a deployment moves.

```jsonc
"schema": "pds_auroradsar_prod.schema_informatica",   // ${schema} below expands to this

"sources": {                                          // read-only; any format
  "operator": {"format": "delta", "table": "sl_bdl_processed_cd_prod.cd.vw_ufsoperator"},
  "golden":   {"format": "delta", "table": "sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden"}
},
"tables": {                                           // engine-owned; read AND written
  "row_registry": {"format": "delta", "table": "${schema}.MDMRowRegistry"}
}
```

**Moving a source to files** is this edit and nothing else — no rebuild of engine logic,
no code change:

```json
"operator": {"format": "parquet", "path": "/Volumes/cat/sch/vol/operator/"}
```

Every read in the engine goes through the single `io.read()` function, so the format is
decided here and only here. `csv` and `parquet` take a `path` (plus optional `options`);
`delta` takes a `table`. Datasets under `tables` must stay Delta tables — the engine
writes them as country slices addressed by name.

Overrides, highest first:

| | |
|---|---|
| `target_schema` / `source` / `golden_source` in `conf/base.json` or a country file | Retarget one environment without touching `storage.config` (what the tests use) |
| `MDM_STORAGE_CONFIG=/path/to/storage.config` | Replace just this file |
| a `storage.config` inside `MDM_CONF_DIR` | Replace it along with the rest of `conf/` |
| the copy packaged in the wheel | The fallback |

## One-time setup

1. **Create the tables.** Run [`sql/setup_tables.sql`](../sql/setup_tables.sql) in a SQL
   editor or notebook cell. It already targets `pds_auroradsar_prod.schema_informatica`,
   which is also the `schema` in `conf/storage.config` — run it as-is. Change that name in
   both places only if your workspace genuinely uses a different catalog/schema.

2. **Check the source views are readable:**
   - `sl_bdl_processed_cd_prod.cd.vw_ufsoperator` — operators to match
   - `sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden` — golden masters

   Both are set in `conf/storage.config` under `sources`.

## Install the wheel

Upload `mdm_engine-<version>-py3-none-any.whl` to a UC Volume (Catalog → your volume →
**Upload to this volume**), then either:

- **Cluster library** — Compute → your cluster → Libraries → Install new → Python whl →
  point at the Volume path. Every notebook and job on that cluster can then
  `from matching import run_country`.
- **Job library** — attach it to the job's task instead, so the version is pinned per job.
- **Notebook-scoped** — `%pip install /Volumes/<cat>/<sch>/<vol>/mdm_engine-0.1.0-py3-none-any.whl`
  as the first cell. Good for trying a new build without touching the cluster.

Add the enrichment dependencies only if you use `src/dq`:
`%pip install "mdm-engine[dq]"` (or install `pandas` and `requests` on the cluster).

## Run

### Notebook

Import [`notebooks/run_match.py`](../notebooks/run_match.py) into your Workspace
(Workspace → ⋮ → Import → File). Set the `countries` widget to `MY`, `MY,SG` or `ALL` and
run all cells. It prints a per-country summary and shows the results.

Its `%pip install` cell is **commented out on purpose** — the normal path is a cluster
library, and a live `%pip` cell with a placeholder path would fail Run All. Uncomment it
only if you are installing the wheel per-notebook, and set the Volume path when you do.
The reporting cells take their table names from `storage.config`, so they follow the
environment automatically.

### Running from source instead (development only)

[`notebooks/run_match_syspath.py`](../notebooks/run_match_syspath.py) does the same run,
but imports the engine from a Workspace checkout of the repo via `sys.path` rather than
from the wheel. Edit a file under `src/matching/`, re-run two cells, and the change is
live — no rebuild, re-upload or cluster restart. Import the whole repo first (Git folder,
or a zip via Workspace → ⋮ → Import), and open the notebook from inside that folder.

Use it for debugging and for learning how the import path resolves. **Do not point a job
at it** — a Workspace folder is mutable and unversioned at run time. Scheduled runs get
the wheel.

### Any notebook cell

```python
from matching import run_country, run_all

run_country(spark, "MY")   # one country, returns its MDMMatchedResults slice
run_all(spark)             # every country with a config
run_all(spark, ["MY", "SG"])
```

### Scheduled job (no notebook)

Create a job with a **Python wheel task**:

| Field | Value |
|---|---|
| Package name | `mdm_engine` |
| Entry point | `mdm-match` |
| Parameters | `["--country", "MY"]` — or `["--all"]`, or `["--country","MY","--country","SG"]` |

`mdm-match --list` prints the configured countries.

## Changing configs without rebuilding

The wheel bundles `conf/` (`storage.config`, `base.json`, `countries/`), which is what
makes an install runnable as-is. To point the engine at configs you can edit in place,
upload the folder and set `MDM_CONF_DIR` **before importing**:

```python
import os
os.environ["MDM_CONF_DIR"] = "/Volumes/<cat>/<sch>/<vol>/mdm_conf"
from matching import run_country
run_country(spark, "MY")
```

To swap only the storage definitions — repoint a schema, or move a source to Parquet —
override that one file instead:

```python
import os
os.environ["MDM_STORAGE_CONFIG"] = "/Volumes/<cat>/<sch>/<vol>/storage.config"
```

The run notebook has a `conf_dir` widget for the folder, and the CLI has `--conf-dir`.
Whatever you use, the bundled configs stay the fallback — a folder holding only
`base.json` still gets the packaged `storage.config`.

Rule and storage changes are the usual reasons to override; for anything the engine has to
be taught (a new format, say), rebuild the wheel.

## Adding a country

1. Copy `src/matching/conf/countries/template.json` (or `MY.json`) to `{CC}.json`.
2. Edit its `exact_match_rules`, `default_fuzzy_blocking` and `fuzzy_match_rules`.
3. Rebuild the wheel (or drop the file into your `MDM_CONF_DIR` folder).
4. Run it: `run_country(spark, "<CC>")`.

`template.json` and any `_*.json` are skipped, so they never run as a country.

Keep `golden_id_floor` identical in every country config, and only ever raise it — the
engine's minted ids have to stay disjoint from the range Informatica can still reach.

## What a run writes

Everything is a country slice, so re-running one country is safe and idempotent.

| Table | Contents |
|---|---|
| `MDMRowRegistry` | Stable `MDMRowId` per country + operator key; golden id crosswalk |
| `MDMGoldenIdSequence` | High-water mark for engine-minted ids (one global row) |
| `MDMGoldenIdHistory` | Append-only old → new id remaps per run (`MERGE` / `SPLIT`) |
| `MDMRuleResults` | Accepted edges per rule stage, incl. blocked Informatica-group bridges |
| `MDMRuleEvaluations` | Every fuzzy candidate pair and its scores, matched or not |
| `MDMMatchExclusions` | Stewardship "do not match" keys (you populate this) |
| `MDMMatchingState` | Intermediate id sets for the waterfall |
| `MDMMatchLinks` | The final match graph |
| `MDMComponentLabels` | Component labels per propagation iteration |
| `MDMMatchedResults` | The output |

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Required table … does not exist` | `sql/setup_tables.sql` has not been run in this environment |
| `Table … is missing required columns` | Tables were created from an older setup SQL — re-run it |
| `Missing country config: …` | No `countries/{CC}.json`; copy `template.json` |
| `did not converge within N iterations` | A match chain is longer than the budget — raise `components_max_iterations` |
| `Informatica golden groupings … would not be preserved` | A safety check fired; nothing was written. Inspect `MDMMatchLinks` / `MDMComponentLabels` for the country |
| `Informatica SourceGoldenRecordId values reach …` | Informatica has entered the engine id range — raise `golden_id_floor` everywhere |
