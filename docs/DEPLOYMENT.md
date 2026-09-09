# Deploying the MDM Match engine to Databricks

The engine is Databricks-oriented: PySpark + Delta Lake are provided by the Databricks
Runtime. Two supported deployment shapes are produced by
`scripts/build_databricks_bundle.sh` (outputs land in `dist/`):

| Artifact | Use it when |
|----------|-------------|
| `mdm_engine_bundle.zip` | You want to upload the whole project (code + `conf/` + `sql/` + `notebooks/`) and run it as-is. Simplest. |
| `mdm_engine-<ver>-py3-none-any.whl` | You want the engine installed as a cluster/job **library**; configs live separately (set `MDM_CONF_DIR`). |

Build both:

```bash
./scripts/build_databricks_bundle.sh
```

## One-time setup (either shape)

1. Create the Delta tables: run `sql/setup_tables.sql` (SQL editor or a notebook cell). It
   already targets `pds_auroradsar_prod.schema_informatica`, which is also the
   `target_schema` in `conf/base.json` — run it as-is; no edits needed. (Only change that
   name, in both places, if your workspace genuinely uses a different catalog/schema.)
2. Confirm the source views are readable:
   - `sl_bdl_processed_cd_prod.cd.vw_ufsoperator` (operator / population to match)
   - `sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden` (golden / already-matched masters)

   These are set in `conf/base.json` under `source` / `golden_source`. To read files
   instead of tables, change the spec (see "Swapping the source format" below).

## Option A — Deploy as a folder via the Workspace Import UI (no Volume, no CLI, no Git)

The whole project lives as one folder in your Workspace; the run notebook sits next to
`src/`, adds it to `sys.path`, and `conf/` resolves next to it.

1. **Build the bundle locally** (once): `./scripts/build_databricks_bundle.sh` →
   `dist/mdm_engine_bundle.zip`.

2. **Import the zip into your Workspace:**
   - Databricks sidebar → **Workspace → Users → `<your user>`**.
   - Click the **⋮ (kebab)** on your user folder (or right-click) → **Import**.
   - In the dialog choose **File**, drop in `mdm_engine_bundle.zip`, and click **Import**.
   - Databricks expands the archive into a folder (`mdm_engine/`) containing
     `src/ conf/ sql/ notebooks/`. The engine modules (`matching/`, `dq/`) and `conf/*.json`
     land as **Workspace files** (importable); the `notebooks/*.py` become **notebooks**.

   This needs Workspace Files, which is on by default in current Databricks. Non-notebook
   files import as files because they lack the `# Databricks notebook source` header.

3. **Create the tables** (one time): open `mdm_engine/sql/setup_tables.sql`, copy it into a
   SQL editor/cell and run it. It already targets `pds_auroradsar_prod.schema_informatica`
   — run as-is.

4. **Run one country:** open `mdm_engine/notebooks/01_run_match_country.py` and set widgets:
   - `src_path` = **blank** — it auto-resolves `src/` because the notebook sits next to it
     in the same Workspace folder (e.g. `/Workspace/Users/<you>/mdm_engine/src`).
   - `country_code` = `MY`, `run_mode` = `country`.

   Run all cells → it calls `run_country(spark, "MY")` and displays the results. `conf/` is
   found automatically next to `src/`.

   Or, from any notebook cell without the entry notebook:

   ```python
   import sys
   sys.path.insert(0, "/Workspace/Users/<you>/mdm_engine/src")
   from matching.pipeline import run_country
   run_country(spark, "MY")   # global from conf/base.json + country from conf/countries/MY.json
   ```

> If your workspace imports the whole zip as notebooks (older workspaces without Workspace
> Files), the engine modules won't be importable. In that case use a **Volume** upload
> (below) or **Repos**/**CLI**.

### Alternatives

- **Volume upload:** upload `mdm_engine_bundle.zip` to a UC Volume (**Catalog → Volume →
  Upload to this volume**), unzip in a `%sh` cell
  (`unzip -o /Volumes/<cat>/<sch>/<vol>/mdm_engine_bundle.zip -d /Volumes/<cat>/<sch>/<vol>/mdm_engine_app`),
  import just `01_run_match_country.py` into the Workspace, and set its `src_path` widget to
  `.../mdm_engine_app/mdm_engine/src` (a notebook can't execute from a Volume, but it can
  import code from one). Unzip into a Volume — not `/tmp` or `/databricks/driver`, which are
  wiped on restart.
- **Git Repos:** Workspace → Repos → Add Repo → paste the Git URL, open
  `notebooks/01_run_match_country.py`, leave `src_path` blank (auto-resolves).
- **Databricks CLI:** `databricks workspace import-dir dist/mdm_engine /Workspace/Users/<you>/mdm_engine --overwrite`.

## Option B — Wheel (cluster/job library)

1. Install `mdm_engine-<ver>-py3-none-any.whl` on the cluster or as a job library.
2. Upload the `conf/` folder somewhere readable (Workspace/Volume/DBFS) and tell the
   engine where it is via an environment variable **before importing**:

   ```python
   import os
   os.environ["MDM_CONF_DIR"] = "/Volumes/<cat>/<sch>/<vol>/mdm_engine/conf"
   from matching.pipeline import run_country, run_all
   run_country(spark, "MY")   # one country
   # or every country in conf/countries/: run_all(spark)
   ```

   (The wheel does not bundle `conf/`; `MDM_CONF_DIR` points the loader at it. Without
   it, the loader falls back to the repo-relative `conf/`.)

## Configuration model

- `conf/base.json` — cross-country defaults (source specs, `EnrichDate`, `standardization`,
  `invalid_values`, `exclude_from_match_filters`, `golden_id_floor`, `target_schema`, ...).
- `conf/countries/{CC}.json` — per-country **match rules** (`exact_match_rules`,
  `default_fuzzy_blocking`, `fuzzy_match_rules`) plus any base overrides; merged on top of
  base (country wins). `filter_condition` defaults to `CountryCode = '<CC>'`. Add a country
  by copying `conf/countries/template.json` (or an existing country like `MY.json`) to
  `{CC}.json` and editing its rules.

## Swapping the source format

`source` / `golden_source` are declarative, so no code change is needed to move from
Delta tables to files:

```json
"source":        { "format": "parquet", "path": "/Volumes/cat/sch/vol/operator/" },
"golden_source": { "format": "csv", "path": "/Volumes/cat/sch/vol/golden/",
                   "options": { "header": "true", "multiLine": "true" } }
```

Supported formats: `delta` (use `table` or `path`), `csv`, `parquet`. Reading is isolated
in `src/matching/sources.py`; the rest of the engine is unaffected.
