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

## Option A — Deploy as a folder via UI upload (no CLI, no Git)

The whole project lives as one folder; a notebook adds `src/` to `sys.path` and `conf/`
resolves next to it. This is the pure point-and-click path.

1. **Build the bundle locally** (once): `./scripts/build_databricks_bundle.sh` →
   `dist/mdm_engine_bundle.zip`.

2. **Upload the zip to a Unity Catalog Volume** (a Volume is the UI-friendly place for
   arbitrary files):
   - Databricks left nav → **Catalog** → pick a catalog/schema → a **Volume** (create one
     with **Create → Volume** if needed).
   - Click **Upload to this volume** and select `mdm_engine_bundle.zip`. It lands at
     `/Volumes/<catalog>/<schema>/<volume>/mdm_engine_bundle.zip`.

3. **Unzip it (once) from a notebook cell** into the same Volume:

   ```python
   %sh
   cd /Volumes/<catalog>/<schema>/<volume>
   unzip -o mdm_engine_bundle.zip -d mdm_engine_app
   ls mdm_engine_app/mdm_engine        # -> conf  notebooks  sql  src
   ```

   You now have the folder at
   `/Volumes/<catalog>/<schema>/<volume>/mdm_engine_app/mdm_engine`.

4. **Create the tables** (one time): open
   `.../mdm_engine_app/mdm_engine/sql/setup_tables.sql`, copy it into a SQL cell/editor and
   run it. It already targets `pds_auroradsar_prod.schema_informatica` — run as-is.

5. **Run the match** from a Python notebook:

   ```python
   import sys
   sys.path.insert(0, "/Volumes/<catalog>/<schema>/<volume>/mdm_engine_app/mdm_engine/src")
   from matching.pipeline import run_country
   run_country(spark, "MY")   # global from conf/base.json + country from conf/countries/MY.json
   ```

   `conf/` resolves automatically because it sits next to `src/` inside the unzipped folder.

### Run one country via `notebooks/01_run_match_country.py`

`01_run_match_country.py` is a ready-made single-country entry point (widgets:
`country_code`, `run_mode`, `src_path`). To run one country set `run_mode = country` and
`country_code = MY` — it calls `run_country(spark, "MY")` for just that country.

Important: a notebook only executes when it lives in the **Workspace/Repos**, not in a
Volume. So with the Volume upload above:

1. Import the notebook into your Workspace: **Workspace → Import → File**, and pick
   `notebooks/01_run_match_country.py` from your machine (the copy inside the unzipped
   bundle also works). A matching `00_path_setup.py` is optional.
2. Open it and set the widgets:
   - `src_path` = `/Volumes/<catalog>/<schema>/<volume>/mdm_engine_app/mdm_engine/src`
     (so it imports the code you uploaded to the Volume; leave blank only when the whole
     folder — including `src/` — is in the Workspace next to the notebook, where it
     auto-resolves).
   - `country_code` = `MY`, `run_mode` = `country`.
3. Run all cells. `conf/` is still found next to that `src/` on the Volume.

> Tip: unzip into a **Volume** (persistent), not `/tmp` or `/databricks/driver` (wiped when
> the cluster restarts). To keep `conf/` somewhere other than next to `src/`, set
> `os.environ["MDM_CONF_DIR"] = ".../mdm_engine/conf"` before importing `matching`.

### Alternatives (if you later have CLI or Git access)

- **Git Repos**: Workspace → Repos → Add Repo → paste the Git URL, then open
  `notebooks/01_run_match_country.py`. Because the notebook sits next to `src/` in the repo,
  leave the `src_path` widget blank — it auto-resolves `src/`.
- **Databricks CLI**: `databricks workspace import-dir dist/mdm_engine /Workspace/Users/<you>/mdm_engine --overwrite`.

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
