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

1. Create the Delta tables: open `sql/setup_tables.sql`, replace
   `pds_auroradsar_prod.schema_informatica` with your target `catalog.schema`, and run it
   (SQL editor or a notebook cell). This is also the `target_schema` value in
   `conf/base.json` — keep them in sync.
2. Confirm the source views are readable:
   - `sl_bdl_processed_cd_prod.cd.vw_ufsoperator` (operator / population to match)
   - `sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden` (golden / already-matched masters)

   These are set in `conf/base.json` under `source` / `golden_source`. To read files
   instead of tables, change the spec (see "Swapping the source format" below).

## Option A — ZIP bundle (upload & import)

1. Upload `mdm_engine_bundle.zip` to the Workspace (or a Volume/DBFS) and unzip so you
   have a folder like `.../mdm_engine/` containing `src/`, `conf/`, `notebooks/`, `sql/`.
2. In a notebook, put `src/` on the path and run — `notebooks/00_path_setup.py` does this
   automatically, or manually:

   ```python
   import sys; sys.path.insert(0, "/Workspace/.../mdm_engine/src")
   from matching.config import load_country_config
   from matching.pipeline import run_country
   run_country(spark, "MY", load_country_config("MY"))
   ```

   `conf/` resolves automatically because it sits next to `src/` in the bundle.

## Option B — Wheel (cluster/job library)

1. Install `mdm_engine-<ver>-py3-none-any.whl` on the cluster or as a job library.
2. Upload the `conf/` folder somewhere readable (Workspace/Volume/DBFS) and tell the
   engine where it is via an environment variable **before importing**:

   ```python
   import os
   os.environ["MDM_CONF_DIR"] = "/Workspace/.../mdm_engine/conf"
   from matching.config import load_country_config
   from matching.pipeline import run_country, run_all
   run_country(spark, "MY", load_country_config("MY"))
   # or every country in conf/countries/: run_all(spark)
   ```

   (The wheel does not bundle `conf/`; `MDM_CONF_DIR` points the loader at it. Without
   it, the loader falls back to the repo-relative `conf/`.)

## Configuration model

- `conf/base.json` — master config shared by every country (source specs, `EnrichDate`,
  `standardization`, match rules, `invalid_values`, `target_schema`, ...).
- `conf/countries/{CC}.json` — per-country **overrides only**; merged on top of base
  (country wins). `filter_condition` defaults to `CountryCode = '<CC>'`. An empty `{}`
  inherits all base defaults (see `MY.json`). Add a country by copying
  `conf/countries/template.json` to `{CC}.json`.

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
