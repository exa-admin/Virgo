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

## Option A — Deploy as a folder (recommended)

The whole project lives as one folder in Databricks; notebooks add `src/` to `sys.path`
and `conf/` resolves next to it. Pick whichever upload method you have access to.

### A1. Git Repos (no build step)

1. In Databricks: **Workspace → Repos → Add Repo**, paste this repo's Git URL. You now
   have a folder like `/Workspace/Repos/<you>/<repo>/` containing `src/ conf/ sql/ notebooks/`.
2. Open `notebooks/01_run_match_country.py`. It runs `%run ./00_path_setup` first, which
   finds `src/` and adds it to `sys.path` automatically. Set the `country_code` widget and
   run all cells. (Pull to update; branches/PRs work as usual.)

### A2. Databricks CLI — upload the folder

Build the folder locally, then import it into the Workspace:

```bash
./scripts/build_databricks_bundle.sh                 # creates dist/mdm_engine/
databricks workspace import-dir dist/mdm_engine \
    /Workspace/Users/<you>/mdm_engine --overwrite
```

(Or copy to a Unity Catalog Volume: `databricks fs cp -r dist/mdm_engine \
dbfs:/Volumes/<cat>/<sch>/<vol>/mdm_engine`.)

### A3. UI upload of the ZIP, then unzip

1. Upload `dist/mdm_engine_bundle.zip` to a Volume (Catalog UI) or DBFS.
2. In a notebook cell: `%sh unzip -o /Volumes/.../mdm_engine_bundle.zip -d /Volumes/.../`.

### Run it (any of A1–A3)

`notebooks/00_path_setup.py` locates `src/` for notebooks in the folder. From a plain
notebook you can also do it explicitly:

```python
import sys; sys.path.insert(0, "/Workspace/Users/<you>/mdm_engine/src")
from matching.pipeline import run_country
run_country(spark, "MY")   # global from conf/base.json + country from conf/countries/MY.json
```

`conf/` resolves automatically because it sits next to `src/` in the folder. If you ever
move `conf/` elsewhere, set `os.environ["MDM_CONF_DIR"]` to its path before importing.

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
