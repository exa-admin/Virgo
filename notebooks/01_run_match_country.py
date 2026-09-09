# Databricks notebook source
# MAGIC %md
# MAGIC # MDM Match — run country
# MAGIC
# MAGIC Thin entrypoint. Ensures `src/` is importable, then calls `run_country` or `run_all`.
# MAGIC
# MAGIC Prerequisites: run `sql/setup_tables.sql` once per environment.
# MAGIC Requires `conf/countries/{CC}.json` (e.g. `MY.json`). Missing files raise — there is no embedded fallback.
# MAGIC To add a country, copy `conf/countries/template.json` and edit.

# COMMAND ----------

from __future__ import annotations

import sys
from pathlib import Path

# --- Locate the engine's src/ so `import matching` works ------------------------
# Set the `src_path` widget to your uploaded .../mdm_engine/src (e.g. a Volume path)
# for UI/folder deploys. Left blank, it auto-resolves by walking up from this
# notebook (works for Git Repos / Workspace where notebooks sit next to src/).
dbutils.widgets.text("src_path", "")  # e.g. /Volumes/<cat>/<sch>/<vol>/mdm_engine_app/mdm_engine/src


def _looks_like_src(candidate: Path) -> bool:
    return (candidate / "matching").is_dir()


def _resolve_src() -> Path:
    override = dbutils.widgets.get("src_path").strip()
    if override:
        src = Path(override)
        if not _looks_like_src(src):
            raise ModuleNotFoundError(f"src_path '{src}' has no matching/ package inside it.")
        return src.resolve()
    starts: list[Path] = []
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        starts.append(Path("/Workspace") / str(nb).lstrip("/"))
    except Exception:
        pass
    starts.append(Path.cwd())
    for start in starts:
        for parent in [start, *start.parents]:
            if _looks_like_src(parent / "src"):
                return (parent / "src").resolve()
    raise ModuleNotFoundError(
        "Could not auto-find the engine src/. Set the 'src_path' widget to your uploaded "
        ".../mdm_engine/src folder (e.g. a /Volumes/... path) and re-run."
    )


SRC = _resolve_src()
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
print(f"Using engine src: {SRC}")

from matching.pipeline import run_all, run_country  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("country_code", "MY")
dbutils.widgets.dropdown("run_mode", "country", ["country", "all"])

country_code = dbutils.widgets.get("country_code").strip().upper()
run_mode = dbutils.widgets.get("run_mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

if run_mode == "all":
    run_all(spark)
else:
    # Config is resolved from country_code: global settings from conf/base.json,
    # country-specific rules/overrides from conf/countries/{country_code}.json.
    result_df = run_country(spark, country_code)
    display(result_df)
