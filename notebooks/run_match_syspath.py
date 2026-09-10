# Databricks notebook source
# MAGIC %md
# MAGIC # MDM Match — run from SOURCE via `sys.path` (learning / debugging)
# MAGIC
# MAGIC Same run as `run_match.py`, but the engine is imported from the **repo folder in
# MAGIC your Workspace** instead of the installed wheel. Nothing here is a different
# MAGIC engine — same `run_country`, same configs. Only *where Python finds it* changes.
# MAGIC
# MAGIC | | `run_match.py` | this notebook |
# MAGIC |---|---|---|
# MAGIC | Engine comes from | the wheel, installed as a cluster/job library | `src/` in your Workspace folder |
# MAGIC | Edit → see the change | rebuild wheel, re-upload, re-install, restart | edit the file, re-run two cells |
# MAGIC | Use it for | **production and scheduled jobs** | learning, debugging, trying a fix |
# MAGIC
# MAGIC > Deployment stays the wheel (`docs/DEPLOYMENT.md`). This notebook is a development
# MAGIC > convenience — do not point a scheduled job at it.
# MAGIC
# MAGIC ## Get the repo into your Workspace first
# MAGIC
# MAGIC You need the whole repo, not just this file:
# MAGIC
# MAGIC - **Git folder (best):** Workspace → Create → Git folder → paste the repo URL.
# MAGIC   Lands at `/Workspace/Repos/<you>/Virgo`.
# MAGIC - **Zip import:** zip the project, then Workspace → ⋮ → Import → File.
# MAGIC   Lands at `/Workspace/Users/<you>/Virgo`.
# MAGIC
# MAGIC Then open **this** notebook from inside that folder, so it can find the repo root on
# MAGIC its own.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `repo_path` | Blank = work it out from this notebook's own path |
# MAGIC | `countries` | `MY`, `MY,SG,TH`, or `ALL` |
# MAGIC | `conf_dir` | Blank = the configs in `src/matching/conf/` of this checkout |

# COMMAND ----------

# Databricks injects `spark`, `dbutils` and `display` into every notebook. This block is
# never executed (TYPE_CHECKING is False at runtime) — it only tells the IDE where those
# names come from, so editors stop reporting them as unresolved.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import dbutils, display, spark

# COMMAND ----------

dbutils.widgets.text("repo_path", "", "Repo root (blank = auto-detect from this notebook)")
dbutils.widgets.text("countries", "MY", "Countries (CC, CC,CC or ALL)")
dbutils.widgets.text("conf_dir", "", "Config folder (blank = this checkout's conf/)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Find the repo on the driver filesystem
# MAGIC
# MAGIC Workspace files are mounted on the driver under `/Workspace`, so a notebook at
# MAGIC `/Users/you/Virgo/notebooks/run_match_syspath` is really the file
# MAGIC `/Workspace/Users/you/Virgo/notebooks/run_match_syspath.py`. That is what makes
# MAGIC `sys.path` work here at all — Python needs a real directory to import from.

# COMMAND ----------

import sys
from pathlib import Path


def _repo_root() -> Path:
    """This checkout's root. Same detection as notebooks/run_tests.py."""
    override = dbutils.widgets.get("repo_path").strip()
    if override:
        return Path(override)
    notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # notebooks/run_match_syspath -> the repo root is its grandparent, under /Workspace.
    return Path("/Workspace") / str(notebook).lstrip("/").rsplit("/", 2)[0]


repo_root = _repo_root()
src_path = repo_root / "src"

if not (src_path / "matching").is_dir():
    raise SystemExit(
        f"No src/matching under {repo_root}. Import the whole repo (not just this notebook), "
        "or set the repo_path widget to the folder that holds src/."
    )

print(f"repo root : {repo_root}")
print(f"src       : {src_path}")
print(f"modules   : {sorted(p.name for p in (src_path / 'matching').glob('*.py'))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What `sys.path` actually is
# MAGIC
# MAGIC `sys.path` is an ordered list of directories. On `import matching`, Python walks it
# MAGIC **top to bottom** and stops at the first hit — so position decides which copy wins.
# MAGIC A cluster with the wheel installed already has a `matching` in `site-packages`;
# MAGIC inserting our `src/` at index 0 puts the source checkout ahead of it.
# MAGIC
# MAGIC Two things people trip over:
# MAGIC
# MAGIC 1. `sys.path` is consulted **only on a fresh import**. A module already in
# MAGIC    `sys.modules` is handed back from cache and the path is never consulted — which
# MAGIC    is why cell 3 purges the cache.
# MAGIC 2. `.insert(0, ...)` wins; `.append(...)` loses to everything already there,
# MAGIC    including the wheel.

# COMMAND ----------

# Is a wheel also installed? Not a problem — but it is what we are overriding, so show it.
from importlib.metadata import PackageNotFoundError, distribution

try:
    installed = distribution("mdm-engine")
    print(f"wheel on this cluster : mdm-engine {installed.version}")
    print(f"                        {installed.locate_file('matching')}")
    print("  -> src/ is inserted ahead of it below, so the SOURCE wins.\n")
except PackageNotFoundError:
    print("wheel on this cluster : none (source is the only copy)\n")

print("sys.path BEFORE (first 5):")
for i, entry in enumerate(sys.path[:5]):
    print(f"  [{i}] {entry}")

# The engine imports as `matching` / `dq`, and both live directly under src/ — so src/ is
# the directory that goes on the path, never src/matching itself.
if str(src_path) in sys.path:
    sys.path.remove(str(src_path))  # keep it at index 0 on a re-run, not buried mid-list
sys.path.insert(0, str(src_path))

print("\nsys.path AFTER (first 5):")
for i, entry in enumerate(sys.path[:5]):
    marker = "  <-- our checkout" if entry == str(src_path) else ""
    print(f"  [{i}] {entry}{marker}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drop cached modules
# MAGIC
# MAGIC **This is the cell to re-run after editing a source file.** Python caches every
# MAGIC imported module in `sys.modules`; without clearing it, a second `import matching`
# MAGIC returns the *old* object and your edit appears to do nothing.
# MAGIC
# MAGIC Deleting the whole `matching.*` tree — not just `matching` — matters, because
# MAGIC `matching.pipeline`, `matching.rules` and friends are cached separately and hold
# MAGIC references to each other.

# COMMAND ----------

def purge_engine_modules() -> list:
    """Forget every cached matching/dq module, so the next import re-reads the files."""
    stale = sorted(
        name
        for name in sys.modules
        if name == "matching" or name.startswith("matching.") or name == "dq" or name.startswith("dq.")
    )
    for name in stale:
        del sys.modules[name]
    return stale


purged = purge_engine_modules()
print(f"purged {len(purged)} cached module(s): {purged}" if purged else "nothing cached yet — first run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Import, and prove where it came from
# MAGIC
# MAGIC `module.__file__` is the honest answer to "which copy am I running?". If it points
# MAGIC into `site-packages`, you are on the wheel and the cells above did not take effect.

# COMMAND ----------

import os

conf_dir = dbutils.widgets.get("conf_dir").strip()
if conf_dir:
    # Same override as the wheel path: configs from a folder you can edit in place.
    os.environ["MDM_CONF_DIR"] = conf_dir

import matching
from matching import available_countries, run_country
from matching.config import dataset_table, resolve_config, storage_config_path

engine_file = Path(matching.__file__).resolve()
from_source = engine_file.is_relative_to(src_path.resolve())

print(f"matching.__file__ : {engine_file}")
print(f"storage.config    : {storage_config_path()}")
print(f"loaded from       : {'SOURCE checkout' if from_source else 'INSTALLED WHEEL'}")

if not from_source:
    raise SystemExit(
        "Still importing the wheel. Re-run cells 2 and 3 in order (path insert, then "
        "module purge) — a cached import ignores sys.path."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run
# MAGIC
# MAGIC Identical to `run_match.py` from here on: country slices, failures collected so one
# MAGIC bad country does not stop the batch. Table names come from `conf/storage.config` —
# MAGIC **this checkout's copy**, which is the other half of what makes debugging easy.

# COMMAND ----------

requested = dbutils.widgets.get("countries").strip().upper()
countries = available_countries() if requested == "ALL" else [c.strip() for c in requested.split(",") if c.strip()]

storage = resolve_config(countries[0]) if countries else None
print(f"Configured countries : {available_countries()}")
print(f"Running              : {countries}")
print(f"Target schema        : {storage['target_schema'] if storage else '(none)'}")

# COMMAND ----------

results, failures = {}, {}

for country_code in countries:
    print(f"\n{'=' * 70}\n{country_code}\n{'=' * 70}")
    try:
        results[country_code] = run_country(spark, country_code)
    except Exception as error:  # noqa: BLE001 - report every country before failing
        failures[country_code] = error
        print(f"!! {country_code} FAILED: {type(error).__name__}: {error}")

print(f"\nDone. Succeeded: {sorted(results)}  Failed: {sorted(failures)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary per country
# MAGIC
# MAGIC `golden_id_changed` should be **0** while a country is still being served by
# MAGIC Informatica — that column is the migration safety check.

# COMMAND ----------

from pyspark.sql import functions as F

if results:
    summary = None
    for country_code, df in results.items():
        row = df.agg(
            F.lit(country_code).alias("country"),
            F.count("*").alias("records"),
            F.countDistinct("golden_id").alias("golden_ids"),
            F.sum(F.col("is_matched").cast("int")).alias("matched"),
            F.sum(F.col("golden_id_is_new").cast("int")).alias("new_golden_ids"),
            F.sum(F.col("golden_id_changed").cast("int")).alias("golden_id_changed"),
        )
        summary = row if summary is None else summary.unionByName(row)
    display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Golden id changes traced this run

# COMMAND ----------

if results and storage:
    country_list = ", ".join(f"'{c}'" for c in sorted(results))
    display(
        spark.sql(f"""
            SELECT ChangeReason, COUNT(*) AS records, COUNT(DISTINCT OperatorConcatId) AS operators
            FROM {dataset_table(storage, "change_log")}
            WHERE CountryCode IN ({country_list})
              AND RunTimestamp >= current_timestamp() - INTERVAL 1 DAY
            GROUP BY ChangeReason ORDER BY records DESC
        """)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## The debug loop
# MAGIC
# MAGIC 1. Edit a file under `src/matching/` — in the Workspace editor, or locally and push
# MAGIC    if this is a Git folder (then **Pull** in the Git folder UI).
# MAGIC 2. Re-run **cell 3** (purge) and **cell 4** (import).
# MAGIC 3. Re-run the run cell.
# MAGIC
# MAGIC No rebuild, no re-upload, no cluster restart.
# MAGIC
# MAGIC ### Getting information out
# MAGIC
# MAGIC The engine has no Python UDFs, so a `print()` in `pipeline.py` runs on the driver
# MAGIC and shows up in the cell output. Inside a Spark job it would land in the executor
# MAGIC logs instead — prefer inspecting the intermediate Delta tables
# MAGIC (`MDMRuleResults`, `MDMRuleEvaluations`, `MDMComponentLabels`) over printing from a
# MAGIC transformation. To poke at one function without a full run:
# MAGIC
# MAGIC ```python
# MAGIC from matching import io
# MAGIC from matching.config import resolve_config
# MAGIC
# MAGIC cfg = resolve_config("MY")
# MAGIC display(io.read(spark, cfg, "operator").limit(20))       # any dataset by name
# MAGIC display(io.read_source_population(spark, cfg).limit(20))  # what the engine sees
# MAGIC ```
# MAGIC
# MAGIC ### Gotchas
# MAGIC
# MAGIC - **Nothing changed after an edit?** You skipped the purge cell, or re-ran the
# MAGIC   import before it. Order is: path → purge → import.
# MAGIC - **`%pip install` restarts Python**, wiping `sys.path` edits *and* every variable.
# MAGIC   Run it before these cells, never between them.
# MAGIC - **`ModuleNotFoundError: matching`** — `src/` did not make it onto the path, or you
# MAGIC   imported only the notebook rather than the whole repo. Check cell 1's output.
# MAGIC - **Editing configs**: `conf/` lives inside `src/matching/`, so this notebook already
# MAGIC   reads this checkout's `storage.config` and `countries/*.json`. Leave `conf_dir`
# MAGIC   blank unless you deliberately want a folder elsewhere.
# MAGIC - **Do not deploy this way.** Jobs get the wheel: a Workspace folder is mutable and
# MAGIC   unversioned at run time, which is exactly what you do not want in production.
