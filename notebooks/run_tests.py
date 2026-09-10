# Databricks notebook source
# MAGIC %md
# MAGIC # MDM Match — run the test suite
# MAGIC
# MAGIC Each test creates a throwaway schema from the real `sql/setup_tables.sql`, loads a
# MAGIC small dummy source population, runs the real `run_country` with the real MY rules,
# MAGIC asserts on `MDMMatchedResults`, then drops the schema.
# MAGIC
# MAGIC ## Steps
# MAGIC
# MAGIC 1. **Get the repo into your Workspace** — the tests need `tests/` and `sql/`, not
# MAGIC    just the wheel. Workspace → Create → **Git folder** and paste the repo URL, or
# MAGIC    zip the project and Workspace → ⋮ → **Import** → File.
# MAGIC 2. **Attach this notebook to a cluster** (DBR 13.3 LTS or newer; any size — the test
# MAGIC    data is a handful of rows).
# MAGIC 3. **Set `test_schema`** to a scratch schema you can create and drop tables in,
# MAGIC    e.g. `pds_auroradsar_dev.mdm_test`.
# MAGIC 4. **Run All.** The last cell fails the notebook if any test fails, so this also
# MAGIC    works as a job task.
# MAGIC
# MAGIC > ⚠️ The suite **drops and recreates `test_schema` between tests**. Point it at
# MAGIC > scratch — it refuses to start against `schema_informatica`.
# MAGIC
# MAGIC To run a subset, put a filter in `pytest_args`, e.g. `-k undermatch` or
# MAGIC `-vv -k test_fuzzy_matches_a_name_typo`. More in `docs/TESTING.md`.

# COMMAND ----------

# Databricks injects `dbutils`. Never executed at runtime — it only tells the IDE where
# the name comes from.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import dbutils

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("repo_path", "", "Repo root (blank = auto-detect from this notebook)")
dbutils.widgets.text("test_schema", "mdm_test", "Scratch schema for the tests")
dbutils.widgets.text("pytest_args", "-q", "Extra pytest args, e.g. -k undermatch")

# COMMAND ----------

import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    override = dbutils.widgets.get("repo_path").strip()
    if override:
        return Path(override)
    notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # notebooks/run_tests -> repo root is its grandparent, under /Workspace on the driver FS.
    return Path("/Workspace") / str(notebook).lstrip("/").rsplit("/", 2)[0]


repo_root = _repo_root()
if not (repo_root / "tests").is_dir():
    raise SystemExit(
        f"No tests/ under {repo_root}. Import the whole repo (not just the notebook), "
        "or set the repo_path widget."
    )

test_schema = dbutils.widgets.get("test_schema").strip()
if test_schema.endswith("schema_informatica"):
    raise SystemExit("Refusing to run the tests against the production schema — they drop it between tests.")

os.environ["MDM_TEST_SCHEMA"] = test_schema
# Import the engine from source so the tests cover this checkout, not an installed wheel.
sys.path.insert(0, str(repo_root / "src"))
sys.path.insert(0, str(repo_root / "tests"))

print(f"repo   : {repo_root}")
print(f"schema : {test_schema}")

# COMMAND ----------

import pytest

args = [str(repo_root / "tests"), *dbutils.widgets.get("pytest_args").split()]
exit_code = pytest.main(args)

print(f"\npytest exit code: {exit_code}  (0 = all passed)")
assert exit_code == 0, "Test suite failed — see the output above."
