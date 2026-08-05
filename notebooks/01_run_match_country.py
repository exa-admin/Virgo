# Databricks notebook source
# MAGIC %md
# MAGIC # MDM Match — run country
# MAGIC
# MAGIC Thin entrypoint. Ensures `src/` is importable, then calls `run_country` or `run_all`.
# MAGIC
# MAGIC Prerequisites: run `sql/setup_tables.sql` once per environment.

# COMMAND ----------

from __future__ import annotations

import sys
from pathlib import Path

# Adjust REPO_ROOT to where this repository is checked out in the workspace.
REPO_ROOT = Path("/Workspace/Repos/Engine")  # <-- change if needed
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from matching.config import load_country_config  # noqa: E402
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
    cfg = load_country_config(country_code)
    result_df = run_country(spark, country_code, cfg)
    display(result_df.limit(100))
