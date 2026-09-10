# Databricks notebook source
import sys

REPO_SRC = "/Workspace/Users/your.name@company.com/Virgo/src"

# COMMAND ----------

for name in [m for m in sys.modules if m == "matching" or m.startswith("matching.") or m == "dq" or m.startswith("dq.")]:
    del sys.modules[name]

if REPO_SRC in sys.path:
    sys.path.remove(REPO_SRC)
sys.path.insert(0, REPO_SRC)

# COMMAND ----------

import matching
from matching import run_country

print(matching.__file__)

# COMMAND ----------

df = run_country(spark, "MY")
display(df)
