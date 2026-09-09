#!/usr/bin/env bash
# Idempotent Cloud Agent bootstrap for the MDM Match & Merge engine.
#
# The engine targets Databricks Runtime (PySpark + Delta Lake are provided there).
# For local editing, type-checking and Spark-native smoke runs we install a
# matching open-source stack: PySpark 4.0 + delta-spark 4.0, which support the
# JDK 21 available on the base image.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== MDM engine install =="

# --- Java (Spark requires a JVM; JDK 21 is preinstalled on the base image) -----
if ! command -v java >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends openjdk-21-jre-headless
fi
JAVA_HOME_GUESS="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
export JAVA_HOME="${JAVA_HOME:-$JAVA_HOME_GUESS}"
echo "Using JAVA_HOME=$JAVA_HOME"
java -version

# --- Python dependencies -------------------------------------------------------
# pyspark/delta-spark: local Spark runtime (Databricks provides these in prod).
# pandas/requests: required by the src/dq address-enrichment utilities.
PIP="pip3 install --break-system-packages"
$PIP --upgrade pip >/dev/null 2>&1 || true
$PIP "pyspark==4.0.0" "delta-spark==4.0.0" "pandas" "requests"

# Editable install so `import matching` / `import dq` resolve from src/ layout.
$PIP -e . || echo "editable install skipped (non-fatal)"

echo "== Verifying imports =="
PYTHONPATH=src python3 - <<'PY'
import importlib
mods = [
    "matching.config", "matching.pipeline", "matching.standardize",
    "matching.golden_ids", "matching.components", "matching.source_golden_groups",
    "matching.match_pipeline", "matching.exact_match", "matching.fuzzy_match",
    "matching.delta_io", "matching.utils", "dq.address_enrichment", "dq.enrich_operators",
]
for m in mods:
    importlib.import_module(m)
print(f"OK: imported {len(mods)} modules")
PY

echo "== Install complete =="
