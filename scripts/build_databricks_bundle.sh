#!/usr/bin/env bash
# Build deployable artifacts for Databricks.
#
# Produces (under dist/):
#   1. mdm_engine_bundle.zip  — the repo layout (src/ conf/ sql/ notebooks/) to upload
#      to a Workspace folder / DBFS / Volume and import (add .../src to sys.path). This
#      is the simplest "import and run" path and keeps conf/ next to the code.
#   2. mdm_engine-<version>-py3-none-any.whl — a wheel of the matching/ + dq/ packages
#      to install as a cluster/job library. Configs are NOT in the wheel; point the
#      engine at them with the MDM_CONF_DIR env var (see docs/DEPLOYMENT.md).
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
DIST="$ROOT/dist"
STAGE="$DIST/mdm_engine"

rm -rf "$STAGE" "$DIST/mdm_engine_bundle.zip"
mkdir -p "$STAGE"

echo "== Staging bundle =="
cp -r src conf sql notebooks "$STAGE/"
cp README.md AGENTS.md "$STAGE/" 2>/dev/null || true
cp docs/DEPLOYMENT.md "$STAGE/" 2>/dev/null || true
# Drop caches from the staged copy.
find "$STAGE" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true

echo "== Zipping bundle =="
( cd "$DIST" && zip -qr mdm_engine_bundle.zip mdm_engine )
echo "  -> $DIST/mdm_engine_bundle.zip"

echo "== Building wheel =="
if python3 -m build --wheel --outdir "$DIST" >/dev/null 2>&1; then
  echo "  -> wheel built via 'python -m build'"
else
  echo "  -> 'python -m build' unavailable; falling back to 'pip wheel'"
  pip3 wheel . --no-deps -w "$DIST"
fi

echo "== Artifacts =="
ls -1sh "$DIST"/*.zip "$DIST"/*.whl 2>/dev/null || true
echo "Done."
