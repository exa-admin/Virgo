#!/usr/bin/env bash
# Build the deployable wheel: dist/mdm_engine-<version>-py3-none-any.whl
#
# The wheel contains the matching/ and dq/ packages plus the country configs
# (matching/conf/). Install it on a Databricks cluster or as a job library.
set -euo pipefail

cd "$(dirname "$0")/.."
rm -rf dist build src/*.egg-info

if ! python3 -m build --wheel --outdir dist; then
  echo "'python -m build' unavailable, falling back to 'pip wheel'"
  pip3 wheel . --no-deps -w dist
fi

find src -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
ls -1sh dist/*.whl
