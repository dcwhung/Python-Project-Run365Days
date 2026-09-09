#!/usr/bin/env bash
# Vercel build: export the processed data, then build the React app in api mode.
#
# 1. Install the package so the run365-export CLI is available. Vercel's build
#    image manages Python with uv (PEP 668), so a throw-away virtualenv is
#    created with uv when present; plain pip is the fallback elsewhere.
# 2. run365-export parses data/raw/ once and writes data/processed/run365.db,
#    which vercel.json bundles into the api/graphql.py function via includeFiles.
# 3. The static JSON output is skipped: on Vercel the app talks to the API.
set -euo pipefail
cd "$(dirname "$0")/.."

# The package is installed non-editable below, so config.ROOT_DIR would
# point into site-packages; anchor the data directory to this checkout.
export RUN365_DATA_DIR="$PWD/data"

# The virtualenv lives outside the checkout so it is never bundled into the function.
VENV="${TMPDIR:-/tmp}/run365-build-venv"
if command -v uv >/dev/null 2>&1; then
  uv venv --quiet "$VENV"
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
  uv pip install --quiet "."
else
  python3 -m pip install --quiet --disable-pip-version-check --break-system-packages "."
fi

run365-export --skip-static
ls -la data/processed/run365.db

cd frontend
npm run build
