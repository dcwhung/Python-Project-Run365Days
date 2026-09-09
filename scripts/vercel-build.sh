#!/usr/bin/env bash
# Vercel build: export the processed data, then build the React app in api mode.
#
# 1. `pip install .` makes the run365-export CLI available in the build image.
# 2. run365-export parses data/raw/ once and writes data/processed/run365.db,
#    which vercel.json bundles into the api/graphql.py function via includeFiles.
# 3. The static JSON output is skipped: on Vercel the app talks to the API.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m pip install --quiet --disable-pip-version-check ".[api]"
run365-export --skip-static
ls -la data/processed/run365.db

cd frontend
npm run build
