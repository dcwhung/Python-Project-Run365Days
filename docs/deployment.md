# Deployment

The same React app is deployed twice from `develop`: in **API mode** on
Vercel and in **static mode** on GitHub Pages. Both builds run
`run365-export` first, so no generated data is ever committed.

| | Vercel | GitHub Pages |
|---|---|---|
| URL | https://python-project-run365-days.vercel.app/ | https://dcwhung.github.io/Python-Project-Run365Days/ |
| Data mode | `api`: React queries `/api/graphql` | `static`: React fetches `/data/*.json` |
| Backend | `api/graphql.py`, a Python serverless function running the Flask + Strawberry app over the bundled SQLite file | none |
| Build | `scripts/vercel-build.sh` via `vercel.json` | `.github/workflows/pages.yml` |
| Trigger | every push to `develop` (production branch) | every push to `develop`, after lint and tests |

## Vercel

`vercel.json` is the whole configuration:

- `installCommand`: `cd frontend && npm ci`
- `buildCommand`: `bash scripts/vercel-build.sh`, which creates a
  throw-away virtualenv with `uv` under `/tmp`, installs the package,
  runs `run365-export --skip-static` with `RUN365_DATA_DIR` pointing at the
  checkout, then `npm run build`.
- `outputDirectory`: `frontend/dist`
- `functions["api/graphql.py"]`: `includeFiles` bundles
  `data/processed/run365.db`; `excludeFiles` keeps `data/raw`, `frontend`,
  `legacy`, `docs`, `tests`, `scripts` and `.github` out of the bundle.
- `rewrites`: `/api/*` to the function (Flask routes `/api/graphql` and
  `/api/health` itself), everything else to `index.html`.

The function file exposes a module-level `app`. It loads `run365days`
from `src/` if the package is somehow not importable and, if start-up
fails for any reason, serves a minimal app whose `/api/health` reports the
exception.

### What the first deployment taught

Each of these cost one failed build; they are recorded so nobody repeats
them.

| Symptom | Cause | Fix |
|---|---|---|
| `Function Runtimes must have a valid version` | `runtime: "python3.12"` in `functions` is only for community runtimes | remove the key; pin the interpreter with `.python-version` |
| `externally-managed-environment` from pip | the build image's Python is managed by uv (PEP 668) | `uv venv` + `uv pip install` in the build script |
| export wrote a 64 KB database with 0 activities | the package was installed non-editable, so `config.ROOT_DIR` resolved inside site-packages | export `RUN365_DATA_DIR=$PWD/data`; `run365-export` now fails loudly when the TCX folder is missing |
| `Total bundle size (395 MB) exceeds the maximum function size (225 MB)` | the venv, `frontend/node_modules` and `data/raw` were bundled with the function | venv under `/tmp`; `excludeFiles` in `vercel.json` |
| `ModuleNotFoundError: No module named 'flask'` at runtime | Vercel installs the project from `pyproject.toml` but ignores optional extras, so `[api]` was never installed | Flask and Strawberry are core dependencies |
| `The pattern "api/graphql.py" ... doesn't match any Serverless Functions` | the `app = ...` assignment had moved inside a `try:` block; Vercel detects a Python function by a module-level `app` | `app = _build_app()` at the top level |

The runtime log (Vercel project, Logs tab) was the only place the Flask
error was visible; the `/api/health` fallback exists so the next start-up
failure is readable in the browser.

## GitHub Pages

The workflow has four jobs. The first two run on every push and pull
request to `develop`; the last two only on pushes.

1. **Lint and test**: `ruff check`, `ruff format --check`, `pytest`, and
   `run365-schema --check frontend/schema.graphql`.
2. **Frontend**: `npm ci`, ESLint, codegen + `tsc`, Vitest, Vite build in
   both data modes.
3. **Build dashboard (static mode)**: `run365-export --skip-db --static-dir
   frontend/public/data`, then `npm run build:static` with
   `VITE_BASE_PATH=/<repo>/`. `dist/index.html` is copied to `404.html` so
   a deep link such as `/activity/7264441638` is served by Pages and picked
   up by the router.
4. **Deploy to GitHub Pages**.

The repository's `github-pages` environment must allow deployments from
`develop` (Settings, Environments, Deployment branches).

## Running it yourself

```bash
pip install -e ".[dev]"
run365-export                                   # SQLite + static JSON
flask --app run365days.api.app:create_app run   # http://127.0.0.1:5000/api/graphql
cd frontend && npm install && npm run dev       # http://localhost:5173 (api mode)
```

For static mode locally: `run365-export --static-dir frontend/public/data`
then `VITE_DATA_MODE=static npm run dev`.
