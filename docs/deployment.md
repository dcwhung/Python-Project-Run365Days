# Deployment

The same React app is deployed twice from the deploy branch: in **API
mode** on Vercel and in **static mode** on GitHub Pages. Both builds run
`run365-export` first, so no generated data is ever committed. The
[GitHub Pages](#github-pages) section below names that branch, generated
from the workflow.

| | Vercel | GitHub Pages |
|---|---|---|
| URL | https://python-project-run365-days.vercel.app/ | https://dcwhung.github.io/Python-Project-Run365Days/ |
| Data mode | `api`: React queries `/api/graphql` | `static`: React fetches `/data/*.json` |
| Backend | `api/graphql.py`, a Python serverless function running the Flask + Strawberry app over the bundled SQLite file | none |
| Build | `scripts/vercel-build.sh` via `vercel.json` | `.github/workflows/pages.yml` |
| Trigger | every push to the Vercel production branch | every push to the deploy branch, after lint and tests |

## Vercel

The production branch should be `develop`. Vercel keeps that setting in
the project dashboard (Settings, Git, Production Branch) rather than in
the repository, so it cannot be read or changed from this checkout —
confirm it there after any change to the branch layout.

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

The workflow has four jobs. The first two are the checks and run wherever
the workflow is triggered; the last two build and deploy the site and are
restricted to one branch by a `github.ref` test. The rules are read out of
`pages.yml` by `run365-ci-docs`, which job 1 runs in `--check` mode against
this file, `README.md` and `docs/architecture.md`:

<!-- ci-facts:start -->
<!-- Generated from .github/workflows/pages.yml by `run365-ci-docs --write`.
     Change the workflow, re-run the command, and commit both. -->
- Lint, tests and the frontend checks run on every push and pull request to
  `develop` and `master`.
- The site is built and deployed from `develop` only; every other branch
  stops after the checks.
- A manual `workflow_dispatch` run follows the same rule: it re-deploys when
  run on `develop`, and is checks-only elsewhere.
<!-- ci-facts:end -->

Only one branch can be the deploy source; two would race for the same
Pages deployment.

1. **Lint and test**: `ruff check`, `ruff format --check`, `pytest`,
   `run365-schema --check frontend/schema.graphql`, and `run365-ci-docs
   --check` on this file, `README.md` and `docs/architecture.md`.
2. **Frontend**: `npm ci`, ESLint, codegen + `tsc`, Vitest, Vite build in
   both data modes.
3. **Build dashboard (static mode)**: `run365-export --skip-db --static-dir
   frontend/public/data`, then `npm run build:static` with
   `VITE_BASE_PATH=/<repo>/`. `dist/index.html` is copied to `404.html` so
   a deep link such as `/activity/7264441638` is served by Pages and picked
   up by the router.
4. **Deploy to GitHub Pages**.

The repository's `github-pages` environment must allow deployments from
the deploy branch (Settings, Environments, Deployment branches). That rule
lives in the repository settings, not in the workflow file, so no check in
this repository can see it: `run365-ci-docs` keeps the documents honest,
but the environment rule has to be updated by hand in the same pass
whenever the deploying branch in `pages.yml` changes — otherwise the deploy
job is rejected at the environment gate even though the workflow itself ran.

## Switching the deploy source to another branch

Moving the deploy source means changing six things in four places — two of
them outside the repository, where a checkout can neither see nor verify
them. Missing one leaves the setup half-switched, and the failure is
usually silent. Do all six together.

In the repository:

1. `.github/workflows/pages.yml` — the `push` and `pull_request` branch
   lists. Keep both branches if both should stay gated; drop `develop` only
   once nothing is being worked on there.
2. `.github/workflows/pages.yml` — the `if:` on the `build` and `deploy`
   jobs, plus the `concurrency` `group` and `cancel-in-progress`
   expressions. All four name the current deploy branch; each has to name
   the new one, or deploys stop happening and non-deploying runs start
   sharing the deploy group. `run365-ci-docs` refuses to run while these
   disagree with each other, so a half-edited workflow fails CI rather
   than deploying from nowhere.
3. `.github/workflows/tag-release.yml` — the `ref` input default. Nothing
   checks this one; `run365-ci-docs` only reads `pages.yml`.
4. `README.md` — the CI badge's `?branch=` query, the Vercel
   production-branch sentence, and the "Versioning and branches" table.
   The badge query is checked against the workflow by `run365-ci-docs`;
   the other two are prose about settings the workflow does not hold.

Outside the repository, so neither readable nor changeable from a
checkout:

5. **GitHub `github-pages` environment** (Settings, Environments,
   `github-pages`, Deployment branches) — allow the new branch. Until this
   is done CI goes green and the deploy job is still rejected at the
   environment gate.
6. **Vercel Production Branch** (project Settings, Git) — API-mode
   deployments follow this setting, not the repository. Until this is done
   Pages and Vercel serve different commits.

Then run `run365-ci-docs --write README.md docs/architecture.md
docs/deployment.md` to regenerate the branch facts in the three documents,
and re-read the prose around them: the intro here, the trigger table, the
Vercel section and this checklist all describe settings that live outside
`pages.yml`, so the generator cannot correct them.

## Tagging a release

`.github/workflows/tag-release.yml` is a manual workflow (Actions, "Tag
release", Run workflow). Given a tag name, a branch or commit and a message
it creates the annotated tag and, by default, a GitHub Release whose notes
are the matching `## [x.y.z]` section of `docs/CHANGELOG.md`. It runs with
the repository token, so no secrets are involved, and it refuses to
overwrite an existing tag.

## Running it yourself

```bash
pip install -e ".[dev]"
run365-export                                   # SQLite + static JSON
flask --app run365days.api.app:create_app run   # http://127.0.0.1:5000/api/graphql
cd frontend && npm install && npm run dev       # http://localhost:5173 (api mode)
```

For static mode locally: `run365-export --static-dir frontend/public/data`
then `VITE_DATA_MODE=static npm run dev`.
