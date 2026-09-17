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

### Seeing budget refusals in the log

A request refused for asking past one of the API's budgets is logged at
`INFO` to the `strawberry.execution` logger, deliberately below the `WARNING`
that `logging` applies by default (CUI-0029). A deployment that configures no
logging — which is what Vercel runs — therefore drops those records, so the
Logs tab shows nothing when a client is refused. That is the intended default:
a refusal is the contract working, not a fault, and the client is told why in
the response.

An operator who does want to see them — to size the budgets, or to spot a
client hammering this unauthenticated endpoint — opts in by lowering that one
logger, before the app is built:

```python
import logging

logging.getLogger("strawberry.execution").setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)   # only if nothing configures handlers
```

Every refusal then appears, with the same message the client got, in the same
stream as the genuine errors it sits beside. Real faults are unaffected by
this: they are logged at `ERROR` with their traceback whether or not the level
is lowered.

## GitHub Pages

`.github/workflows/pages.yml` is the source of truth for this deployment:
the branches it runs on, its jobs and each job's steps all live in that
file. This section describes only the shape, which is what a reader needs
before opening it — a prose copy of the branch lists and job order has
gone stale twice already, so it is deliberately not kept here.

Lint, tests and the frontend checks run on every branch the workflow gates,
so a pull request into any of them is covered. Building and deploying the
site is restricted to a single branch — `develop` today — by a `github.ref`
test in the workflow, so a gated branch that is not the deploy source runs
the checks and stops there. A manual `workflow_dispatch` follows the
same restriction: it re-deploys when run on the deploy branch, and is
CI-only anywhere else. Only one branch can hold that role; two would race
for the same Pages deployment.

The site build copies `dist/index.html` to `404.html`, so a deep link such
as `/activity/7264441638` is served by Pages and picked up by the router.
It also sets `VITE_BASE_PATH=/<repo>/`, which `frontend/vite.config.ts`
reads into Vite's `base`, so the asset URLs resolve from the repository
subpath Pages serves the site on. Vercel serves from the domain root and
leaves it unset, where the config falls back to `/`. Both are shape facts
rather than job steps, which is why they are written down here.

Two dependency-audit gates run, one per language side, and both are hard
gates: a failing audit stops the workflow like any other check.

The frontend gate audits the whole npm tree, devDependencies included. It
carried `--omit=dev` until the development tree's advisories were cleared by
a bump of the codegen and vitest roots, and widening it to the full tree was
the stated end state of that narrower threshold rather than a regression.

The Python gate upgrades `pip` and `setuptools` and then runs `pip-audit`
over the environment the job installed: the declared dependencies, the
`[dev]` extras and their transitives, and `pip-audit`'s own closure
(CacheControl, cyclonedx-python-lib, requests, rich and the rest), which
installing the auditor into that same environment pulls in. That last part is
the price of auditing an environment rather than a lockfile, and it is worth
stating in the policy: a red here can in principle come from a package only
`pip-audit` itself needs. It has not so far. That upgrade is a fix and not a
suppression -- the advisories it removes all carry fix versions -- which is
why an `--ignore-vuln` allowlist was rejected in its place; a list of
exceptions drifts, and the gate then reports on the list rather than on the
tree. It also means the runner's own `pip` version is in scope, so a future
pip advisory published ahead of its fix would turn the gate red on something
no pull request here can cause. That is deliberate. `pip-audit` always skips
`run365days` itself, which is installed from the checkout and is not on PyPI,
so the gate does not run with `--strict`.

Both audits run last in their jobs. Either is expected to go red on an
upstream advisory published since the last run rather than on anything a pull
request did, and running it from the front of a job would bury the checks the
author can act on -- which is not hypothetical for the frontend one: it did,
and lint, typecheck, unit tests and both builds never ran (S-097). Which
threshold is in force is a policy fact rather than a job step, which is why it
is written down here; the commands themselves, and the reasoning behind each
threshold, are in `pages.yml` beside the steps.

Both gates sit upstream of deployment. They run in `lint-test` and `frontend`,
`build` needs both, and `deploy` needs `build`, so on `develop` -- the only
branch that deploys, and one that deploys on every push -- an advisory
published upstream and unrelated to anything in this repository can hold every
deployment, a hotfix included. That is the accepted price of hard gates rather
than advisory ones, and it is stated here so the trade reads as a decision
already taken rather than one discovered during an incident. Decoupling the
audits from the deployment path is open as CUI-0052; `continue-on-error` is
not the answer, since it would leave the gate on the page while removing its
only effect.

The repository's `github-pages` environment must allow deployments from
`develop` (Settings, Environments, Deployment branches). That rule lives
in the repository settings, not in the workflow file, so it has to be
updated in the same pass whenever the deploying branch in `pages.yml`
changes — otherwise the deploy job is rejected at the environment gate
even though the workflow itself ran.

## Switching the deploy source from `develop` to `master`

`develop` is the deploy source today. Moving it to `master` means changing
six things in four places — two of them outside the repository, where a
checkout can neither see nor verify them. Missing one leaves the setup
half-switched, and the failure is usually silent. Do all six together.

In the repository:

1. `.github/workflows/pages.yml` — the `push` and `pull_request` branch
   lists. Keep both branches if both should stay gated; drop `develop` only
   once nothing is being worked on there.
2. `.github/workflows/pages.yml` — the `if:` on the `build` and `deploy`
   jobs, plus the `concurrency` `group` and `cancel-in-progress`
   expressions. All four test `refs/heads/develop`; each has to name the
   new branch, or deploys stop happening and non-deploying runs start
   sharing the deploy group.
3. `.github/workflows/tag-release.yml` — the `ref` input default.
4. `README.md` — the CI badge's `?branch=` query, the "Continuous
   integration and deployment" section, the Vercel production-branch
   sentence, and the "Versioning and branches" table with the paragraph
   under it.

Outside the repository, so neither readable nor changeable from a
checkout:

5. **GitHub `github-pages` environment** (Settings, Environments,
   `github-pages`, Deployment branches) — allow the new branch. Until this
   is done CI goes green and the deploy job is still rejected at the
   environment gate.
6. **Vercel Production Branch** (project Settings, Git) — API-mode
   deployments follow this setting, not the repository. Until this is done
   Pages and Vercel serve different commits.

Then update this file: the intro, the trigger table, the Vercel section,
the GitHub Pages section and this checklist.

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
