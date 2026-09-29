# New PC handoff

This repository is a Principal Data Engineer interview demo. It accepts
Saleor-shaped JSONL or Saleor GraphQL orders, validates and stores raw input in
Bronze, builds Silver and Gold models in DuckDB with dbt, publishes a validated
release, and serves it with Plotly Dash. Airflow provides the daily workflow.
The project and this handoff are in English.

## Repository state

- Remote: `https://github.com/peterpanmj/gaoxin_de.git` (private; sign in to
  GitHub on the new PC).
- PR #6, which added Git Bash commands to the guides, is merged into `main`.
- The source of truth for continuing on another PC is the remote `main` branch
  after this handoff change is merged. Check `git status` before making changes.
- Start with [README.md](README.md), [DEMO_DESIGN.md](DEMO_DESIGN.md),
  [Git Bash CLI guide](docs/CLI_USER_GUIDE.md), and
  [Windows CLI guide](docs/CLI_USER_GUIDE_WINDOWS.md). The
  [modern DE demo guide](docs/MODERN_DE_DEMO.md) covers the Airflow scenarios.

## Set up a fresh Windows PC

Install Git with Git Bash, Python 3.12, `uv`, and Docker Desktop with Linux
containers. Docker is needed for Airflow and Saleor, but not for the local
mock-to-Dash path. Authenticate GitHub before cloning the private repository.
In Git Bash:

```bash
git clone --recurse-submodules https://github.com/peterpanmj/gaoxin_de.git
cd gaoxin_de
git status
# First setup only; this downloads the versions pinned in uv.lock.
uv sync --frozen
uv run saleor-analytics doctor
```

Do not repeat `uv sync --frozen` before every demo once `.venv` is ready. On a
slow or unavailable connection, run `uv sync --frozen --offline` to use only
the existing cache; it fails quickly when a required package is missing. A
second simultaneous sync waits for uv's environment/cache lock, so cancel the
duplicate rather than waiting for both.

The submodule is the pinned upstream Saleor development stack. If the clone
already exists, use `git pull --ff-only` on `main` and
`git submodule update --init --recursive`. Run project commands from the
repository root. Use `uv run saleor-analytics --help` and a command's `--help`
for CLI parameters.

## Quick repeatable demo without Docker

These commands create synthetic Saleor-shaped JSONL, ingest that same file,
build a validated release, and open Dash. They do not write to Saleor.

```bash
export ANALYTICS_ROOT="$(pwd -W)/var/handoff-$(date +%Y%m%d-%H%M%S)"
uv run saleor-analytics mock-data "$ANALYTICS_ROOT/base.jsonl" --count 20
uv run saleor-analytics ingest-file "$ANALYTICS_ROOT/base.jsonl" --snapshot-id base
uv run saleor-analytics build-warehouse --release-id base
uv run saleor-analytics status
uv run saleor-analytics dashboard
```

Open `http://localhost:8050`. The default dataset should show 20 orders and
USD 400 gross order value. Press Ctrl+C in Git Bash to stop the dashboard.
The local output is under the chosen `var/handoff-*` directory; its
`warehouse/current.json` points to the published release. See the
[CLI guide](docs/CLI_USER_GUIDE.md) for Bronze, quarantine, release, and dbt
evidence paths.

## Airflow demonstration

From the repository root, start Docker Desktop and run:

```bash
docker compose -f infra/airflow/compose.yml build
docker compose -f infra/airflow/compose.yml up -d
docker compose -f infra/airflow/compose.yml ps
```

Open `http://localhost:8081`, sign in with the credentials produced locally by
Airflow standalone startup, and open `saleor_analytics_daily`. Trigger a manual
run with this JSON to make a baseline without requiring a live Saleor service:

```json
{"generate_mock": true, "scenario": "baseline", "count": 20}
```

The DAG runs `prepare -> extract_and_validate -> stage -> transform_and_test
-> publish -> monitor`. Its scheduled default uses Saleor extraction, so set
up Saleor and its runtime credentials before relying on scheduled runs.
The [Saleor setup](SALEOR_SETUP.md) and [demo guide](docs/MODERN_DE_DEMO.md)
cover the API path, incremental polling, failure scenarios, and proxy settings.

After one successful Airflow publication, the Git Bash presentation launcher
can start both web UIs and export the active release for Dash:

```bash
bash scripts/start_presentation_demo.sh
```

It opens Airflow at `http://localhost:8081` and Dash at
`http://localhost:8051`. It needs a published release in the Airflow data
volume first. Stop Airflow without deleting its volumes with
`docker compose -f infra/airflow/compose.yml stop`.

## What Git does and does not move

Git transfers code, documentation, DAGs, dbt models, tests, and the lockfile.
It does **not** transfer ignored `var/`, `artifacts/`, `.venv/`, local `.env*`
files, credentials, presentation `.pptx` files, or Docker named volumes. The
Airflow `analytics-data` volume is mounted at `/opt/data` in the container;
the host `artifacts/` folder is mounted at `/opt/artifacts`. A fresh PC starts
with no prior snapshots or warehouse release. Run the baseline DAG there
before using the presentation launcher. Do not assume that an exported review
bundle restores the Airflow volume: `export-artifacts` copies evidence for
inspection, not a complete runtime backup.

Never commit local credentials, proxy settings, runtime data, or prompt logs.
For further changes, branch from up-to-date `main`, run the checks in
[README.md](README.md#validation-and-delivery), and open a pull request.

## Interview boundaries

The implemented incremental path polls Saleor by `updatedAt` with overlap;
it is not database log CDC and cannot reliably detect hard deletes. dbt
rebuilds current Silver and Gold models from accepted history at this demo
scale. Silver and Gold are DuckDB tables; a Parquet analytical export and
Spark execution are future work. The GitHub Actions pipeline validates the
project; its manual production job illustrates promotion and is not an
operational deployment. See [assessment review](docs/ASSESSMENT_REVIEW.md)
for the requirement mapping and remaining limits.
