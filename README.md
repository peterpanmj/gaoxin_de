# Saleor Analytics Demo

A Principal Data Engineer interview demo: Saleor GraphQL, a Python Click CLI,
PySpark, Parquet, Airflow and Plotly Dash. All data is synthetic and all project
content is in English.

## Start here

- [New PC handoff](HANDOFF.md): fresh setup, repeatable demo, Airflow run, and
  runtime data that Git does not transfer.
- [Runnable modern DE demonstration](docs/MODERN_DE_DEMO.md): replay, updates,
  incremental extraction, quarantine and Airflow.
- [CLI user guide for Git Bash](docs/CLI_USER_GUIDE.md): environment setup, commands, and mock ingestion use cases.
- [CLI user guide for PowerShell](docs/CLI_USER_GUIDE_WINDOWS.md): the same workflow with Windows commands.
- [Historical assessment A-F review](docs/ASSESSMENT_REVIEW.md): September 2026 audit of the earlier DuckDB/dbt branch.
- [Operations](docs/OPERATIONS.md) and [Saleor setup](SALEOR_SETUP.md).
- [Original design](DEMO_DESIGN.md) and [implementation plan](IMPLEMENTATION_PLAN.md).

## Quick start without Docker

Install Python 3.12, Java 17 or later, and uv, then run from the repository root in either shell.
`uv sync --frozen` is required only for the first setup or after `uv.lock`
changes; it may download packages. If the cached `.venv` already works, skip
that step. Use `uv sync --frozen --offline` to check the cache without network
access; it fails quickly if a dependency is missing.

PowerShell:

```powershell
# First setup only.
uv sync --frozen
$demoSession = [guid]::NewGuid().ToString('N')
$env:ANALYTICS_ROOT = Join-Path (Get-Location) "var/demo-$demoSession"
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/base.jsonl" --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/base.jsonl" --snapshot-id base
uv run saleor-analytics build-warehouse --release-id base
uv run saleor-analytics status
uv run saleor-analytics dashboard
```

Git Bash:

```bash
# First setup only.
uv sync --frozen
export ANALYTICS_ROOT="$(pwd -W)/var/demo-$(date +%Y%m%d-%H%M%S)"
uv run saleor-analytics mock-data "$ANALYTICS_ROOT/base.jsonl" --count 20
uv run saleor-analytics ingest-file "$ANALYTICS_ROOT/base.jsonl" --snapshot-id base
uv run saleor-analytics build-warehouse --release-id base
uv run saleor-analytics status
uv run saleor-analytics dashboard
```

Open http://localhost:8050. Expected: 20 orders, USD400 gross order value.
Stop on an unexpected nonzero exit code. The mock generator creates local
Saleor-shaped JSONL; it does not modify Saleor. For a real API source and Airflow,
follow the linked guides. Clone with `--recurse-submodules` for the pinned
upstream Saleor development stack.

## Architecture and guarantees

```mermaid
flowchart LR
    A[Saleor API or mock JSONL] --> B[Python contract and deduplication]
    B --> C[Bronze raw / accepted / manifest]
    B --> Q[Quarantine]
    C --> D[Isolated Spark staging in Parquet]
    D --> E[PySpark Silver and Gold plus 11 quality checks]
    E --> F[Validated immutable release]
    F --> G[Atomic release and watermark pointer]
    G --> H[Currency-filtered Dash reports]
```

Same-input retries reuse snapshots; exact duplicates collapse; conflicting
versions fail. Invalid rows are quarantined, with a configurable reject threshold.
Current-state models select the complete latest order and line set. Both build
and direct publication require checksum-bound Spark evidence. Failed validation
or publication preserves the previously served release and source checkpoint.

Incremental API polling uses `updatedAt`, persisted bounds and five-minute
overlap. This is not log CDC: hard deletes and every intermediate mutation are
not captured. Spark rebuilds from accepted history at this scale. Staging,
Silver, and Gold are Parquet datasets in each immutable release.
An existing DuckDB release pointer can be superseded by building a new Spark
release from its retained Bronze snapshots; use a new release ID. Until then,
the new dashboard will not serve the legacy release.

### Parquet datasets

Each validated release stores its analytical datasets under
`releases/<release-id>/dataset/`. PySpark writes them; Dash reads the small
Gold outputs with PyArrow.

| Layer | Parquet path in `dataset/` | Grain / purpose |
|---|---|---|
| Staging | `staging/order_versions/` | Approved order versions from Bronze. |
| Silver | `silver/orders/` | One current, trusted version per Saleor order. |
| Silver | `silver/order_lines/` | One current line per current order version. |
| Gold | `gold/daily_order_metrics/` | Daily order count, gross value, and average order value by channel and currency. |
| Gold | `gold/daily_product_metrics/` | Daily units and gross value by product, channel, and currency. |

Bronze evidence is file-based: raw and accepted
JSONL plus a manifest are stored under `bronze/<snapshot-id>/`.

For a quick inspection, read a Gold directory with
`pandas.read_parquet("<analytics-root>/releases/<release-id>/dataset/gold/daily_order_metrics")`.

Airflow orders preparation, ingestion, Spark staging, transformation/checks, publication and monitoring.
Mock generation is optional and restricted to manual runs. Dash reloads one
release every 30 seconds, with matching currency/channel/date filters for KPIs
and charts. Gross order value excludes draft/canceled orders; it is not recognized
revenue. No conversion or cross-currency total is implied.

## Validation and delivery

Run these commands from either PowerShell or Git Bash:

```bash
uv run ruff format --check src tests dags
uv run ruff check src tests dags
uv run pytest -q
uv build
```

Tests include real Spark builds and publication-failure recovery. GitHub Actions
validates pull requests and main; GitLab CI is an alternative example. The manual
production environment job is a promotion illustration, not a deployed service;
configure repository protection/reviewers separately. Runtime data, local secrets,
and AI configuration/prompt logs are ignored by Git.

The demo uses local filesystem locks, a single-node Spark session and environment
secrets. HA, cloud IAM, external alert delivery, retention enforcement, source
completeness guarantees and a 4 GB full-stack acceptance test remain outside
implemented scope. Use a fresh data root for contract v2; legacy snapshots are
not silently upgraded.
