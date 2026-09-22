# Saleor Analytics Demo

A Principal Data Engineer interview project using Saleor as a synthetic commerce source, Python and Click for ingestion, dbt and DuckDB for analytical modeling, and Plotly Dash for reporting.

## What this demonstrates

This is a synthetic, local commerce-analytics pipeline designed for the Principal Data Engineer take-home. It uses a real Saleor GraphQL source, but no real customer data. The required Python component is the `saleor-analytics` Click CLI. It extracts structured JSON, applies a versioned order contract, quarantines invalid records, writes immutable Bronze snapshots, builds a DuckDB candidate, runs dbt transformations and tests, and only then atomically publishes Gold tables for a Plotly Dash dashboard.

## Project documentation

- [Design and assessment requirements](DEMO_DESIGN.md)
- [Saleor setup](SALEOR_SETUP.md)
- [Operational design, governance and cost](docs/OPERATIONS.md)

## Clone

```bash
git clone --recurse-submodules https://github.com/peterpanmj/gaoxin_de.git
cd gaoxin_de
```

For an existing clone:

```bash
git submodule update --init --recursive
```

The official Saleor Platform repository is pinned as a submodule under `infra/saleor-platform`. Its upstream development configuration is intended only for local synthetic-data use. Runtime data and local credentials must not be committed.

All project documentation, code comments and user-facing text are in English.

## Local run

Prerequisites: Docker Desktop with at least 4 GB assigned, Python 3.12, and `uv`.

```powershell
uv sync
docker compose -f infra/saleor-platform/docker-compose.yml pull
docker compose -f infra/saleor-platform/docker-compose.yml run --rm api python3 manage.py migrate
docker compose -f infra/saleor-platform/docker-compose.yml run --rm api python3 manage.py populatedb --createsuperuser
docker compose -f infra/saleor-platform/docker-compose.yml up -d

$env:SALEOR_EMAIL = "admin@example.com"
$env:SALEOR_PASSWORD = "admin"
uv run saleor-analytics extract-saleor --snapshot-id saleor-initial-001
uv run saleor-analytics build-warehouse --release-id release-initial-001
uv run saleor-analytics dashboard
```

The dashboard is served at `http://127.0.0.1:8050`. Saleor GraphQL is at `http://localhost:8000/graphql/`, and its local dashboard is at `http://localhost:9000`.

For a file-based contract demonstration, use `uv run saleor-analytics ingest-file INPUT.jsonl --snapshot-id example-001`. It expects the same Saleor order JSON shape selected by the extractor. Invalid rows remain in `var/quarantine/<snapshot-id>/`; a snapshot whose reject rate exceeds the configuration threshold fails before warehouse publication.

## Repository map

- `src/saleor_analytics/`: typed, testable Python extraction, validation, release and dashboard modules.
- `analytics/`: dbt staging, Silver current-order and Gold aggregate models plus data tests.
- `dags/`: Airflow workflow definition.
- `infra/saleor-platform/`: pinned official Saleor submodule used only for synthetic local data.
- `.gitlab-ci.yml`: merge-request validation and protected default-branch promotion skeleton.

## Design choices and trade-offs

Bronze preserves raw JSONL and a checksum-bearing manifest. Silver selects one current version per order, with deterministic tie-breaking. Gold has daily order and product metrics that make the dashboard cheap to query. This is a batch design with a daily target freshness. It is intentionally small: DuckDB and full snapshot replay optimize interview clarity and local reproducibility. `docs/OPERATIONS.md` describes the incremental object-storage/warehouse evolution.

The contract requires identifiers, timezone-aware timestamps, positive line quantities, internally consistent money/currency, and unique line IDs. Python handles row-level reject/quarantine behavior; dbt enforces uniqueness, nullability, allowed currencies, relationships, and aggregate reconciliation. Failed Python validation or dbt tests blocks publication.

No secrets or runtime data are committed. In production, use service-account credentials injected from a secret manager, source TLS, encryption at rest, role-scoped Gold access, and audited releases. The extractor deliberately omits PII fields from its selection set.

## Validation

```powershell
uv run ruff check src tests
uv run pytest -q
uv run dbt build --project-dir analytics --profiles-dir analytics --target candidate
```

The dbt command needs `ANALYTICS_DATABASE_PATH` pointing to a candidate database; `saleor-analytics build-warehouse` sets it automatically.
