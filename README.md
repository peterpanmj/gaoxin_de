# Saleor Analytics Demo

A Principal Data Engineer interview project using Saleor as a synthetic commerce source, Python and Click for ingestion, dbt and DuckDB for analytical modeling, and Plotly Dash for reporting.

## What this demonstrates

This is a synthetic, local commerce-analytics pipeline designed for the Principal Data Engineer take-home. It uses a real Saleor GraphQL source, but no real customer data. The required Python component is the `saleor-analytics` Click CLI. It extracts structured JSON, validates and normalizes orders, quarantines recognized invalid records, and creates Bronze snapshots. The ordinary `build-warehouse` path builds a DuckDB candidate, runs dbt models/tests, then replaces the serving database. The separate `publish-candidate` command does not enforce those checks.

**Assessment status: partially met, not yet submission-ready.** The [A-F review](docs/ASSESSMENT_REVIEW.md) maps implementation evidence to the original brief and identifies outstanding defects. In particular, Python deduplication is missing, the illustrative Airflow DAG lacks an extraction-to-build dependency, and the dashboard combines currencies. The previous happy-path checks do not prove these requirements are complete.

## Project documentation

- [Design and assessment requirements](DEMO_DESIGN.md)
- [Saleor setup](SALEOR_SETUP.md)
- [Operational design, governance and cost](docs/OPERATIONS.md)
- [Assessment coverage, CLI mapping and repair priorities](docs/ASSESSMENT_REVIEW.md)

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

Prerequisites: Docker Desktop, Python 3.12, and `uv`. The complete stack has not been acceptance-tested under a 4 GB memory limit. Run the following from the repository root; use new snapshot/release IDs on subsequent runs. Run `populatedb` only for the initial synthetic bootstrap.

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
- `.gitlab-ci.yml`: illustrative validation/package/manual-promotion skeleton. It is not executed by this GitHub repository; GitHub Actions and explicit MR/PR coverage remain outstanding.

## Design choices and trade-offs

Bronze preserves raw JSONL and a checksum-bearing manifest. Silver selects one current version per order, with deterministic tie-breaking. Gold has daily order and product metrics that make the dashboard cheap to query. This is a batch design with a daily target freshness. It is intentionally small: DuckDB and full snapshot replay optimize interview clarity and local reproducibility. `docs/OPERATIONS.md` describes the incremental object-storage/warehouse evolution.

Validation checks identifiers, timezone-aware timestamps, selected quantity/money types, currency consistency, and unique line IDs within each order. Some malformed nested types and non-finite decimals escape row quarantine; see the review. dbt checks uniqueness, nullability, allowed currencies, relationships, and daily order aggregate reconciliation. A dbt failure blocks publication on the `build-warehouse` path, but the direct publication command bypasses that gate.

Python currently retains duplicate orders. dbt chooses the current order by `updated_at DESC, snapshot_id DESC, payload_json DESC`; it does not reject conflicting equal-time versions. Repeated orders within a snapshot can duplicate line rows and fail dbt uniqueness tests. Full accepted-history replay differs from revalidating raw files under a new contract.

Runtime data and local credential files are ignored by Git. The credentials shown above are Saleor's public local-development defaults; never use them for a deployed service. In production, use service-account credentials injected from a secret manager, source TLS, encryption at rest, role-scoped Gold access, and audited releases. The extractor omits customer identity/address fields; arbitrary file inputs are preserved verbatim, so only ingest synthetic inputs in this demo.

## CLI requirement mapping

Each command's `--help` includes its assessment mapping, inputs/outputs and limitations:

- `doctor`: configuration validation and execution setup (B/F); no service health check.
- `ingest-file`: required structured-file Python path, normalization, quarantine and counts (A/B/C/F).
- `extract-saleor`: paginated API-to-file ingestion with bounded read retries (A/B/D).
- `build-warehouse`: accepted-history replay, dbt current-state/Gold models and a tested publication path (A/B/C/D).
- `publish-candidate`: unchecked file replacement; not a quality or production approval gate (A/D, with C/E gaps).
- `dashboard`: Gold consumption (A/F); current mixed-currency totals and unfiltered product ranking require correction.

For example, `uv run saleor-analytics build-warehouse --help` explains the current deduplication rule and what `--no-publish` does. See the [review walkthrough](docs/ASSESSMENT_REVIEW.md#cli-walkthrough-and-analytical-semantics) for analytical grains and exact command order.

## Observability and next steps

Snapshot manifests record checksums, accepted/rejected counts and validation status; Silver rows retain snapshot IDs. Freshness alerts, per-release test/source inventories and exported lineage are proposed, not deployed. The nominal design target is daily processing with a 06:30 UTC freshness objective; it is not currently monitored. Prioritize publication safety, DAG ordering, currency-correct reporting, validation/deduplication and failure/retry tests, then complete the agreed synthetic scenarios and CI/runtime setup. Production security, retention and incremental-storage evolution are described in [operations](docs/OPERATIONS.md).

## Validation

```powershell
uv run ruff check src tests
uv run pytest -q
uv run dbt build --project-dir analytics --profiles-dir analytics --target candidate
```

The dbt command needs `ANALYTICS_DATABASE_PATH` pointing to a candidate database; `saleor-analytics build-warehouse` sets it automatically.
