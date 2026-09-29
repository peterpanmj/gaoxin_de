# Saleor Analytics: Implementation Plan and Delivery Status

**Status:** Core interview-demo scope implemented. This document records the
delivered design, its verification evidence, and the remaining production work.
It is not a time-boxed checklist. The implementation was planned for roughly
8-14 hours of AI-assisted engineering; presentation preparation and the final
write-up are separate work.

**Stack:** Saleor-compatible GraphQL extraction, local JSONL mock data, Python,
Click, DuckDB, dbt, Plotly Dash, Airflow, Docker Compose, uv, and GitHub Actions.

## 1. Delivered scope

| Area | Status | Delivered behavior | Main evidence |
|---|---|---|---|
| Runtime environment | Implemented | Installable Python package, locked dependencies, local configuration, Docker Compose Airflow runtime. | `pyproject.toml`, `uv.lock`, `config/`, `infra/airflow/` |
| Source ingestion and backfill | Implemented | Saleor GraphQL full/incremental extraction, explicit UTC date-range backfill by `updatedAt`, and local Saleor-shaped JSONL ingestion. Backfills are immutable snapshots and never advance the incremental watermark. | `src/saleor_analytics/extract.py`, `cli.py`, `tests/test_reliability.py` |
| Synthetic scenarios | Implemented | Deterministic `baseline`, `update`, `duplicate`, and `invalid` local JSONL scenarios; no mutation of Saleor. | `src/saleor_analytics/mock.py` |
| Bronze and quarantine | Implemented | Raw and accepted JSONL, manifests, checksums, rejection evidence, and replayable snapshots. | `pipeline.py`, runtime `bronze/` and `quarantine/` |
| Contract and normalization | Implemented | Controlled v2 order contract, UTC timestamps, decimal money, canonical line items, duplicate collapse, and conflict failure. | `records.py` |
| DuckDB and dbt | Implemented | Candidate DuckDB warehouse, Silver current-state tables, Gold daily order/product metrics, dbt tests. | `analytics/`, `pipeline.py` |
| Publication and lineage | Implemented | Immutable release directories and atomic `warehouse/current.json` pointer after dbt validation. | `pipeline.py` |
| Orchestration | Implemented | Daily Airflow DAG with retries, manual mock controls, quality-gated publication, and freshness monitoring. | `dags/saleor_analytics.py` |
| Dashboard | Implemented | Currency/channel/date filters, KPIs, Gold trend and product charts, release/quality status, single-day markers. | `dashboard.py` |
| Artifact export | Implemented | CLI export of a release and latest snapshot by default; `--all-snapshots` exports full source lineage. | `export-artifacts` command |
| CI | Implemented | PR/main formatting, linting, tests, package build, and package-artifact retention in GitHub Actions. | `.github/workflows/` |
| CD example | Illustrative | Manual production-environment promotion job downloads a tested package; no deployment target is configured. | `.github/workflows/` |

## 2. Current end-to-end flow

```text
Saleor GraphQL or mock JSONL
  → raw Bronze capture
  → Python contract, normalization, deduplication, quarantine
  → accepted Bronze snapshot and manifest
  → isolated DuckDB candidate
  → dbt Silver and Gold models plus data tests
  → immutable validated release
  → current.json pointer
  → Dash dashboard and artifact export
```

Airflow invokes the same CLI boundaries in this order:

```text
prepare → extract_and_validate → stage → transform_and_test → publish → monitor
```

Publication happens only after dbt validation succeeds. A failed run retains the
previous release and its checkpoint for dashboard consumers.

For a historical repair, `extract-saleor --mode backfill --start UTC --end UTC`
reads the explicit start-inclusive/end-exclusive `[start, end)` `updatedAt`
window into a new Bronze snapshot. Operators then build a new release through
the ordinary dbt and publication gates. This keeps historical reprocessing
separate from incremental watermark advancement.

## 3. Validation and acceptance evidence

The validation strategy is deliberately layered:

| Layer | Checks |
|---|---|
| Python contract | Required fields, object shapes, UTC timestamps, finite money, supported currencies/statuses, positive quantities, and unique line IDs. |
| Ingestion quality gate | Exact duplicates collapse; same-version differing payloads fail; invalid records quarantine; reject rate must remain within the configured limit. |
| Backfill guardrails | Both UTC bounds are required; start must precede end; `[start, end)` filtering is enforced locally because the Saleor API upper filter is inclusive; backfills do not advance the incremental watermark. |
| Artifact integrity | Contract version plus SHA-256 verification of raw and accepted snapshots. |
| dbt | Keys, relationships, accepted currencies, line/order contracts, conflicting versions, and Gold-to-Silver metric reconciliation. |
| Release gate | A candidate requires successful dbt results and matching checksums before publication. |
| Operations | `status --max-age-hours 24` reports release/extraction freshness; Airflow records task outcomes and retries. |
| CI | `ruff format --check`, `ruff check`, pytest, package build, and artifact upload for pull requests and `main`. |

Run the local checks with:

```bash
uv sync --frozen
uv run ruff format --check src tests dags
uv run ruff check src tests dags
uv run pytest -q
uv build
```

## 4. Presentation workflow

The presentation environment uses an already published release in the Airflow
`analytics-data` Docker volume. From Git Bash at the repository root:

```bash
bash scripts/start_presentation_demo.sh
```

The launcher starts Airflow, repairs a stale Airflow webserver PID marker only
when it is stale, waits for the Airflow UI, exports the active release if needed,
and starts Dash from the exported release. It does not configure a proxy, pull
images, generate data, or overwrite source history.

Open:

```text
Airflow: http://localhost:8081
Dash:    http://localhost:8051
```

See [CLI_USER_GUIDE.md](docs/CLI_USER_GUIDE.md),
[MODERN_DE_DEMO.md](docs/MODERN_DE_DEMO.md), and
[DEMO_DESIGN.md](DEMO_DESIGN.md) for the detailed walkthrough, runtime artifact
paths, and business definitions.

## 5. Deliberate boundaries and next work

| Trigger or need | Next implementation step |
|---|---|
| More source volume | Move retained source data to object storage and process incremental partitions or Parquet files. |
| Near-real-time updates or deletes | Use durable event/log CDC with tombstones; `updatedAt` polling cannot capture hard deletes or every intermediate mutation. |
| Historical source reconstruction | Add source-native snapshots or CDC/event retention. Date-range backfill re-reads the current API by `updatedAt`; it is not an as-of query and cannot restore hard-deleted data. |
| Multiple consumers or large analytical workloads | Move the same dbt models to a managed warehouse or governed lakehouse. |
| Full artifact reproducibility by default | Export all snapshots by default or make the export an atomic, locked copy operation. The current default export is an inspection bundle containing the latest snapshot. |
| Automated deployment | Build and publish a versioned container image, configure a deployment target, and protect promotion with GitHub environment reviewers. |
| Production security | Add IAM, secret management, encryption, retention controls, access auditing, and alert delivery. |
| Stronger data observability | Add source completeness/reconciliation feeds, volume anomaly detection, external alerts, and an enforced end-to-end freshness SLO. |

## 6. Requirement traceability

| Assessment area | Current repository evidence |
|---|---|
| A. Architecture and design | [DEMO_DESIGN.md](DEMO_DESIGN.md), `analytics/`, Bronze/Silver/Gold/release workflow. |
| B. Python programming | `src/saleor_analytics/`, Click CLI, contract normalization, extraction, and tests. |
| C. Quality and testing | Python contract checks, dbt models/tests, quarantine, integrity checks, and pytest. |
| D. Orchestration and operations | `dags/saleor_analytics.py`, `infra/airflow/compose.yml`, freshness status command, and presentation launcher. |
| E. CI/CD | `.github/workflows/`, locked dependency installation, automated validation, package build, and illustrative promotion job. |
| F. Documentation | README, design, CLI guide, operations guide, modern demo guide, verification report, and this document. |
