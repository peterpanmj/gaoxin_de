# Assessment coverage: PySpark branch

This review describes `codex/pyspark-analytics` against sections A-F of
`Principal_Data_Engineer_Candidate_Take_Home.md`. The brief explicitly permits
PySpark for the required Python component and custom validation for data tests;
dbt is optional. The agreed flexible implementation timeframe supersedes the
brief's suggested completion time.

## Current implementation

PySpark replaces the earlier DuckDB/dbt analytical path. Python captures and
normalizes source records, and PySpark stages accepted history, selects current
orders and their complete line sets, builds Silver/Gold Parquet datasets, and
runs 11 data quality checks. Dash reads published Gold Parquet through
pandas/PyArrow. There is no DuckDB database or dbt execution in this branch.

The CLI retains names such as `build-warehouse` for continuity. Here, warehouse
means the logical analytical product stored in release directories, not a
separate database service. See [the design](../DEMO_DESIGN.md) and
[implementation plan](../IMPLEMENTATION_PLAN.md).

## A-F requirement mapping

| Requirement | Implemented behavior | Evidence and boundary |
|---|---|---|
| A. Architecture and design | Bronze JSONL, isolated PySpark staging, Silver current state, Gold metrics, release lineage, full/incremental API polling, and explicit date-range backfill. | [Design](../DEMO_DESIGN.md), [Spark engine](../src/saleor_analytics/spark_engine.py), and [operations](OPERATIONS.md). Spark replays retained accepted history locally; no multi-node scaling, log CDC, or hard-delete capture is claimed. |
| B. Required Python programming | Click CLI reads structured JSONL, validates fields/types, normalizes timestamps/money/strings/nested lines, deduplicates exact versions, quarantines invalid input, and produces curated outputs through PySpark. | [CLI](../src/saleor_analytics/cli.py), [contract](../src/saleor_analytics/records.py), [ingestion/publication](../src/saleor_analytics/pipeline.py), and [samples](../samples). This is Python application code plus PySpark DataFrame transformations. |
| C. Quality, contracts, and testing | Input contract/reject gate, same-version conflict detection, Parquet current-state checks and reconciliation, checksum-bound publication, and pytest failure/recovery coverage. | [Spark checks](../src/saleor_analytics/spark_engine.py), [tests](../tests), and [container testing](TESTING.md). Dataset checks run during each build; pytest runs during developer verification and CI. |
| D. Orchestration and operations | Daily Airflow dependencies, retries, manual-only mock controls, safe same-input replay, release-bound watermarks, freshness status, and backfills without watermark advancement. | [DAG](../dags/saleor_analytics.py), [CLI guide](CLI_USER_GUIDE.md), and [operations](OPERATIONS.md). Sequential local Airflow is a demonstration; external alerts and enforced end-to-end SLOs remain future work. |
| E. CI/CD and delivery | GitHub PR/main format/lint/tests/package validation; manual main-only promotion illustration; environment-based runtime credentials. | [GitHub workflow](../.github/workflows/ci.yml) and [optional GitLab example](../.gitlab-ci.yml). Production deployment, required reviewers, and repository protections require separate configuration. |
| F. Documentation | Setup, CLI scenarios, repository structure, design choices, data locations, tests, handoff, observability, security/cost trade-offs, and production evolution. | [README](../README.md), [CLI guide](CLI_USER_GUIDE.md), [PowerShell guide](CLI_USER_GUIDE_WINDOWS.md), and [verification record](VERIFICATION.md). Only synthetic data belongs in the demo. |

## Quality and failure semantics

- Python rejects malformed records and retains rejection evidence. The configured
  reject threshold determines whether an ingestion snapshot can proceed.
- Exact duplicates collapse; different payloads sharing an order/version key
  block progress. Older versions do not replace newer order state.
- PySpark checks order/line uniqueness, keys, values, timestamps, parent links,
  conflicting versions, and daily order/product reconciliation.
- Validation records `quality/results.json` beside the candidate dataset.
  Publication checks the dataset, source, and retained results hashes, together
  with the base-release identity. Unvalidated, changed, or stale candidates fail.
- Failed builds/publication preserve the previously served release and watermark.
  Monitoring runs after publication; a monitoring failure does not undo it.

## Analytical meaning and trade-offs

Silver contains one current order and its selected current lines. Gold groups
eligible orders by UTC creation date, channel, and currency; product metrics
also group by SKU/name. Gross order value excludes `DRAFT` and `CANCELED` orders.
It is not recognized revenue or a combined monetary total across currencies.

This branch demonstrates Spark DataFrame processing and portable Parquet output.
Java startup, memory overhead, full-history rebuilds, and local filesystem locks
are accepted demo trade-offs. Distributed storage/catalogs, incremental partition
processing, IAM/secret management, encryption, retention, and source completeness
controls are production evolution work, not delivered capabilities.

## Verification and previous review

Run developer tests inside the Linux container using [TESTING.md](TESTING.md).
Use [VERIFICATION.md](VERIFICATION.md) for observed results and pending checks;
implemented code alone does not prove a successful container or live demo run.

The earlier DuckDB/dbt audit is retained in
[Git history](https://github.com/peterpanmj/gaoxin_de/blob/0f036f8/docs/ASSESSMENT_REVIEW.md).
Its old model paths, test counts, and open findings describe that baseline and
are not instructions or the acceptance status for this branch.
