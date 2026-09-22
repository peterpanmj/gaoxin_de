# Operational design

## Airflow

`dags/saleor_analytics.py` is an illustrative daily DAG, not a deployed or runtime-verified workflow. The diagram below shows the intended sequence. The current DAG lacks an `extract -> transform_and_publish` dependency; both tasks depend only on `run_id`. Correct that edge before relying on scheduled execution. See the [assessment review](ASSESSMENT_REVIEW.md).

```mermaid
flowchart LR
    A[Saleor GraphQL] --> B[extract-saleor]
    B --> C[Bronze snapshot and quarantine]
    C --> D[build-warehouse]
    D --> E[dbt models and tests]
    E --> F[Atomic DuckDB publish]
    F --> G[Dash dashboard]
```

The HTTP client retries transient read failures. Extraction reaches file ingestion only after every page returns; validation then writes a passed or failed manifest. A paging failure does not trigger publication from that command. The DAG's missing dependency currently prevents it from guaranteeing the intended sequence. One active DAG run does not order tasks inside a run or serialize external CLI calls.

Snapshot and release directories refuse reuse. Manual CLI replays require new IDs; Airflow retries reuse `ts_nodash` and may fail on an existing or incomplete directory. Retry/resume idempotency is not implemented. Warehouse rebuilds replay all approved normalized files, not raw files through a new validator.

Historical logical dates do not constrain the source query: triggering an old date still reads current Saleor state. Supported local reprocessing is a rebuild of retained accepted history with a new release ID. True time-bounded or as-of backfills require an explicit source/history contract. In a larger deployment, isolate backfill compute and require validated promotion; the current direct `publish-candidate` command does not verify test evidence.

Saleor's official `populatedb` command currently provides one-time synthetic bootstrap. The agreed CLI API scenarios and optional user-controlled mock generation inside Airflow remain unimplemented. The intended behavior is an explicit manual-trigger parameter, disabled for normal scheduled runs. `allow_mock` currently has no effect.

## Freshness, observability and lineage

Snapshot manifests record raw checksum, ingestion completion time (named `extracted_at`), counts, reject rate, contract label, and validation status. Silver keeps snapshot IDs; Gold can be related through its grouping keys. Complete release lineage is absent: releases do not record input inventories or retained test results, and replay does not verify checksums. Proposed production monitoring would export these as OpenTelemetry/OpenLineage events and alert when:

- the successful snapshot is older than the 06:30 UTC freshness SLO;
- reject rate exceeds the configured 5% threshold;
- accepted record volume changes materially from the trailing baseline; or
- dbt tests or the atomic publication step fail.

No such alerts or exporters currently run. The 06:30 UTC SLO is a design target. The dashboard loads data once at startup and must restart to see a new publication. It does not show freshness, release ID or quality status.

## Security and governance

Saleor credentials come from `SALEOR_TOKEN` or runtime-only environment variables. Git ignores local data, `.env` files, Python environments and Codex/ChatGPT artifacts. The extractor selects no customer names, emails, addresses, or private metadata. Production access should use a read-only service account, TLS, encrypted storage, least-privilege warehouse roles, secret-manager injection, and audit logging for releases and dashboard access.

## Cost and scale

This demo chooses DuckDB because the source has tens of synthetic orders and the dashboard needs compact local aggregates. It replays immutable snapshots for clarity. At scale, Bronze becomes partitioned object storage, the version table becomes an incremental Iceberg/warehouse table clustered by `updated_at` and order ID, and dbt processes only changed partitions. Lifecycle rules expire raw and quarantine data according to policy; the dashboard reads Gold aggregates, never raw order payloads.
