# Modern data engineering demonstration

This is the current implementation guide. The earlier assessment review is a
historical gap audit; the changes below address its publication, deduplication,
malformed-input, reporting and DAG-ordering findings. All data is synthetic.

## What the demo proves

| Concept | Implementation | Evidence to show |
|---|---|---|
| Reliable publication | Immutable DuckDB releases; checksum-bound dbt results; atomic `current.json` | Failed tests or failed pointer replacement preserve the prior release |
| Idempotent processing | Canonical version hash, exact duplicate collapse, same-input retry | Same snapshot retry returns the same manifest; duplicate count is visible |
| Incremental extraction | Saleor `updatedAt` range, overlap window, publication-bound checkpoint | Initial full load followed by a small/empty delta |
| Data contracts | Explicit v2 normalization, finite money, nested types, status/timestamps | Invalid records quarantined; equal-version conflicts block publication |
| Consistent mutable entities | Order ID/version hash selects the entire current line set | Removed lines disappear; older arriving versions do not roll state back |
| Currency-correct reporting | Currency selector; common channel/date filters | Every chart and KPI uses the same currency and release |
| Observable data product | Counts, durations, checksums, source inventory, dbt artifacts, status command | Trace release -> snapshots -> raw files; show stale status |
| Controlled orchestration | Serial Airflow tasks; manual-only optional mock generation | Generation on/off; failed validation blocks publication |
| Efficient analytical files | Separate JSON/Parquet benchmark and monthly partitions | Reconciled sums and `Scanning Files: 1/12` |
| Controlled delivery | GitHub Actions tests/build; illustrative protected promotion job | PR validation and main-only manual promotion example |

Incremental extraction does not imply incremental transformation: dbt still
rebuilds this small warehouse from retained accepted history. We demonstrate
idempotent results without claiming SQL MERGE is required. Spark, log-based CDC,
cloud IAM deployment and SCD2 remain deferred until justified by requirements.

## Quick rehearsal: mock generation, replay and updates

Use PowerShell at the repository root. Start a fresh root to keep the existing
demo intact and avoid mixing legacy v1 snapshots with the v2 contract:

```powershell
uv sync --frozen
$demoSession = [guid]::NewGuid().ToString('N')
$env:ANALYTICS_ROOT = Join-Path (Get-Location) "var/modern-$demoSession"
uv run saleor-analytics doctor
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/mock/base.jsonl" --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/mock/base.jsonl" --snapshot-id base
uv run saleor-analytics build-warehouse --release-id base
uv run saleor-analytics status
```

Stop on any unexpected nonzero `$LASTEXITCODE`. Expected: 20 USD orders totaling
400.00. Re-run `ingest-file` with the same file and ID: it returns the original
manifest rather than inserting again. Re-run the same build with unchanged inputs:
it reuses the validated candidate and publication is a no-op.

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/mock/dup.jsonl" --scenario duplicate --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/mock/dup.jsonl" --snapshot-id duplicate
uv run saleor-analytics build-warehouse --release-id duplicate
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/mock/update.jsonl" --scenario update --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/mock/update.jsonl" --snapshot-id update
uv run saleor-analytics build-warehouse --release-id update
uv run saleor-analytics dashboard --port 8051
```

Duplicate input has 21 rows, 20 accepted versions and one collapsed duplicate.
The order count remains 20. Updated input changes one order from USD20 to USD30;
the total becomes USD410. The dashboard reloads one consistent release every
30 seconds. Empty channel selection means no channels, not all channels.
Press Ctrl+C to stop the server. Mock generation writes local JSONL; it does
not mutate Saleor. It requires `allow_mock=true`, enabled in local demo config.

## Inspect the current release

Publication writes `warehouse/current.json`, not `warehouse/analytics.duckdb`.
Use the pointer to locate the immutable database and retained test evidence:

```powershell
$release = Get-Content "$env:ANALYTICS_ROOT/warehouse/current.json" -Raw | ConvertFrom-Json
Get-Content "$env:ANALYTICS_ROOT/releases/$($release.release_id)/release.json"
uv run python -c 'import os,pathlib,duckdb; from saleor_analytics.config import Settings; from saleor_analytics.pipeline import published_database; p,_=published_database(Settings(pathlib.Path(os.environ["ANALYTICS_ROOT"]))); c=duckdb.connect(str(p),read_only=True); print(c.sql("select * from analytics.analytics.daily_order_metrics")); c.close()'
```

Each release keeps input manifest hashes, contract version, model checksums,
database checksum, `dbt/run_results.json`, dbt manifest and logs. Silver keeps
snapshot ID and canonical payload hash. Gold metrics can be traced through their
date/channel/currency/product grouping keys. Raw/quarantine contain full synthetic
input and should not be distributed to consumers who only need Gold.

## Failure and recovery

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/mock/bad.jsonl" --scenario invalid --count 2
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/mock/bad.jsonl" --snapshot-id bad
```

Expected failure: one of two records is invalid (50%), exceeding the 5% limit.
Inspect `quarantine/bad/orders.rejected.jsonl`. The published pointer and checkpoint
stay unchanged. Fix the source and ingest with a new snapshot ID. At count=20,
one invalid row is exactly 5% and partial acceptance is allowed; explain that
threshold deliberately rather than assuming every rejected row aborts the batch.

Conflicting canonical payloads for the same order ID and source timestamp fail
even below the reject threshold. Cross-snapshot conflicts fail dbt. To recover
from an unpublished bad snapshot without erasing evidence:

```powershell
uv run saleor-analytics exclude-snapshot --snapshot-id conflicting-input --reason 'Correcting a synthetic equal-version conflict'
```

Substitute the actual bad snapshot ID, then correct/reingest with new IDs and
build a new release. Exclusions are audited in `excluded-snapshots.json`;
inputs of the current published release cannot be excluded. This is not a
production erasure/retention mechanism.

`build-warehouse --no-publish` creates a validated candidate. `publish-candidate`
now verifies its database, input/test hashes and base release before committing.
It rejects unvalidated, altered, foreign or stale candidates. An old validated
release cannot overwrite a newer publication. Candidate and root locks coordinate
local writers; filesystem administration remains outside this trust boundary.

## Full load to incremental API polling

Use a separate fresh root for Saleor data and existing local credentials. The
first incremental run requires a successfully published full extraction:

```powershell
$env:ANALYTICS_ROOT = Join-Path (Get-Location) "var/api-$demoSession"
$env:SALEOR_URL = 'http://localhost:8000/graphql/'
$env:SALEOR_EMAIL = 'admin@example.com'
$env:SALEOR_PASSWORD = 'admin'
uv run saleor-analytics extract-saleor --snapshot-id initial --mode full
uv run saleor-analytics build-warehouse --release-id initial
uv run saleor-analytics extract-saleor --snapshot-id delta --mode incremental
uv run saleor-analytics build-warehouse --release-id delta
```

The lower bound is the committed source watermark minus five minutes; the upper
bound is invocation time minus five seconds. Bounds are inclusive and duplicates
are safe. Each extraction ID persists its window under `extractions/ID/request.json`.
The source is identified by a hash of the configured URL; use one stable URL.
An empty bounded delta is valid. Ordinary empty file input is not.

No watermark advances at extraction or staging. Publication commits the serving
release and watermark in one pointer replacement. Retry a completed extraction
with the same ID to reuse its original snapshot. Source paging failures never
create an approved snapshot. Repeated/missing continuation cursors fail rather
than loop indefinitely. Changing source/mode requires a new extraction ID.

API polling assumes a mostly quiet synthetic source, synchronized clocks and
delays within the overlap. It is not a consistent database snapshot or a guarantee
to capture every intermediate mutation. Hard deletes are not inferred. Production
requires reconciliation, tombstones/events or log CDC and explicit retention rules.

## Airflow

```powershell
docker compose -f infra/airflow/compose.yml build
docker compose -f infra/airflow/compose.yml up -d
```

For this Windows proxy setup only, set
`$env:BUILD_HTTP_PROXY='http://host.docker.internal:10808'` before building if
package downloads need the host proxy. The address is a runtime build setting,
not a required project dependency. Airflow is available at `http://localhost:8081`.
Its standalone command creates local login credentials; retrieve them locally
from the standalone startup output. This is an isolated demo, not HA production.

Stages: `prepare -> extract_and_validate -> stage -> transform_and_test -> publish -> monitor`.
The CLI runs in a separate Python environment from Airflow to avoid dependency
conflicts. Docker named volumes retain scheduler state and analytical data.
No data-volume reset is needed for repeated runs; run IDs derive unique batch IDs.

Manual trigger examples in the Airflow UI:

```json
{"generate_mock": true, "scenario": "duplicate", "count": 20}
```

```json
{"generate_mock": false, "input_mode": "fixture"}
```

```json
{"generate_mock": true, "scenario": "invalid", "count": 2}
```

The last run must fail ingestion and leave publish unexecuted. Scheduled runs use
generation disabled; attempting generation on a nonmanual run fails explicitly.
For Saleor, set runtime credential environment variables before Compose startup,
choose `input_mode=saleor`, and use `extract_mode=incremental` after a published
full baseline. A historical Airflow logical date does not create an as-of source query.
Retries reuse IDs and artifacts; deterministic validation errors still require
corrected input/new IDs. Monitoring failure happens after publication and does
not roll a successful release back.

## Parquet and pruning experiment

```powershell
uv run saleor-analytics benchmark-storage --run-id parquet-001 --rows 120000
Get-Content "$env:ANALYTICS_ROOT/benchmarks/parquet-001/results.json"
Get-Content "$env:ANALYTICS_ROOT/benchmarks/parquet-001/pruning-plan.txt"
```

This isolated experiment exports 120,000 deterministic rows as JSON and typed,
ZSTD-compressed monthly Parquet partitions. Both queries return 10,000 rows and
490,000.00 for month 1. Inspect `File Filters` and `Scanning Files: 1/12` in the
actual plan. One local run measured 11,885,290 JSON bytes versus 133,908 Parquet
bytes, with query durations around 0.111s and 0.028s respectively. Repeated
synthetic values compress unusually well. Cache, query order and scale affect
timings; these are observations, not a general performance guarantee. The main
warehouse still uses DuckDB tables; this is not an Iceberg or Spark deployment.

## Schema, history and security boundaries

Contract v2 rejects unknown statuses, unsupported currencies, invalid nested
types, non-finite/out-of-range money, invalid timestamps and backward source
timestamps. Optional unknown fields are ignored in normalized data while raw
bytes remain available. Replaying an unsupported contract fails closed. To migrate
legacy v1 data, create a fresh root and ingest its raw file under a new ID; no
automatic migration changes an existing release.

Versioned orders support current-state analytics, not SCD2 or event-time history.
SCD2 would need a real historical question, validity intervals and persistent
snapshot state. Secrets remain runtime environment values; local defaults are
only for synthetic Saleor. Cloud secret management/IAM, TLS, RBAC, retention and
completeness monitoring remain production design tasks.

## Delivery and assessment mapping

GitHub Actions runs formatting/lint, Python invariants, fixture-backed dbt
integration/failure tests and package build. Manual dispatch on main can exercise
an illustrative `production` environment gate with the same tested artifact.
Configure required reviewers in repository environment settings; YAML alone does
not establish approval policy. No real production deployment target is configured.

A: layers, incremental/replay semantics, Parquet/pruning and lineage.
B: Click, structured files, normalization, deduplication and curated output.
C: quarantine, contract/conflict tests and publication gates.
D: ordered Airflow tasks, retries, checkpoints and freshness status.
E: automated CI and illustrative controlled artifact promotion.
F: reproducible guide, evidence and explicit local/production trade-offs.
