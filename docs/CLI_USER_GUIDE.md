# CLI user guide

Run commands from the repository root after `uv sync --frozen`. The Click entry
point is `uv run saleor-analytics`. Use `--help` on any command for its assessment
mapping and parameters. Global `--config PATH` comes before the command.

## Mock-data ingestion walkthrough

Use a fresh root so experiments cannot alter an earlier demonstration:

```powershell
$demoSession = [guid]::NewGuid().ToString('N')
$env:ANALYTICS_ROOT = Join-Path (Get-Location) "var/guide-$demoSession"
uv run saleor-analytics doctor
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/base.jsonl" --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/base.jsonl" --snapshot-id base
uv run saleor-analytics build-warehouse --release-id base
uv run saleor-analytics status
uv run saleor-analytics dashboard --port 8051
```

Expected: 20 USD orders and USD400 gross order value. Press Ctrl+C to stop Dash.
The generator requires `allow_mock=true` (enabled in local config). It creates
Saleor-shaped files without changing the Saleor database. `ingest-file` is the
required structured-file Python ingestion path (B), independently of the API.

### Retry and duplicate input

Repeat ingestion with the same file and snapshot ID: the original manifest is
returned. Different content cannot reuse that ID. To demonstrate duplicate rows:

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/duplicate.jsonl" --scenario duplicate --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/duplicate.jsonl" --snapshot-id duplicate
uv run saleor-analytics build-warehouse --release-id duplicate
```

The manifest reports 21 input rows, 20 accepted versions and one duplicate.
Gold still contains 20 orders totaling USD400. Accepted counts describe historical
versions; they are not the current business order count.

### Apply updated source state

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/update.jsonl" --scenario update --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/update.jsonl" --snapshot-id update
uv run saleor-analytics build-warehouse --release-id update
```

One order becomes USD30; 20 orders now total USD410. The newer version replaces
the complete current line set. Older arrivals cannot roll that order backward.

### Demonstrate quarantine and a failed batch

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/invalid.jsonl" --scenario invalid --count 2
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/invalid.jsonl" --snapshot-id invalid
Get-Content "$env:ANALYTICS_ROOT/quarantine/invalid/orders.rejected.jsonl"
```

This intentionally exits nonzero: one invalid quantity among two rows exceeds
5%. The published release stays unchanged. Correct input and use a new ID.
With 20 rows the same single rejection is exactly 5%, which is permitted.
Equal-version conflicting payloads always fail regardless of reject threshold.

To use the checked-in small fixtures instead, start another fresh root and ingest
`samples/orders-baseline.jsonl` (2 orders, USD50), then
`samples/orders-update.jsonl` (same 2 orders, USD60).
For the verified update and tolerant-quarantine scenario, see the sample inventory
and modern guide. Keep fixtures separate from generated orders to preserve the
expected totals.

## Command responsibilities

| Command | Data engineering purpose | Requirements |
|---|---|---|
| `doctor` | Resolve config and paths; not service health | B/F |
| `mock-data OUTPUT` | Deterministic baseline/update/duplicate/invalid JSONL | C/D/F |
| `ingest-file INPUT --snapshot-id ID` | Validate, normalize, deduplicate, quarantine, retain raw lineage | A/B/C |
| `extract-saleor --snapshot-id ID --mode full` | Bounded paginated API snapshot through the same ingestion contract | A/B/D |
| `extract-saleor --snapshot-id ID --mode incremental` | Poll changes since published checkpoint, with overlap | A/D |
| `stage-warehouse --release-id ID` | Build isolated staging tables from approved history | B/D |
| `validate-candidate DATABASE` | Run dbt models/tests and bind retained evidence to the candidate | C/D |
| `publish-candidate DATABASE` | Verify evidence and atomically commit release/checkpoint | C/D |
| `build-warehouse --release-id ID` | Stage, validate and publish in one command | B/C/D |
| `exclude-snapshot --snapshot-id ID --reason TEXT` | Audit exclusion of an unpublished bad snapshot | A/C/D |
| `status --max-age-hours 24` | Print release/count/freshness metadata; fail when stale | D/F |
| `dashboard` | Query Gold with consistent currency/channel/date filters | A/F |
| `benchmark-storage --run-id ID --rows 120000` | Compare JSON/Parquet results, sizes and pruning | A/F |

`build-warehouse --no-publish` validates without changing serving state. Paths for
explicit validate/publish commands are `ROOT/releases/ID/analytics.duckdb`.
Use new release IDs when inputs change. Reusing unchanged IDs is retry-safe.
A stale candidate cannot replace a newer published release.

## Source, orchestration and troubleshooting

The [modern DE guide](MODERN_DE_DEMO.md) contains executable full/incremental
Saleor commands, Airflow manual trigger JSON, partition-pruning evidence and
release inspection. Set `SALEOR_URL`, `SALEOR_EMAIL`, `SALEOR_PASSWORD` in the
runtime environment. Source URL is the checkpoint identity; keep it stable.

- Missing published baseline: publish a full extraction before incremental mode.
- Contract mismatch: use a fresh root and reingest raw input under contract v2.
- Failed dbt conflict test: correct the source; exclude the unpublished bad
  snapshot with a reason, ingest corrected content with new IDs and rebuild.
- Checksum mismatch: investigate changed evidence; do not bypass the guard.
- No dashboard data: verify a published release, currency and channel selection.
  An empty channel selection intentionally shows no data.
- Failed `status`: examine publication and extraction age. Fresh publication
  alone does not establish source completeness.

Raw files and quarantine may contain arbitrary input fields. Use synthetic data
only. Files under `var/` are local runtime evidence and are not Git deliverables.
