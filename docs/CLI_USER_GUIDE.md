# CLI user guide

Run commands from the repository root after `uv sync --frozen`. The Click entry
point is `uv run saleor-analytics`. Use `--help` on any command for its assessment
mapping and parameters. Global `--config PATH` comes before the command.

## Python environment and command runner

`uv` is Astral's Python project and package manager, not a Click command. It
creates the project's `.venv`, installs the exact dependency versions recorded
in `uv.lock`, and runs commands in that environment. Install it on Windows with:

```powershell
winget install --id=astral-sh.uv -e
uv --version
```

`uv run saleor-analytics doctor` means: use this repository's managed Python
environment, start the installed `saleor-analytics` executable, then pass
`doctor` to the Click CLI. The execution path is:

```text
uv run -> saleor-analytics executable -> Click CLI -> Python pipeline modules
```

After `uv sync`, the equivalent Windows command is
`./.venv/Scripts/saleor-analytics.exe doctor`; prefer `uv run` in documentation,
CI and the demo because it reliably selects the project environment. In Git Bash,
the same `uv run` commands work. Use `export ANALYTICS_ROOT=...` rather than the
PowerShell `$env:ANALYTICS_ROOT = ...` form when setting environment variables.

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
| `export-artifacts [DESTINATION] [--release-id ID] [--all-snapshots]` | Copy a release and its latest Bronze, quarantine, extraction, and dbt evidence for review | A/F |
| `status --max-age-hours 24` | Print release/count/freshness metadata; fail when stale | D/F |
| `dashboard` | Query Gold with consistent currency/channel/date filters | A/F |

`build-warehouse --no-publish` validates without changing serving state. Paths for
explicit validate/publish commands are `ROOT/releases/ID/analytics.duckdb`.
Use new release IDs when inputs change. Reusing unchanged IDs is retry-safe.
A stale candidate cannot replace a newer published release.

## Source, orchestration and troubleshooting

The [modern DE guide](MODERN_DE_DEMO.md) contains executable full/incremental
Saleor commands, Airflow manual trigger JSON and
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

## Export a review bundle

After a successful publication, copy the active release and its latest source
snapshot to a folder for inspection:

```powershell
uv run saleor-analytics export-artifacts
```

This creates `artifacts/<release-id>/` with the release DuckDB file, dbt evidence,
the latest Bronze snapshot, related quarantine/extraction files, and
`warehouse/current.json`. The command never overwrites an existing bundle. To
export a specific historical or failed candidate, supply `--release-id RELEASE_ID`.
Add `--all-snapshots` only when a reviewer needs complete source lineage.
The DuckDB file contains the complete release state; the default bundle's single
Bronze snapshot is not enough to rebuild that state from source.
Pass `DESTINATION` to use another folder.

For data produced by Airflow, the compose file maps `./artifacts` on the host to
`/opt/artifacts` in the container. From `infra/airflow/`, run:

```bash
docker compose exec airflow /opt/analytics/bin/saleor-analytics export-artifacts
```

## Presentation launcher (Git Bash)

After you have created and published a baseline release, run this from the
repository root:

```bash
bash scripts/start_presentation_demo.sh
```

It starts Airflow, repairs a stale Airflow webserver PID marker when the marker
does not belong to a running process,
waits for the UI, exports the active release if needed, then starts Dash at
`http://localhost:8051`. It does not configure networking or generate data.
