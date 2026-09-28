# CLI user guide (Windows PowerShell)

Run these commands in PowerShell from the repository root. Set `$repoRoot` to
your own clone location first. For Git Bash syntax, use the
[Git Bash guide](CLI_USER_GUIDE.md). The Python
entry point is `uv run saleor-analytics`; `uv` manages the locked project
environment and `saleor-analytics` is the Click CLI.
The [end-to-end flow diagram](CLI_USER_GUIDE.md#end-to-end-demo-flow) shows
where mock JSONL and Saleor GraphQL ingestion meet and how a release reaches
Dash. The [layer results](CLI_USER_GUIDE.md#what-each-layer-produces) section
explains the Bronze, quarantine, Silver, Gold, and DuckDB outputs for either
shell.

## Environment setup

Install Python 3.12, Docker Desktop, and uv. Start Docker Desktop before using
Compose. If uv is missing, install it with Windows Package Manager:

```powershell
winget install --id=astral-sh.uv -e
```

Open a new PowerShell terminal after installation. From the repository root,
verify the tools, install locked dependencies, and check the CLI:

```powershell
$repoRoot = 'C:\path\to\<repository-folder>'
Set-Location $repoRoot
python --version
uv --version
docker version
docker compose version
uv sync --frozen
uv run saleor-analytics doctor
```

Local JSONL ingestion, warehouse builds, and Dash run on the host without
Docker. For the Airflow DAG, build its image once, then start the service:

```powershell
docker compose -f infra/airflow/compose.yml build
docker compose -f infra/airflow/compose.yml up -d
docker compose -f infra/airflow/compose.yml ps
```

Open `http://localhost:8081` for Airflow. The Compose `analytics-data` volume
persists the pipeline data after the container stops. The repository's
`artifacts/` directory maps to `/opt/artifacts` in the container. A host
`ANALYTICS_ROOT` under `var/` is separate from the Docker volume. Stop the
service while preserving the volume with:

```powershell
docker compose -f infra/airflow/compose.yml stop
```

If image build downloads require a proxy, configure Docker Desktop for image
pulls and set the Compose build variable for package downloads:

```powershell
$env:BUILD_HTTP_PROXY = 'http://host.docker.internal:10808'
docker compose -f infra/airflow/compose.yml build
```

Use that variable only when the v2rayN listener is running and reachable from
Docker. See the [network setup](MODERN_DE_DEMO.md#docker-pulls-and-image-builds)
for the distinction between Docker Desktop pulls and image builds.

## Mock-data ingestion walkthrough

Use a fresh root for each rehearsal. The commands below create a local JSONL
file, capture it in Bronze, run dbt, publish a DuckDB release, and start Dash:

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

Open `http://localhost:8051`. Expect 20 USD orders and USD400 gross order
value. Press Ctrl+C in the Dash terminal to stop it before continuing. The
generator creates local Saleor-shaped data and does not mutate Saleor.

### Retry and duplicate input

Reingesting the same file with the same snapshot ID returns its existing
manifest. To show duplicate-row handling, run:

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/duplicate.jsonl" --scenario duplicate --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/duplicate.jsonl" --snapshot-id duplicate
uv run saleor-analytics build-warehouse --release-id duplicate
```

The manifest reports 21 input rows, 20 accepted versions, and one duplicate.
Gold still has 20 orders and USD400 gross order value.

### Apply updated source state

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/update.jsonl" --scenario update --count 20
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/update.jsonl" --snapshot-id update
uv run saleor-analytics build-warehouse --release-id update
```

One order becomes USD30, so the 20 orders total USD410. The latest complete
line set represents the current order state.

### Show quarantine and a blocked batch

```powershell
uv run saleor-analytics mock-data "$env:ANALYTICS_ROOT/invalid.jsonl" --scenario invalid --count 2
uv run saleor-analytics ingest-file "$env:ANALYTICS_ROOT/invalid.jsonl" --snapshot-id invalid
```

The ingestion command intentionally exits nonzero: one rejected row out of two
exceeds the 5% threshold. Inspect the rejected record:

```powershell
Get-Content "$env:ANALYTICS_ROOT/quarantine/invalid/orders.rejected.jsonl"
uv run saleor-analytics status
```

The previously published `update` release remains active. Correct the input
and use new snapshot and release IDs for another attempt.

## CLI responsibilities

| Command | Purpose |
|---|---|
| `doctor` | Resolve configuration and local paths. |
| `mock-data OUTPUT` | Generate baseline, update, duplicate, or invalid JSONL. |
| `ingest-file INPUT --snapshot-id ID` | Validate, normalize, deduplicate, and quarantine source rows. |
| `extract-saleor --snapshot-id ID --mode full` | Capture a paginated API snapshot. |
| `extract-saleor --snapshot-id ID --mode incremental` | Poll source updates since the published checkpoint. |
| `extract-saleor --snapshot-id ID --mode backfill --start UTC --end UTC` | Re-read a historical `[start, end)` `updatedAt` interval without moving the checkpoint. |
| `build-warehouse --release-id ID` | Stage, dbt validate, and publish a release. |
| `stage-warehouse`, `validate-candidate`, `publish-candidate` | Run those release steps separately. |
| `exclude-snapshot --snapshot-id ID --reason TEXT` | Audit exclusion of an unpublished bad snapshot. |
| `status --max-age-hours 24` | Report release and extraction freshness. |
| `export-artifacts` | Copy the active release and latest source snapshot for review. |
| `dashboard` | Serve the Gold metrics in Dash. |

Use `uv run saleor-analytics COMMAND --help` for options. Place global
`--config PATH` before `COMMAND`. The
[modern DE guide](MODERN_DE_DEMO.md) covers Saleor full, incremental, and date-range backfill
extraction plus Airflow manual trigger JSON.

## Export and present Airflow results

After a successful Airflow DAG run, export the active release into the host
`artifacts/` directory:

```powershell
docker compose -f infra/airflow/compose.yml ps
docker compose -f infra/airflow/compose.yml exec airflow /opt/analytics/bin/saleor-analytics export-artifacts
```

The export does not overwrite an existing bundle. It includes the DuckDB
release, dbt evidence, and the latest Bronze snapshot.

For a separate dated review copy, pass a destination inside the mapped
`/opt/artifacts` directory. The bundle then appears under the host's
`artifacts/<review-name>/<release-id>/`:

```powershell
$reviewName = 'review-' + (Get-Date -Format 'yyyyMMdd-HHmmss')
docker compose -f infra/airflow/compose.yml exec airflow /opt/analytics/bin/saleor-analytics export-artifacts "/opt/artifacts/$reviewName"
Get-ChildItem "artifacts/$reviewName" -Recurse -File
```

To view the default export in Dash on the host, set that bundle as the data
root:

```powershell
$releaseJson = docker compose -f infra/airflow/compose.yml exec -T airflow cat /opt/data/warehouse/current.json
if ($LASTEXITCODE -ne 0) { throw 'Airflow is unavailable or has no published release.' }
$releaseId = ($releaseJson | ConvertFrom-Json).release_id
if (-not $releaseId) { throw 'The Airflow current.json has no release_id.' }
$env:ANALYTICS_ROOT = Join-Path (Get-Location) "artifacts/$releaseId"
uv run saleor-analytics status
uv run saleor-analytics dashboard --host 127.0.0.1 --port 8051
```

Open `http://localhost:8051`; press Ctrl+C to stop Dash. The Git Bash
[presentation launcher](../scripts/start_presentation_demo.sh) automates
Airflow startup, export, and Dash startup when you choose to present from Git
Bash.
