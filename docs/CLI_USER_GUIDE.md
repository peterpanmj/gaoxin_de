# CLI user guide

This guide uses the commands that exist today. All supplied samples are synthetic.
The quickest demo is **mock JSONL file -> Python validation -> dbt/DuckDB -> Dash**.
It does not require Docker, Saleor, authentication or Airflow.

There are two ways to ingest mock data:

1. **Local files:** use the included `samples/*.jsonl` files with `ingest-file`.
   This is the easiest way to test valid, updated and rejected records. It does
   not insert anything into Saleor.
2. **Saleor's synthetic dataset:** initialize Saleor with its official
   `populatedb` command, then use `extract-saleor` to read its API.

There is currently no `seed`, `generate-mock` or API mutation CLI command.
`allow_mock` has no effect, and optional generation inside Airflow is pending.

## 1. Setup and help

Examples use Windows PowerShell. Run them from the repository root, in the same
terminal unless instructed otherwise. Install Python 3.12 and `uv`, then:

```powershell
Set-Location D:\gaoxin_de
uv sync --frozen
uv run saleor-analytics --help
uv run saleor-analytics ingest-file --help
uv run saleor-analytics build-warehouse --help
```

The equivalent module entry point is `uv run python -m saleor_analytics`.
Each command's help describes its assessment mapping and known limitations.
Check `$LASTEXITCODE` immediately after a command: zero means success; nonzero
means stop and inspect its output. Current domain errors often print tracebacks.

## 2. Start an isolated mock-data session

The following assigns a fresh data directory. It keeps these examples separate
from your existing Saleor snapshots and published warehouse:

```powershell
$guideSession = [guid]::NewGuid().ToString('N')
$env:ANALYTICS_ROOT = Join-Path (Get-Location) "var/cli-guide-$guideSession"
uv run saleor-analytics doctor
```

`doctor` prints the resolved root and checks configuration bounds. It does not
check connectivity or credentials. Keep this terminal open: the environment
override applies to every following command. A second terminal must use the
same absolute `ANALYTICS_ROOT` to see this session's data.

## 3. Use case: ingest valid mock orders and create reports

The baseline file contains two USD orders in channel `mock-us`:

- `mock-order-1`: two notebooks at USD 10 each, order total USD 20.
- `mock-order-2`: three mugs at USD 10 each, order total USD 30.

```powershell
uv run saleor-analytics ingest-file samples/orders-baseline.jsonl --snapshot-id baseline-001
if ($LASTEXITCODE -ne 0) { throw 'Baseline ingestion failed' }
uv run saleor-analytics build-warehouse --release-id baseline-001
if ($LASTEXITCODE -ne 0) { throw 'Warehouse build failed' }
```

Expected ingestion output: `Accepted 2; rejected 0; quality gate passed`.
The build runs six dbt models and seven data tests, then publishes to
`$env:ANALYTICS_ROOT/warehouse/analytics.duckdb`.

Inspect the analytical result without installing a database GUI:

```powershell
uv run python -c 'import os, pathlib, duckdb; p = pathlib.Path(os.environ["ANALYTICS_ROOT"]) / "warehouse/analytics.duckdb"; c = duckdb.connect(str(p), read_only=True); print(c.sql("select * from analytics.analytics.daily_order_metrics order by order_date, channel, currency")); c.close()'
```

Expected Gold row: date `2026-01-15`, channel `mock-us`, currency `USD`,
`order_count=2`, `gross_amount=50.00`, `average_order_value=25.00`.
These are gross order values, not payments or recognized revenue.

Start the dashboard on a port separate from the existing demo:

```powershell
uv run saleor-analytics dashboard --port 8051
```

Open `http://127.0.0.1:8051/`. Press **Ctrl+C** to stop the foreground server and
continue the guide. Restart it after later publications: data loads at startup.
These fixtures use one currency/channel because the current dashboard mixes
currencies in totals and does not filter product rankings by channel.

## 4. Use case: process an updated order

`orders-update.jsonl` contains a newer version of `mock-order-1`: three notebooks
and a USD 30 total. Its creation date is unchanged; its source update time is
later. The other order is absent from this file but remains in retained history.

```powershell
uv run saleor-analytics ingest-file samples/orders-update.jsonl --snapshot-id update-001
if ($LASTEXITCODE -ne 0) { throw 'Update ingestion failed' }
uv run saleor-analytics build-warehouse --release-id update-001
if ($LASTEXITCODE -ne 0) { throw 'Updated build failed' }
```

Run the SQL inspection command from section 3 again. Expected result: **two
orders, USD 60 gross, USD 30 average**, still grouped under `2026-01-15`.
This demonstrates current-state selection and historical aggregate recomputation.
It does not demonstrate CDC, historical as-of querying or source deletion.

## 5. Use case: reject troublesome mock data

`orders-mixed.jsonl` contains the two valid baseline orders plus one invalid
order with `quantity=0`. The default reject-rate limit is 5%; one rejected row
out of three is 33.3%, so the whole snapshot fails its gate:

```powershell
uv run saleor-analytics ingest-file samples/orders-mixed.jsonl --snapshot-id bad-001
$ingestExit = $LASTEXITCODE
if ($ingestExit -eq 0) { throw 'Expected the strict reject-rate gate to fail' }
Get-Content (Join-Path $env:ANALYTICS_ROOT 'bronze/bad-001/manifest.json')
Get-Content (Join-Path $env:ANALYTICS_ROOT 'quarantine/bad-001/orders.rejected.jsonl')
```

Expected manifest: `accepted_count=2`, `rejected_count=1`,
`quality_gate="failed"`. The rejected row includes its input line number, raw
line and an error saying quantity must be a positive integer. Ingestion does not
publish, so the serving warehouse remains unchanged. A subsequent build ignores
this failed snapshot, including its two valid rows.

To correct real input, fix its source and ingest it with a **new snapshot ID**.
Do not edit Bronze or its manifest to change a failed result into a passed one.

### Optional: allow a known reject in a teaching-only run

The included `config/mock-tolerant.toml` permits 34% rejects so this tiny fixture
can demonstrate partial acceptance. This is not a recommended production threshold.
The session's `ANALYTICS_ROOT` still overrides the root in that config.

```powershell
uv run saleor-analytics --config config/mock-tolerant.toml ingest-file samples/orders-mixed.jsonl --snapshot-id tolerant-001
if ($LASTEXITCODE -ne 0) { throw 'Tolerant ingestion failed' }
uv run saleor-analytics build-warehouse --release-id tolerant-001
if ($LASTEXITCODE -ne 0) { throw 'Tolerant build failed' }
```

Expected ingestion output: `Accepted 2; rejected 1; quality gate passed`.
Only accepted records become staging input. If you followed section 4, the
newer order version still wins and the Gold result stays **two orders / USD 60**.
Without section 4, the result is **two orders / USD 50**.

Changing the config does not retroactively approve `bad-001`: the new snapshot
is evaluated independently. A reject rate exactly equal to a configured limit
passes; empty input always fails.

## 6. Use case: build for inspection without publishing

```powershell
uv run saleor-analytics build-warehouse --release-id inspect-001 --no-publish
```

This still runs dbt models and tests. The candidate is under
`releases/inspect-001/analytics.duckdb`, while the serving warehouse stays as it
was. To create a newly tested serving release, run another normal build with a
new ID. Every build reads **all passed snapshots in the selected root**; it has
no option to select only one input snapshot.

The advanced `publish-candidate DATABASE` command merely copies a file. It does
not verify dbt evidence or run tests, so use `build-warehouse` for normal demos.

## 7. Use case: ingest Saleor's own mock data via its API

This route requires Docker and the Saleor submodule. It changes the local Saleor
database during bootstrap. If the existing Saleor instance is already populated,
skip migration/population and proceed to the environment settings and extraction.

```powershell
git submodule update --init --recursive
docker compose -f infra/saleor-platform/docker-compose.yml pull
docker compose -f infra/saleor-platform/docker-compose.yml run --rm api python3 manage.py migrate
docker compose -f infra/saleor-platform/docker-compose.yml run --rm api python3 manage.py populatedb --createsuperuser
docker compose -f infra/saleor-platform/docker-compose.yml up -d
```

Run `populatedb` only for initial bootstrap; it is not an idempotent CLI scenario
generator. Wait until `http://localhost:8000/graphql/` is available. Use another
isolated root to avoid combining these orders with the file fixtures:

```powershell
$saleorSession = [guid]::NewGuid().ToString('N')
$env:ANALYTICS_ROOT = Join-Path (Get-Location) "var/saleor-guide-$saleorSession"
$env:SALEOR_URL = 'http://localhost:8000/graphql/'
$env:SALEOR_EMAIL = 'admin@example.com'
$env:SALEOR_PASSWORD = 'admin'
uv run saleor-analytics extract-saleor --snapshot-id saleor-001
if ($LASTEXITCODE -ne 0) { throw 'Saleor extraction failed' }
uv run saleor-analytics build-warehouse --release-id saleor-001
if ($LASTEXITCODE -ne 0) { throw 'Saleor warehouse build failed' }
```

These are Saleor's public local-development credentials. An existing
`SALEOR_TOKEN` takes precedence over email/password; use a valid token for the
same instance or clear the stale variable. Keep credentials out of committed files.
Query retries are bounded; mutations are not retried. Extraction reads the
current state and should run while the synthetic source is not being edited.
Source-generated counts can vary; the earlier local bootstrap yielded 20 orders.
Saleor data can include USD and PLN: inspect Gold by currency rather than relying
on the dashboard's combined monetary KPI.

## 8. Create your own JSONL mock files

Copy a supplied sample and edit it. The following expanded JSON shows the input
shape; **save each complete order on one line**, not as a pretty-printed object
spanning multiple lines and not wrapped in a JSON array:

```json
{
  "id": "mock-order-99",
  "number": "MOCK-99",
  "created": "2026-01-15T10:00:00Z",
  "updatedAt": "2026-01-15T10:30:00Z",
  "status": "UNFULFILLED",
  "channel": {"slug": "mock-us", "currencyCode": "USD"},
  "total": {"gross": {"amount": "20.00", "currency": "USD"}},
  "lines": [{
    "id": "mock-line-99",
    "productName": "Synthetic notebook",
    "productSku": "MOCK-99",
    "quantity": 2,
    "unitPrice": {"gross": {"amount": "10.00", "currency": "USD"}},
    "totalPrice": {"gross": {"amount": "20.00", "currency": "USD"}}
  }]
}
```

Use UTF-8 without a byte-order mark. Provide timezone offsets or `Z`, finite
non-negative monetary amounts, positive integer quantities, and nonempty line
lists. Use distinct order/line IDs for new entities. To simulate an update, reuse
the order ID, increase `updatedAt` and provide its complete current line list.
The currently allowed warehouse currencies are USD, EUR and PLN. `CANCELED` and
`DRAFT` orders remain in Silver but are excluded from Gold metrics.

For deterministic bad-data exercises, use missing required fields, timezone-free
timestamps, mismatched currencies, malformed JSON or zero quantities. Some other
malformed nested types currently abort the process rather than reaching quarantine;
see the [assessment review](ASSESSMENT_REVIEW.md). Python does not yet collapse
duplicate orders; repeating one within the same snapshot may fail line uniqueness
in dbt. Do not use equal-time conflicting versions as a supported update scenario.

## 9. Command and configuration reference

| Command | Purpose | Main inputs |
|---|---|---|
| `doctor` | Validate selected config bounds and show root | Global `--config` |
| `ingest-file` | Validate file and write Bronze/quarantine | JSONL path, `--snapshot-id` |
| `extract-saleor` | API extraction followed by file ingestion | Credentials, `--snapshot-id` |
| `build-warehouse` | Stage history, run dbt, normally publish | `--release-id`, `--no-publish` |
| `publish-candidate` | Unchecked advanced file copy | Existing database path |
| `dashboard` | Start read-only report server | `--host`, `--port` |

Global `--config PATH` goes **before** the subcommand. Input file paths are
relative to the current working directory. The default is `config/local.toml`.
TOML `root` is relative to that config file; `ANALYTICS_ROOT` overrides it and a
relative override resolves from the working directory. Prefer an absolute root.
`SALEOR_URL` overrides the API endpoint. `page_size`, `timeout_seconds`,
`max_attempts` and `reject_rate_limit` are nonsecret TOML settings.

Snapshot/release IDs must be 1-80 characters, starting with a letter or digit and
containing only letters, digits, hyphens or underscores. They are directory keys,
not dates defining extraction ranges. Never reuse an ID in the same data root.

Expected layout beneath the selected root:

```text
bronze/<snapshot-id>/orders.raw.jsonl
bronze/<snapshot-id>/orders.accepted.jsonl
bronze/<snapshot-id>/manifest.json
quarantine/<snapshot-id>/orders.rejected.jsonl
releases/<release-id>/analytics.duckdb
releases/<release-id>/release.json
warehouse/analytics.duckdb
```

dbt artifacts are currently shared under `analytics/target` and `analytics/logs`,
not stored per release. Run this local demo one pipeline invocation at a time.

## 10. Troubleshooting and ending a session

- **Already exists:** use a new snapshot/release ID. For a clean rehearsal,
  repeat section 2 to create another isolated root; no data deletion is needed.
- **Reject-rate error:** inspect the manifest and quarantine; fix the input and
  use a new ID. A failed snapshot is excluded from future builds.
- **dbt failure:** inspect the reported model/test. The normal build stops before
  publication. Do not bypass it with direct publication.
- **Warehouse missing:** ingestion alone creates no Gold tables. Run a successful
  `build-warehouse` and check that the dashboard uses the same root.
- **Port already in use:** choose another `dashboard --port`, such as 8052.
- **Stale dashboard:** stop and restart it after publication.
- **Wrong API instance/authentication:** check `SALEOR_URL` and token precedence.

Close the PowerShell session to discard its environment overrides, or restore
variables to their previous values. If you set them only for this guide, remove
them with `Remove-Item Env:ANALYTICS_ROOT` and likewise for `SALEOR_URL`,
`SALEOR_EMAIL`, and `SALEOR_PASSWORD` if set. The data files remain for inspection.

## Assessment mapping

File ingestion/normalization demonstrates B; manifests and layer separation A;
quarantine and dbt gates C; controlled replay and failure discussion D; this
runbook F. The CLI does not implement E's production approval mechanism.
The [assessment review](ASSESSMENT_REVIEW.md) remains the implementation-gap reference.

## Example verification

The file-based walkthrough was executed in an isolated data root on 2026-09-22:
baseline USD 50, updated USD 60, strict rejection, tolerant acceptance, and
`--no-publish` all behaved as described. Each of the four warehouse builds ran
dbt successfully. Checksums confirmed strict ingestion failure and candidate-only
building left the serving file unchanged. The Saleor bootstrap/API route was
documented from existing project behavior, not rerun for this guide.
