# CLI user guide (Git Bash)

Run these commands in Git Bash from the repository root. Set `REPO_ROOT` to
your own clone location, then enter it. The Click entry point is
`uv run saleor-analytics`. Use `--help` on any command for its assessment
mapping and parameters. Global `--config PATH` comes before the command.

## Environment setup

Install Python 3.12, uv, and Docker Desktop, and start Docker Desktop. Check
that Git Bash can find each tool:

```bash
export REPO_ROOT="/d/path/to/<repository-folder>"
cd "$REPO_ROOT"
python --version
uv --version
docker version
docker compose version
```

Install the locked Python dependencies and verify the CLI:

```bash
uv sync --frozen
uv run saleor-analytics doctor
```

Local JSONL ingestion, warehouse builds, and Dash run on the host and do not
require Docker. To run the Airflow DAG, build its image once and start Compose
from the repository root:

```bash
docker compose -f infra/airflow/compose.yml build
docker compose -f infra/airflow/compose.yml up -d
docker compose -f infra/airflow/compose.yml ps
```

Open `http://localhost:8081` for Airflow. The Compose service stores pipeline
data in its `analytics-data` Docker volume and maps the repository's
`artifacts/` directory into the container. A local `ANALYTICS_ROOT` under
`var/` is separate from that Docker volume. To stop Airflow while keeping the
volume, run:

```bash
docker compose -f infra/airflow/compose.yml stop
```

If package downloads during image build require the local proxy, see the
[network setup](MODERN_DE_DEMO.md#docker-pulls-and-image-builds). Docker
Desktop's image-pull proxy is configured separately.

## Python command runner

`uv` is Astral's Python project and package manager, not a Click command. It
creates the project's `.venv`, installs the exact dependency versions recorded
in `uv.lock`, and runs commands in that environment.

`uv run saleor-analytics doctor` means: use this repository's managed Python
environment, start the installed `saleor-analytics` executable, then pass
`doctor` to the Click CLI. The execution path is:

```text
uv run -> saleor-analytics executable -> Click CLI -> Python pipeline modules
```

Use `uv run` because it reliably selects this project's environment.

## End-to-end demo flow

```text
Choose one source path for a run

  A. Repeatable local demo                         B. Live Saleor demo
     mock-data OUTPUT                                Saleor application
         |                                              | GraphQL orders query
         v                                              v
     base.jsonl                                     extract-saleor --mode full
         | ingest-file --snapshot-id base                | (later: incremental)
         |                                              | captures orders.jsonl
         |                                              |
         +---------------------+------------------------+
                               |
                               v
                    Validate and normalize orders
                    - timestamps, money, strings, lines
                    - collapse exact duplicates
                    - quarantine invalid rows; fail on conflicting versions
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
       bronze/<snapshot-id>/             quarantine/<snapshot-id>/
       raw JSONL, accepted JSONL,        rejected JSONL (if any)
       manifest and checksums
                 |
                 v
          build-warehouse --release-id ID
          (Airflow runs these stages separately)
                 |
                 +--> stage all accepted history in a candidate DuckDB file
                 +--> dbt builds Silver orders/lines and Gold daily metrics
                 +--> dbt tests contracts and reconciles metrics
                 +--> publish only after validation succeeds
                               |
                               v
                releases/<release-id>/analytics.duckdb
                releases/<release-id>/release.json + dbt evidence
                               |
                               v
                    warehouse/current.json
                    (points to the active release)
                               |
                 +-------------+----------------+
                 |                              |
                 v                              v
       status --max-age-hours 24              dashboard
       (daily DAG monitor task)                 (Dash)

Manual review action, outside the daily DAG:

  warehouse/current.json + active release
                  |
                  v
          export-artifacts DESTINATION
                  |
                  v
  artifacts/<review-name>/<release-id>/
  portable DuckDB, dbt, Bronze, and quarantine evidence
```

`mock-data` writes the JSONL that `ingest-file` reads. For the live path,
`extract-saleor` reads Saleor through GraphQL and captures its response before
the same validation path; this project does not query Saleor's PostgreSQL
database. The mock and Saleor paths use separate data roots during the demo.
If validation or dbt tests fail, `warehouse/current.json` keeps pointing to
the previous successful release.

The daily Airflow DAG wraps the production steps as
`prepare → extract_and_validate → stage → transform_and_test → publish → monitor`.
Scheduled runs use Saleor extraction by default; a manual trigger can opt into
mock data. `export-artifacts` is intentionally outside this DAG because it
copies an already-published release only for review, audit, or the demo.

## What each layer produces

`ANALYTICS_ROOT` is the data root selected for the run. A `snapshot-id` names
one input capture; a `release-id` names one tested warehouse version, which
can incorporate several accepted snapshots. The walkthrough happens to use
`base` for both IDs, but they identify different artifacts.

| Layer | Result under `ANALYTICS_ROOT` | What it means |
|---|---|---|
| Bronze | `bronze/<snapshot-id>/orders.raw.jsonl`, `orders.accepted.jsonl`, `manifest.json` | Retains the source payload, the normalized accepted rows, and counts/checksums for that ingestion. It is the replay and audit evidence. |
| Quarantine | `quarantine/<snapshot-id>/orders.rejected.jsonl` | Holds rows that failed the input contract, with rejection details. It can be empty when all rows pass. Excessive rejection fails the ingestion quality gate. |
| Silver | `orders` and `order_lines` tables inside `releases/<release-id>/analytics.duckdb` | Represents the latest valid version of each order and its matching current line set, with controlled fields and types. This is current state, not one row per historical version. |
| Gold | `daily_order_metrics` and `daily_product_metrics` tables in the same DuckDB file | Aggregates eligible orders by date, channel, and currency; product metrics also group by SKU/name. Dash reads these tables. |
| Published release | `releases/<release-id>/release.json`, dbt evidence, and `warehouse/current.json` | Records the tested release and points readers to the active one. A failed validation leaves the previous pointer in place. |

For an Airflow run, Compose mounts its named `analytics-data` volume at
`/opt/data` and sets `ANALYTICS_ROOT=/opt/data`. The paths in the table are
relative to that mount. For example:

```text
analytics-data volume (mounted at /opt/data)
  bronze/<snapshot-id>/orders.raw.jsonl
  bronze/<snapshot-id>/orders.accepted.jsonl
  bronze/<snapshot-id>/manifest.json
  quarantine/<snapshot-id>/orders.rejected.jsonl
  releases/<release-id>/analytics.duckdb  (Silver and Gold tables)
  releases/<release-id>/release.json
  releases/<release-id>/dbt/run_results.json
  warehouse/current.json                  (active release pointer)
```

From Git Bash at the repository root, inspect the volume through the running
container:

```bash
docker compose -f infra/airflow/compose.yml exec -T airflow ls /opt/data/bronze
docker compose -f infra/airflow/compose.yml exec -T airflow ls /opt/data/releases
docker compose -f infra/airflow/compose.yml exec -T airflow cat /opt/data/warehouse/current.json
```

Docker Desktop manages this named volume; it is not the host's `var/` folder.
Host CLI runs use the `ANALYTICS_ROOT` you set in Git Bash. An explicit
`export-artifacts` command copies selected evidence to the separate host
`artifacts/` directory, which Compose mounts at `/opt/artifacts`.

DuckDB is the physical warehouse file containing the staging, Silver, and Gold
tables. Silver and Gold are logical model layers inside that file, not separate
Parquet directories. `orders.raw.jsonl` remains the original source shape;
`orders.accepted.jsonl` is the normalized contract used to rebuild the
warehouse. The quarantine file is retained for inspection, not loaded into
Silver or Gold.

After the baseline walkthrough below, expect 20 accepted Bronze orders,
20 current Silver orders, and USD400 in the Gold daily order metric. Use
`uv run saleor-analytics status` to see the active release and its quality
counts, then Dash to inspect Gold. The later `invalid --count 2` example
creates a rejected row and fails its quality gate, so those results do not
replace the published baseline or update release.

## Mock-data ingestion walkthrough

Use a fresh root so experiments cannot alter an earlier demonstration:

```bash
export ANALYTICS_ROOT="$(pwd -W)/var/guide-$(date +%Y%m%d-%H%M%S)"
uv run saleor-analytics doctor
uv run saleor-analytics mock-data "$ANALYTICS_ROOT/base.jsonl" --count 20
uv run saleor-analytics ingest-file "$ANALYTICS_ROOT/base.jsonl" --snapshot-id base
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

```bash
uv run saleor-analytics mock-data "$ANALYTICS_ROOT/duplicate.jsonl" --scenario duplicate --count 20
uv run saleor-analytics ingest-file "$ANALYTICS_ROOT/duplicate.jsonl" --snapshot-id duplicate
uv run saleor-analytics build-warehouse --release-id duplicate
```

The manifest reports 21 input rows, 20 accepted versions and one duplicate.
Gold still contains 20 orders totaling USD400. Accepted counts describe historical
versions; they are not the current business order count.

### Apply updated source state

```bash
uv run saleor-analytics mock-data "$ANALYTICS_ROOT/update.jsonl" --scenario update --count 20
uv run saleor-analytics ingest-file "$ANALYTICS_ROOT/update.jsonl" --snapshot-id update
uv run saleor-analytics build-warehouse --release-id update
```

One order becomes USD30; 20 orders now total USD410. The newer version replaces
the complete current line set. Older arrivals cannot roll that order backward.

### Demonstrate quarantine and a failed batch

```bash
uv run saleor-analytics mock-data "$ANALYTICS_ROOT/invalid.jsonl" --scenario invalid --count 2
uv run saleor-analytics ingest-file "$ANALYTICS_ROOT/invalid.jsonl" --snapshot-id invalid
```

The ingestion command is expected to fail. Inspect the rejected row with:

```bash
cat "$ANALYTICS_ROOT/quarantine/invalid/orders.rejected.jsonl"
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

After the `base` release in the walkthrough is published, copy it and its
latest source snapshot to a dated review directory outside `ANALYTICS_ROOT`:

```bash
REVIEW_ROOT="$(pwd -W)/artifacts/review-$(date +%Y%m%d-%H%M%S)"
uv run saleor-analytics export-artifacts "$REVIEW_ROOT"
find "$REVIEW_ROOT" -maxdepth 5 -type f
```

For example, if `REVIEW_ROOT` resolves to
`<repository-root>/artifacts/review-20260926-210000`, the `base` bundle is
created under `<repository-root>/artifacts/review-20260926-210000/base/`. It
contains:

```text
base/
  export.json
  warehouse/current.json
  releases/base/analytics.duckdb
  releases/base/release.json
  releases/base/dbt/run_results.json
  bronze/base/orders.raw.jsonl
  bronze/base/orders.accepted.jsonl
  bronze/base/manifest.json
  quarantine/base/orders.rejected.jsonl
```

The command never overwrites an existing bundle. With no `DESTINATION`, it
uses `artifacts/` and creates `artifacts/<release-id>/`. To export a specific
historical or failed candidate, supply `--release-id RELEASE_ID`; a non-active
release does not include `warehouse/current.json`. Add `--all-snapshots` when
a reviewer needs complete source lineage. The DuckDB file contains the full
release state; the default bundle's latest Bronze snapshot alone cannot
rebuild it.

For data produced by Airflow, Compose maps the repository's `artifacts/`
directory to `/opt/artifacts` in the container. From the repository root,
use a destination visible on both sides of that mapping:

```bash
REVIEW_NAME="review-$(date +%Y%m%d-%H%M%S)"
docker compose -f infra/airflow/compose.yml exec airflow \
  /opt/analytics/bin/saleor-analytics export-artifacts "/opt/artifacts/$REVIEW_NAME"
find "artifacts/$REVIEW_NAME" -maxdepth 5 -type f
```

The host bundle is `artifacts/$REVIEW_NAME/<active-release-id>/`.

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
