# Verification record: PySpark branch

Updated 2026-10-25 for `codex/pyspark-analytics`. The current analytical path is
PySpark to Parquet, with pandas/PyArrow serving Gold in Dash. DuckDB/dbt results
from the earlier implementation do not validate this branch.

## Observed results and pending work

| Check | Evidence / status | What it establishes |
|---|---|---|
| Native Windows Python/PySpark suite | 23 passed in 73.71 seconds after configuring Java 17 and matching Windows Hadoop helpers. | Real Spark staging/builds and Python failure/recovery tests ran on that configured host. This is not a container test result. |
| Linux GitHub Actions | [Run 40169814175](https://github.com/peterpanmj/gaoxin_de/actions/runs/40169814175) succeeded for commit `f428a08`. | Formatting, lint, full pytest suite, and package build passed directly on Ubuntu with Java 17. This predates the local container-test additions. |
| Linux test container | Image built successfully; the full suite passed: **23 passed in 65.86 seconds**, exit code 0. | Real PySpark processing and failure/recovery tests executed inside Linux on the Windows Docker host. |
| Container format/lint | Ruff format checked 18 files; Ruff lint passed, both inside the test image. | The documented test/lint runner works with its installed tools. |
| Configuration/documentation checks | Compose parsing, test collection (23 tests), relative documentation links, lock-version consistency, and Git whitespace checks pass. | Supporting static checks, separate from runtime execution. |
| Spark-enabled Airflow image | Runtime image rebuilt successfully from the same verified application/Java base. | Image build verified; actual Airflow scheduler/DAG execution still needs its own rehearsal. |
| Runtime image CLI baseline | `mock-data` -> `ingest-file` -> `build-warehouse` -> `status` succeeded in a disposable runtime container. Gold assertions verified **20 orders and USD400**, with all **11 dataset checks** passing. | The installed Click CLI and PySpark publication path work in the Airflow runtime image without host dependencies or demo-volume mounts. |
| Live Saleor and dashboard rehearsal | Repeat on the Spark-enabled runtime before presenting. | Unit/integration fixtures do not establish API connectivity, scheduler health, or browser availability. |
| Full stack under 4 GB RAM | Not acceptance-tested. | No resource-limit guarantee is claimed. |

## Reproduce the recommended tests

Start Docker Desktop with Linux containers and run from the repository root.
Host Python/Java/uv and Windows Hadoop helpers are unnecessary for this route.
Build successfully before running; rebuild after changing code, tests, or locks.

PowerShell:

```powershell
docker compose -f infra/airflow/compose.test.yml build tests
if ($LASTEXITCODE -ne 0) { throw 'Test image build failed.' }
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests
if ($LASTEXITCODE -ne 0) { throw 'Container tests failed.' }
```

Git Bash / Bash:

```bash
docker compose -f infra/airflow/compose.test.yml build tests &&
  docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests
```

Expected for the current suite: `23 passed`, exit code `0`, matching the local
container run recorded above. See [TESTING.md](TESTING.md) for focused tests,
lint checks, and isolation.
The disposable runner mounts no presentation data and starts no Airflow service.

## Local container environment and build details

Verified on 2026-10-25 with Docker Desktop's Linux engine on a Windows host:

- Container platform: Linux x86_64 (`linux/amd64`).
- Python 3.12.9, Eclipse Temurin Java 17.0.21+9, PySpark 4.2.0, PyArrow 24.0.0,
  pytest 9.1.1, and Ruff 0.16.8.
- The running test container was inspected: `Mounts=[]`. It used disposable
  temporary data, with no host directories or presentation named volumes mounted.
- Docker had about 8 GB available; this is not a 4 GB acceptance result.

To avoid the slow network, this build used the optional ignored
`var/wheelhouse/` cache: the existing PySpark universal wheel, Linux/universal
dependency wheels verified against `uv.lock`, build-backend tools, and a
Java 17 Linux JRE checked against its vendor-published SHA-256. The build used
`PYTHON_WHEELHOUSE=../../var/wheelhouse` and `--build-arg PIP_NO_INDEX=1`.
See [cache setup](TESTING.md#reuse-manually-downloaded-packages-optional).
The empty-cache Debian/network download path was not completed during this run.

The first test run exposed missing Linux executable permissions in the
Windows-built PySpark wheel. The shared Docker build now restores executable
bits on Unix Spark launcher scripts. The complete suite was rebuilt and rerun
successfully after that fix; no tests were skipped to obtain the passing result.

Local logs remain ignored under `var/`: `container-test-build.log`,
`container-pytest-results.log`, `container-runtime-versions.log`, and
`container-runtime-build.log`, and `container-runtime-cli-results.log`. They are
runtime evidence, not tracked source. The CLI rehearsal used `/tmp` inside a
disposable container and did not start Airflow or modify existing demo volumes.

## Evidence produced by a normal pipeline build

Paths below are relative to `ANALYTICS_ROOT`, normally `/opt/data` in Airflow:

| Artifact | Purpose |
|---|---|
| `bronze/<snapshot-id>/orders.raw.jsonl` | Source records retained for investigation and replay. |
| `bronze/<snapshot-id>/orders.accepted.jsonl` and `manifest.json` | Normalized accepted contract, counts, and input checksums. |
| `quarantine/<snapshot-id>/orders.rejected.jsonl` | Invalid records and rejection reasons. |
| `releases/<release-id>/dataset/staging/order_versions/` | Spark staging Parquet from approved source history. |
| `releases/<release-id>/dataset/silver/{orders,order_lines}/` | Current validated order and line Parquet datasets. |
| `releases/<release-id>/dataset/gold/{daily_order_metrics,daily_product_metrics}/` | Curated daily reporting Parquet datasets. |
| `releases/<release-id>/quality/results.json` | Named results of the 11 PySpark data quality checks. |
| `releases/<release-id>/release.json` | Inputs, model/data/results hashes, validation status, counts, and timestamps. |
| `warehouse/current.json` | Active release, dataset checksum, quality summary, and committed API watermarks. |

Pytest results appear in the terminal. Pipeline dataset-check results are
separate persisted JSON evidence; there is no dbt `target/run_results.json` in
this branch. A failed candidate retains its available diagnostic artifacts and
cannot replace the current release.

## Boundaries and historical evidence

The Spark session is local, not a distributed-cluster benchmark. Full-history
rebuilds, file locks, and a single active DAG run suit the small synthetic demo.
Log CDC, hard-delete capture, SCD2, cloud IAM, external alert delivery, production
deployment, and source completeness guarantees remain outside implemented scope.

The [modern demo guide](MODERN_DE_DEMO.md) covers operational rehearsal. The
[previous verification report](https://github.com/peterpanmj/gaoxin_de/blob/0f036f8/docs/VERIFICATION.md)
is retained in Git history for the earlier DuckDB/dbt implementation. Its
18-test count, SQL model results, old image runtime evidence, and retired
benchmark do not describe the current branch.
