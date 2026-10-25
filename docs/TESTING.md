# Run tests in the Linux container

Use the disposable Docker test runner for this PySpark branch. Run the commands
from the repository root in PowerShell, Git Bash, or a Linux/macOS terminal.
Docker Desktop must be running with **Linux containers** on Windows. Host Python,
Java, uv, `JAVA_HOME`, `HADOOP_HOME`, and Windows `winutils.exe` are not required
for this test workflow; the image supplies Python 3.12, Java 17, and PySpark.
The Compose services select `linux/amd64`; other CPU architectures need Docker
support for running that platform, potentially through emulation.
Use Docker Compose 2.17 or later with BuildKit support for the named build cache
context; the local verification environment uses Compose 2.27.

## 1. Build the test image

PowerShell:

```powershell
docker compose -f infra/airflow/compose.test.yml build tests
```

Git Bash / Bash:

```bash
docker compose -f infra/airflow/compose.test.yml build tests
```

The first build downloads the Airflow base image, Java, the large PySpark
distribution, and Python dependencies. Build it before the presentation, with
working network access and enough Docker memory and disk space. Test execution
does not require a live Saleor API. If a build needs a proxy, use the existing
[Docker build network guidance](MODERN_DE_DEMO.md#docker-pulls-and-image-builds).

The `test` target in `infra/airflow/Dockerfile` shares its application/runtime
base with the Airflow `runtime` target. It adds the locked development tools
from `requirements-test.lock`, the tests, and DAG source files. The regular
Airflow image does not include those extras. Do not run pytest in an older
running Airflow container and assume it contains this test environment.
The test target also bypasses Airflow's startup entrypoint, so running tests
does not initialize an Airflow metadata database or wait for its services.

### Reuse manually downloaded packages (optional)

If the large PySpark download is already available, put its compatible wheel
or source archive and any Linux/universal dependency wheels in the ignored
`var/wheelhouse/` directory. Set a build context path before building:

PowerShell:

```powershell
$env:PYTHON_WHEELHOUSE = '../../var/wheelhouse'
docker compose -f infra/airflow/compose.test.yml build tests
```

Git Bash / Bash:

```bash
export PYTHON_WHEELHOUSE='../../var/wheelhouse'
docker compose -f infra/airflow/compose.test.yml build tests
```

The path is relative to `infra/airflow/`, where the Compose file lives. Leave
the variable unset to use the default empty `infra/airflow/wheelhouse/` and
download packages normally. Both test and Airflow runtime builds support it.
Use artifacts matching the pinned versions from `uv.lock`; Linux containers
cannot install Windows-specific wheels. Pure Python/universal wheels such as
PySpark's `py2.py3-none-any` wheel can be reused across the two systems.
Windows-built PySpark wheels can lose Unix launcher executable bits; the
shared Docker build restores them before either image target is produced.

This cache is a read-only build input, not a runtime data volume or a replacement
for the lockfile. Pip still downloads missing dependencies and build tools;
Java/OS packages also need network access on an uncached first build unless a
cached Java runtime is provided. Local package files are ignored by Git and are
not copied into the final image as a download directory.

For slow OS-package downloads, the same cache can optionally contain a trusted
Eclipse Temurin Java 17 **Linux x64 JRE** archive renamed to
`java17-linux-x64.tar.gz`, plus `java17-linux-x64.tar.gz.sha256`. The checksum
file must contain the vendor-published SHA-256, two spaces, and that archive
filename. The build verifies it before extracting the runtime into `/opt/java`.
Obtain both from the [official Adoptium release metadata](https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=x64&image_type=jre&os=linux&vendor=eclipse).
Do not use a Windows Java archive. Without the cached archive, the image
installs OpenJDK 17 from Debian's package repositories instead.

When the cache contains **all** runtime/test wheels and the application build
backend (`hatchling` and its dependencies), you can prevent pip from consulting
PyPI. With `PYTHON_WHEELHOUSE` set as above, both shells accept:

```bash
docker compose -f infra/airflow/compose.test.yml build --build-arg PIP_NO_INDEX=1 tests
```

Missing packages then fail immediately instead of being downloaded. The base
image and Java must also be cached for a fully offline build. Leave this build
argument at its default when the cache is incomplete.

## 2. Run the complete suite

PowerShell:

```powershell
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests
```

Git Bash / Bash:

```bash
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests
```

This starts a new container, runs the full `python -m pytest -q` suite, returns
its exit code, and removes the container. A successful run ends with
`23 passed` for the current suite and exit code `0`. Failed tests produce a
nonzero exit code; Spark warnings alone do not mean a test failed. The console
shows the result; this command does not create a persistent report on the host.

The standalone test Compose file starts no Airflow scheduler, web UI, or Saleor
service, opens no host ports, and mounts no host directories or named volumes.
Tests use synthetic fixtures and isolated pytest temporary directories under
`/tmp/pytest`. They do not read or modify the presentation's `/opt/data` volume
or host `var/` artifacts. Container removal discards the temporary test data.
Stop unnecessary demo services first if Docker memory is constrained; a 4 GB
full-stack run has not been acceptance-tested.

## 3. Rerun after changes

Source, samples, and tests are copied into the image rather than bind-mounted.
Rebuild after changing them or either dependency lockfile, then run again:

```bash
docker compose -f infra/airflow/compose.test.yml build tests
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests
```

These two commands also work unchanged in PowerShell. Continue to the run only
if the build succeeds; do not accidentally test a previously built image after
a failed rebuild. Docker reuses unchanged build layers. Once the image is built,
repeated runs do not reinstall dependencies or need to download PySpark again.

For a focused investigation, override the default command. Both shells accept:

```bash
# API errors and retry behavior, without a live Saleor service.
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests python -m pytest -q tests/test_api.py

# Real Spark transformation and publication/recovery behavior, plus other reliability checks.
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests python -m pytest -q tests/test_reliability.py

# List the tests without executing them.
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests python -m pytest --collect-only -q

# Formatting and lint checks in the same environment.
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests ruff format --check src tests dags
docker compose -f infra/airflow/compose.test.yml run --rm --no-deps tests ruff check src tests dags
```

These commands avoid absolute container-path arguments, so Git Bash does not
need an `MSYS_NO_PATHCONV` override. Run the complete suite before review even
when a focused test passes. `--no-deps` prevents starting dependency services;
`--rm` removes the finished test container, and neither option deletes demo
volumes.

## What is tested

| Coverage | Evidence |
|---|---|
| Click interface and configuration | CLI help and invalid settings. |
| Source and ingestion contracts | GraphQL errors/retries, malformed types, invalid records, quarantine, duplicates, and conflict handling. |
| Mock scenarios | Explicit generation controls, deterministic outputs, updates, and dated data. |
| Real PySpark processing | Parquet staging, current order/line selection, Gold totals, and publication quality gates. |
| Recovery and lineage | Replay, failed publication, unchanged active pointer, source checkpoints/backfill behavior, and artifact export. |

The Python suite includes real local Spark jobs; it is not just mocked DataFrame
processing. The 11 dataset quality checks executed by pipeline builds also
remain part of the daily DAG's `transform_and_test` step. Pytest is a developer
verification command and is not an additional daily DAG task. This suite does
not prove live Saleor connectivity, browser availability, scheduler behavior,
or multi-node Spark scaling; rehearse those demo services separately.

## CI and native execution

GitHub Actions currently runs the same suite directly on an Ubuntu runner with
Java 17 and dependencies installed through `uv sync --frozen`. It also checks
formatting/lint and builds a Python package. That is Linux verification, not
evidence that this Docker image was built or exercised by CI.

Native host execution is optional for developers who deliberately maintain a
working Python/Java/Hadoop environment. It is not the recommended test setup,
especially on Windows. Container testing avoids Windows-specific Java/Hadoop
path configuration, while still requiring a healthy Docker engine and enough
resources for Spark.

If dependencies change, regenerate the test tools lock from `uv.lock` on a
development machine with uv, then rebuild:

```bash
uv export --frozen --only-group dev --no-emit-project --no-hashes --output-file infra/airflow/requirements-test.lock
```
