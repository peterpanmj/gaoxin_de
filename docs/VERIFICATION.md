# Verification record: 2026-09-22

The original assessment requests a representative production-minded pattern,
not a fully productionized platform. The evidence below is local execution;
it does not claim that remote CI, approval policies or a production deployment
have succeeded.

| Check | Observed result | Requirements |
|---|---|---|
| Python suite | 18 passing tests, including malformed values, duplicates, retry and conflict handling | B/C |
| Real dbt integration | Six models and ten data tests; removed lines, failed publication, stale candidates and conflict exclusion/recovery exercised | A/B/C/D |
| Live Saleor API | Full extraction accepted 20 orders; subsequent bounded incremental extraction accepted a valid empty delta; both releases passed ten dbt tests | A/B/D |
| Dash | HTTP page/layout responses passed; callback read live release delta-001 and produced 11 USD orders totaling 2,846.16 in default-channel | A/F |
| Airflow image | Built successfully with isolated CLI/dbt virtual environment and packaged dbt resources | D/E |
| Airflow DAG runtime | Manual duplicate generation and generation-disabled fixture runs completed all six tasks | C/D |
| Airflow failure | Invalid quantity in one of two rows failed ingestion; publish was upstream_failed; previous pointer unchanged | C/D |
| Parquet experiment | 120,000 rows; JSON/Parquet month-1 results both 10,000 rows and 490,000.00; plan scanned 1/12 files | A/F |
| Delivery | Ruff formatting/lint passed; wheel and source distribution built | E |

## Reproduce

```powershell
uv run ruff format --check src tests dags
uv run ruff check src tests dags
uv run pytest -q
uv build
docker compose -f infra/airflow/compose.yml build
docker compose -f infra/airflow/compose.yml up -d
docker compose -f infra/airflow/compose.yml cp infra/airflow/smoke.py airflow:/tmp/smoke.py
docker compose -f infra/airflow/compose.yml exec -T airflow python /tmp/smoke.py
```

Wait until standalone initialization completes before the smoke command. It
uses an isolated named-volume subdirectory and unique manual run dates. It runs
actual DAG tasks via `dag.test`; task retries are disabled only in the smoke
harness to avoid delaying expected failure. This verifies dependencies and task
behavior, not scheduler throughput or high availability. The invalid scenario
prints an expected task traceback; the script succeeds only when failure and
unchanged-publication assertions pass. No Saleor mutations are required.

The executable [modern guide](MODERN_DE_DEMO.md) reproduces API polling, mock
updates and quality gates. Local logs, dbt artifacts and databases remain under
ignored runtime directories or Docker volumes. Committed tests and scripts are
the reproducible evidence; no secrets or raw real-world records are included.

## Boundaries

No log-based CDC, hard-delete capture, SQL incremental MERGE, SCD2, Spark cluster,
cloud IAM deployment or source completeness guarantee is claimed. The main
warehouse rebuilds accepted history; Parquet export is not implemented. Mock
generation writes local JSONL, not Saleor mutations. External alerts, repository
approval settings, production deployment and a full-stack 4 GB acceptance run
remain documented follow-on work.
