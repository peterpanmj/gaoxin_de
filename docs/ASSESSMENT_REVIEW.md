# Assessment coverage and implementation review

Reviewed on 2026-09-22 against `Principal_Data_Engineer_Candidate_Take_Home.md`,
sections A-F, and implementation baseline `1c86a14`. The original brief is the
acceptance reference; the agreed flexible implementation timeframe supersedes
its suggested 2-3 hours. All status statements below describe code, not promises
in earlier design documents. This review changes documentation and docstrings;
the listed implementation defects remain open.

## Verdict

**Partially meets the requirements; not yet submission-ready.** A substantial
Python component and a working source-to-warehouse happy path exist. However,
deduplication, malformed-input handling, publication safety, orchestration and
delivery evidence need work. The previous completion report overstated coverage.
The brief permits illustrative orchestration and CI, but those examples still
need correct dependencies and meaningful quality/promotion gates.

## A. Data Architecture and Design — substantially described, partly implemented

- **Layers and consumers:** Python/API ingestion lands raw and accepted JSONL in
  `var/bronze`; engineers use these for investigation and replay. dbt `orders`
  and `order_lines` are Silver current-state tables for analysts; daily Gold
  models serve reporting. Quarantine is for restricted engineering access.
- **Batch/incremental/streaming:** `extract.py` performs a full paginated scan;
  `build_candidate` replays all approved normalized files. Incremental/CDC and
  streaming are design extensions, not implemented modes. Assume a small
  synthetic source, no concurrent edits during extraction, and one local writer.
- **Schema evolution:** the manifest labels the contract `saleor-order-v1`.
  Unknown JSON fields are ignored; mandatory selected fields are validated.
  Version compatibility and schema migration are not enforced when replaying.
- **Late data/duplicates/reprocessing:** dbt ranks by source update time, so an
  older arriving version does not replace a newer version. Equal timestamps
  use lexical snapshot ID and payload ordering. Conflicts are not rejected.
  Accepted-history replay recomputes historical groups; it does not rerun raw
  validation, support as-of backfills or propagate source deletions.
- **Auditability:** raw SHA-256, counts and contract label appear in snapshot
  manifests; Silver preserves snapshot IDs. Candidate construction does not
  recheck hashes. Release metadata lacks the input inventory, pipeline version,
  test evidence and checksum needed for complete release-to-source lineage.
- **Security/cost:** API field selection omits customer identity and addresses;
  credentials come from the environment. Raw file ingestion retains arbitrary
  supplied content, including unknown fields: only use synthetic inputs. Local
  schemas/read-only connections are not authorization boundaries. TLS, encrypted
  storage, RBAC, retention and audit logging remain production design controls.
  Small local batches justify DuckDB; full replay and in-memory validation grow
  with retained history. Object storage/columnar files, partition pruning,
  compaction and lifecycle policies are future options, not measured savings.

Managed warehouse/object storage would reduce self-hosted operations at the cost
of service charges and platform dependence. Self-hosted Saleor/DuckDB makes the
interview reproducible. Streaming is unjustified for the proposed daily freshness
target; tighter latency would require new checkpoint and source consistency rules.
There is no measured performance or 4 GB all-services acceptance result.

Evidence: [design intent](../DEMO_DESIGN.md), [operational notes](OPERATIONS.md),
[extractor](../src/saleor_analytics/extract.py),
[storage](../src/saleor_analytics/pipeline.py), [dbt models](../analytics/models).

## B. Required Python Programming Assignment — partial

- **Runnable CLI:** `pyproject.toml` exposes `saleor-analytics`; `__main__.py`
  also supports `python -m saleor_analytics`. Nonsecret TOML and environment
  overrides avoid hard-coded data roots. Running from the checkout is supported.
- **Structured-file input:** `ingest-file` reads UTF-8 JSONL, one order per
  nonblank line. The API command writes JSONL before invoking this same boundary.
- **Validation/normalization:** `records.py` checks selected fields, aware
  timestamps, currencies, line IDs and quantities. It converts timestamps to
  UTC and money to two-decimal strings. Input text fields are often coerced rather
  than strictly type checked; blank strings containing spaces are not rejected.
- **Deduplication:** Python retains duplicate records. dbt selects one current
  order by `updated_at DESC, snapshot_id DESC, payload_json DESC`. This implements
  current-order selection but does not fulfill the agreed Python exact-duplicate
  step. Repeated orders within a single snapshot duplicate line rows; line
  uniqueness tests then block the normal build instead of resolving duplicates.
- **Curated output:** `build-warehouse` loads staging and invokes dbt for two
  Gold datasets. Python supplies substantial parsing/normalization/control;
  deduplication and aggregation are SQL responsibilities, not Python functions.
- **Output/errors:** Click prints counts, dbt output and paths. Known JSON and
  RecordError failures are quarantined. Other failures propagate as tracebacks.
  Nested `unitPrice: null`, non-finite amounts and malformed connection shapes
  are not reliably classified as row rejects. A crash can leave an incomplete
  directory that prevents reusing the same snapshot ID.
- **Maintainability:** responsibilities are split among config, API, extraction,
  records, storage, CLI and reporting. Type hints and injectable transport support
  tests. `run_dbt_build` depends on the repository-relative `analytics/` directory;
  the wheel only includes the Python package, so standalone deployment is incomplete.

Evidence: [CLI](../src/saleor_analytics/cli.py),
[normalization](../src/saleor_analytics/records.py),
[Python tests](../tests/test_pipeline.py), [package config](../pyproject.toml).

## C. Data Quality, Contracts, and Testing — partial

Implemented controls include required-field/timestamp checks, per-order distinct
line IDs, a configurable reject-rate gate, quarantine, and seven dbt data tests:
order ID nullability/uniqueness, accepted currencies (USD/EUR/PLN), line ID
nullability/uniqueness, order-line relationships and daily order reconciliation.
An empty file fails the reject-rate gate. A rate exactly at the limit passes.

Missing evidence/controls: strict nested types and finite decimals; accepted
statuses and timestamp ordering; same-version conflicts; Python duplicate counts;
hand-calculated aggregate oracles; multi-page extraction; removed-line/update
scenarios; failed-build preservation and retry recovery. Reconciliation compares
Gold with Silver, not independently with the source. Product totals and order
totals have different semantics and should not be assumed equal.

`build-warehouse` runs dbt before publication. **`publish-candidate` and
`publish_candidate()` do not enforce any quality evidence** and can publish
untested staging-only databases. Existing Python tests even exercise that path.
CI runs `dbt parse`, which does not execute these data tests.

Evidence: [dbt tests](../analytics/models/schema.yml),
[reconciliation](../analytics/tests/reconcile_daily_order_metrics.sql),
[publication](../src/saleor_analytics/pipeline.py), [CI](../.gitlab-ci.yml).

## D. Orchestration and Operational Design — partial, blocking DAG defect

The DAG sets a daily schedule, one active run and extraction retries, but the
actual dependency graph is `run_id -> extract` and `run_id -> transform_and_publish`.
There is **no extract-to-build edge**, so transformation may start before the
new snapshot exists. `max_active_runs=1` does not order tasks within a run.

Validation is folded into extraction, while modeling/tests/publication share a
task; monitoring has no task. The DAG was syntax-checked previously, not executed
in Airflow. Airflow is not a project dependency and there is no runnable local
Airflow deployment. Credentials, mounted config, dbt resources and working
directory must be supplied by a future runtime setup.

Transient source reads retry with bounded backoff. The DAG retries the entire
extract task, including deterministic validation failures. Retry IDs reuse
`ts_nodash`; existing or partial directories prevent safe resume. A logical date
does not constrain API extraction, so a historical trigger retrieves current
source state rather than historical data. Freshness alerts and OpenLineage are
proposed, not running. CLI invocations have no publication lock.

Evidence: [DAG](../dags/saleor_analytics.py), [operations](OPERATIONS.md).

## E. CI/CD and Delivery Practices — skeleton present, partial coverage

`.gitlab-ci.yml` has lint, Python test, dbt parse, package and manual default-branch
promotion stages. This meets the requested skeleton format, not an executable
delivery guarantee. It does not explicitly enable merge-request pipelines, check
formatting, execute fixture-backed dbt builds or promote a real artifact. Protected
environments/approval policies are not established by the current YAML. Production
promotion is an echo placeholder. Runtime secrets must come from protected CI
variables or a secret manager, and separate environments need separate roots/config.

The repository is hosted on GitHub; GitLab YAML does not run there. The agreed
GitHub Actions workflow is missing. Before promotion, require formatting/lint,
unit and contract tests, fixture-backed dbt/reconciliation tests, failed-release
preservation, package installation and an explicit protected approval gate.
Promote the tested artifact rather than rebuilding it. Keep production secrets
out of untrusted PR/MR jobs.

Evidence: [CI skeleton](../.gitlab-ci.yml), [package definition](../pyproject.toml).

## F. README and Design Explanation — covered with this review's qualifications

The README provides setup, structure, data flow, validation, assumptions,
security and cost discussion. This review and revised CLI docstrings add the
missing implementation traceability and qualify production claims. Earlier
design/plan content is intention, not proof of completed work. See the repair
priorities below for the production-readiness roadmap.

## CLI walkthrough and analytical semantics

Use new IDs on every invocation; existing IDs currently fail instead of resuming.
These commands assume the repository root as the working directory:

```powershell
uv run saleor-analytics --help
uv run saleor-analytics ingest-file --help
uv run saleor-analytics build-warehouse --help
uv run saleor-analytics --config config/local.toml doctor
uv run saleor-analytics ingest-file INPUT.jsonl --snapshot-id review-001
uv run saleor-analytics build-warehouse --release-id review-001 --no-publish
```

`INPUT.jsonl` is a placeholder for a synthetic Saleor order export. The committed
`order()` helper in `tests/test_pipeline.py` is a representative fixture; a
standalone committed sample file and scenario generator remain missing.

`ingest-file` demonstrates B's file parsing, normalization and errors plus C's
quarantine. `extract-saleor` adds A/D's paginated operational source boundary.
`build-warehouse` supplies B's curated output through supporting dbt and C's data
tests; `--no-publish` retains an inspected candidate. Ordinary `build-warehouse`
publishes after its dbt run. Direct `publish-candidate` is a low-level unchecked
copy, not an E approval mechanism. `dashboard` demonstrates A's consumption layer.

Silver grains are one row per order and per line. Daily order metrics group by
UTC creation date, channel and currency: order count, sum of source gross order
total, and average gross order total. Daily product metrics additionally group
by SKU/name and sum quantities and source gross line totals. Both exclude
`CANCELED` and `DRAFT`. These are order-value measures, not recognized revenue,
net sales, payments or refunds; order totals may include shipping/discount effects.

**Reporting defect:** Gold separates currencies, but Dash's headline total and
product chart sum them together. The product chart also ignores selected channels;
empty channel selection means all channels. Data loads only at server startup.
Until corrected, use Gold SQL grouped by currency for analytical evidence, not
the combined monetary KPI. Publication/quality metadata is not displayed.

## Agreed demo scope beyond the original brief

The controlled Saleor API seed/update/cancel scenarios, troublesome-data CLI,
optional parameter-controlled mock generation inside Airflow, GitHub Actions,
publication manifest and dashboard release/quality display are not implemented.
Official `populatedb` is a one-time bootstrap, not those requested scenarios.
`allow_mock` is currently unused. These remain outstanding commitments rather
than new requirements imposed by this review.

## Repair order before submission

1. Enforce a validated publication boundary, correct the DAG dependency, and
   separate/filter dashboard monetary values by currency and channel.
2. Add Python exact-duplicate handling; strict nested/decimal validation;
   conflict detection; order-version/line-set consistency; and tests proving
   retry, late-update, removed-line and failed-publication behavior.
3. Add durable release provenance, concurrency control and safe resume/replay.
   Preserve old serving data on every failure and verify Windows file behavior.
4. Deliver the agreed synthetic scenarios, optional DAG generation, executable
   runtime setup and GitHub CI with data-quality and package-install gates.
5. Rehearse a clean checkout and update all acceptance evidence. Production-only
   TLS/RBAC/retention/catalog and scale work can remain explicitly documented.

## Verification scope

Prior build evidence: 20 source orders extracted; six dbt models and seven data
tests passed on that dataset; seven Python tests passed; Dash returned HTTP 200.
These establish a happy path, not correctness of untested failure scenarios or
the Dash callbacks. This audit uses source inspection and focused local probes;
it does not execute source mutations, publish a new warehouse, deploy Airflow,
or validate GitLab/GitHub promotion. Current-check results are reported alongside
this review rather than treating historical runs as new verification.

Current audit results:

- Existing Python suite: **7 passed**. This is a narrow regression suite, not
  evidence that all the failure modes above are handled.
- All seven CLI help pages (group plus six commands) exit successfully and
  display their assessment mapping.
- Isolated synthetic probes confirm: a null unit price raises AttributeError;
  boolean quantity is accepted; two identical orders remain two accepted rows;
  and direct publication accepts a database containing only staging tables.
- AST comparison confirms all eight edited Python files have identical executable
  logic after excluding docstrings. No implementation fixes are claimed here.
- Ruff lint/format and Git whitespace checks pass after docstring formatting.

The probes used disposable files beneath `var/`; they did not change Saleor or
the serving warehouse. dbt models were unchanged and not rebuilt in this audit.
