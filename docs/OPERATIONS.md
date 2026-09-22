# Operational design

## Execution and recovery

Airflow runs `prepare -> extract_and_validate -> stage -> transform_and_test ->
publish -> monitor`, with one active run and optional manual-only mock generation.
The CLI can run independently. Local root/release locks serialize writes; a
reader opens one immutable release. A pointer replacement commits data and the
source watermark together. Validation, altered evidence and stale-base guards
apply to direct publication as well as the combined build command.

A retry reuses its snapshot/release ID and persisted extraction window. Different
content needs a new ID. Failed snapshots stay on disk but are not staged.
Cross-snapshot conflicts fail dbt; an unpublished input can be excluded with an
audited reason before corrected input is ingested. Published history cannot be
silently excluded. Failed publication can be retried without rebuilding valid
unchanged data. Monitoring failure occurs after publication and does not undo it.

See [the executable runbook](MODERN_DE_DEMO.md) for commands, Airflow setup,
parameter examples and failure injection. Source mutations through Saleor API
are not automated: bootstrap Saleor with its upstream tools or use local mock
JSONL. A historical Airflow logical date does not reconstruct historical source
state. Backfill means replaying retained accepted history into a new release.

## Freshness, observability and lineage

Bronze manifests retain raw/accepted checksums, contract version, record/rejection/
duplicate counts, duration and extraction bounds. Releases retain the complete
input inventory, model checksums, dbt manifest/results/logs and database checksum.
Silver retains snapshot ID and canonical payload hash. Trace Gold through its
grouping keys and dbt dependencies to the selected Silver versions.

The dashboard refreshes every 30 seconds and shows release, publication age and
quality counts. `status --max-age-hours 24` exits nonzero for stale publication
or extraction. Airflow exposes task failure; external alert delivery is not
configured. Daily 06:00 UTC execution is configured; the proposed 06:30 UTC
freshness objective is not an enforced end-to-end SLO. No completeness or volume
anomaly monitor is claimed. Empty valid deltas can advance a checkpoint.

## Incremental processing and storage

Polling uses Saleor updatedAt bounds with five-minute overlap and an upper bound
five seconds behind the invocation clock. Windows persist across retries; only
publication advances committed progress. This assumes synchronized clocks and
mostly quiet synthetic data. Hard deletes, changes delayed beyond overlap and
consistent source pagination require stronger production reconciliation/CDC.

The warehouse rebuilds retained accepted history. This simplifies replay and
current-state correctness for the demo. It is not SQL MERGE, SCD2 or distributed
processing. A separate benchmark compares equivalent JSON and Parquet queries
and exposes monthly partition pruning. Production scale would motivate object
storage, partition-aware incremental transformation and workload isolation.

## Security, governance and cost

Only synthetic data is allowed in this local demo. API projection omits customer
identity and addresses, but arbitrary file input is preserved verbatim. Keep
Bronze/quarantine access separate from Gold consumers. Runtime environment
variables supply source credentials. Saleor development defaults are public
local defaults, not deployable credentials. Airflow standalone is local-only.

Production work includes secret-manager/IAM integration, TLS, encryption at rest,
role-scoped Gold access, retention/deletion policy, backup/restore drills and
auditable release permissions. Filesystem checksums detect accidental changes;
they do not protect against an administrator altering code and evidence together.

Single-node DuckDB and sequential Airflow limit resource usage and dependencies.
The complete Saleor/Airflow/dashboard stack has not been acceptance-tested under
4 GB RAM. Run components separately when memory is constrained; monitor Docker
usage before promising that limit. No cloud cost estimate is represented as a
measurement of this local project.

## CI and promotion

GitHub Actions validates PRs and main with formatting, lint, Python tests, real
dbt integration tests and a built package. An explicit manual workflow input on
main demonstrates promotion of the same artifact through a production environment.
Configure required reviewers and branch protection in repository settings; YAML
alone does not enable approval governance. The promotion job is a placeholder,
not an actual deployment. GitLab CI is an alternative MR/default-branch example.

Use short feature branches and clear A-F requirement labels in commits. GitFlow
can be applied for multiple release lines; this small deliverable does not require
long-lived develop/release branches. The original planned workflow is design
context rather than evidence that repository protections have been configured.
