# Saleor Analytics: Implementation Plan

**Status:** Proposed implementation sequence. Pipeline development has not started.

**Estimate:** Approximately 8-14 hours of AI-assisted implementation, including tests and integration verification. This is a planning range, not a deadline or hard cap. Complete the agreed scope and verification even if more time is needed. The final write-up and demo preparation are separate.

**Stack:** Saleor, Docker Compose, Python, Click, DuckDB, dbt, Plotly Dash, Airflow and GitHub Actions.

## 1. Establish the runnable environment

**Estimate: 60-105 minutes**

- [ ] Start Saleor and verify its API is accessible.
- [ ] Initialize the Python package and Click CLI.
- [ ] Add a dependency lock and environment-specific configuration.

**Verification gate:** Compose health checks pass; an authenticated GraphQL query succeeds; CLI help works; the package installs in a clean environment.

## 2. Implement controlled synthetic-data scenarios

**Estimate: 60-105 minutes**

- [ ] Implement baseline seeding through supported Saleor API workflows.
- [ ] Add order update, line removal and cancellation scenarios where supported by the installed API and order state.
- [ ] Record scenario manifests and affected synthetic entity IDs.
- [ ] Make retries avoid unintended duplicate source mutations.

**Verification gate:** Synthetic entities can be created and retrieved; scenario preconditions are checked; retries do not create unintended duplicates.

## 3. Implement extraction and bronze snapshots

**Estimate: 60-105 minutes**

- [ ] Extract paginated GraphQL data with timeouts and bounded retries.
- [ ] Handle GraphQL errors even when HTTP returns success.
- [ ] Write immutable JSONL snapshots and completion manifests.

**Verification gate:** Pagination completeness and error handling tests pass. Interrupted extraction never produces a snapshot marked complete.

## 4. Implement Python validation and staging

**Estimate: 45-75 minutes**

- [ ] Define and enforce the input contract.
- [ ] Normalize money, timestamps and identifiers.
- [ ] Handle exact duplicates and quarantine invalid records.
- [ ] Load validated data into run-specific DuckDB staging tables.

**Verification gate:** Focused pytest cases cover money, timestamps, malformed records and duplicates. Input counts reconcile with accepted, duplicate and rejected records.

## 5. Build the dbt analytical warehouse

**Estimate: 90-165 minutes**

- [ ] Model current orders and their complete current line sets.
- [ ] Build daily order and product metrics with explicit status and currency semantics.
- [ ] Add model descriptions and dbt data tests.

**Verification gate:** `dbt build` passes uniqueness, relationship, accepted-value, conflicting-version and monetary reconciliation checks. Changed orders and removed lines produce the expected analytical results.

## 6. Implement validated publication and Dash

**Estimate: 60-105 minutes**

- [ ] Promote candidate releases only after successful validation.
- [ ] Build one read-only Dash page with filters, KPI cards and charts.
- [ ] Display publication version/time and data-quality status.

**Verification gate:** A failed build leaves the published release unchanged. Replay produces equivalent results. Dashboard values match gold queries and use one consistent release per refresh.

## 7. Add Airflow orchestration

**Estimate: 60-105 minutes**

- [ ] Create one DAG with stage dependencies and shared run artifacts.
- [ ] Expose optional, user-controlled mock generation through manual-trigger parameters.
- [ ] Default mock generation to disabled and skip it for scheduled runs.
- [ ] Configure bounded retries and serialized publication.

**Verification gate:** The DAG imports successfully. Manual runs work with generation enabled and disabled. Failed quality gates block publication. Scheduled execution skips seeding.

## 8. Add CI and perform acceptance testing

**Estimate: 45-75 minutes**

- [ ] Add GitHub Actions checks and package building.
- [ ] Define an illustrative controlled promotion gate.
- [ ] Complete integration fixes and verify repository state.

**Verification gate:** Fresh setup, complete source-to-dashboard execution, repeat execution, order-change processing, deliberate failure and recovery all behave as documented. Git working tree is clean after committing changes.

## Working rules

- Click is the execution interface; Airflow invokes the same underlying functionality.
- dbt owns most transformations and analytical assertions. Python tests cover ingestion and operational behavior.
- Mock generation is optional and restricted to synthetic demo entities.
- Failed validation preserves the previous published release for Dash consumers.
- GitHub Actions stages map explicitly to the assessment's CI/CD requirements.
- After each step, report changes, tests, results and limitations, then commit the coherent change using the assessment mapping below.
- Resolve failed gates before proceeding to dependent work. Time allocations are targets, not permission to skip verification.

## Commit messages and assessment traceability

Map commits to the original `Principal_Data_Engineer_Candidate_Take_Home.md` using these section identifiers:

- **A:** Data Architecture and Design.
- **B:** Required Python Programming Assignment.
- **C:** Data Quality, Contracts, and Testing.
- **D:** Orchestration and Operational Design.
- **E:** CI/CD and Delivery Practices.
- **F:** README and Design Explanation.

Use a concrete subject in the form `type(scope): describe the change [Req B,C]`. Include only directly relevant sections; do not tag every commit with every requirement.

The commit body should explain the resulting behavior and why it matters, identify the specific assessment expectations addressed, and record relevant verification results or limitations. Never claim unexecuted tests passed. Keep commits coherent; split a step into multiple commits when its changes have separate purposes. Do not rewrite existing commits solely to apply this convention.

Planned examples, to be adjusted to the actual implemented changes:

1. `build(env): configure Saleor and installable pipeline CLI [Req A,B]`
2. `feat(seed): add repeatable synthetic order scenarios [Req B,C]`
3. `feat(ingest): persist complete paginated Saleor snapshots [Req A,B,D]`
4. `feat(validate): normalize orders and quarantine invalid records [Req B,C]`
5. `feat(dbt): build tested order and product marts [Req A,C]`
6. `feat(publish): serve validated warehouse releases in Dash [Req A,C,D]`
7. `feat(airflow): orchestrate pipeline with optional mock generation [Req D]`
8. `ci(github): validate and package the pipeline before promotion [Req E]`
9. `docs(assessment): map implementation evidence to deliverables [Req F]`

## Separate documentation and demo work

Separate from the implementation estimate:

- [ ] Complete the requirements traceability for assessment sections A-F.
- [ ] Document business metrics, model grains, contracts and source-to-report lineage.
- [ ] Explain retry, replay, quarantine, publication and recovery behavior.
- [ ] Document setup, CLI commands, orchestration, CI/CD, security, cost and production evolution.
- [ ] Prepare and rehearse the interview walkthrough.

All project content must be in English. Distinguish verified functionality, illustrative configuration, assumptions and future work.
