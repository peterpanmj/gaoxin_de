"""Click interface for the assessment's Python data-engineering component.

Requirement references use Principal_Data_Engineer_Candidate_Take_Home.md:
A = architecture, B = Python, C = data quality, D = orchestration,
E = delivery, and F = documentation. See docs/ASSESSMENT_REVIEW.md for gaps.

Python reads and normalizes files, quarantines records, stages DuckDB data and
coordinates dbt. SQL in dbt performs current-order selection and Gold aggregation;
Python does not currently deduplicate input records. These commands are usable
from a repository checkout; standalone wheel deployment is not yet complete.

Click validates command-line paths and options. Domain, filesystem and subprocess
exceptions otherwise propagate (usually as tracebacks and nonzero process exits).
No command implements a production approval policy.
"""

from pathlib import Path

import click

from saleor_analytics.config import Settings
from saleor_analytics.dashboard import create_app
from saleor_analytics.extract import extract_orders
from saleor_analytics.pipeline import (
    build_candidate,
    ingest_jsonl,
    publish_candidate,
    run_dbt_build,
)


@click.group()
@click.option(
    "--config",
    type=click.Path(path_type=Path, exists=True),
    default="config/local.toml",
    show_default=True,
)
@click.pass_context
def cli(ctx: click.Context, config: Path) -> None:
    """Extract, validate, model and publish synthetic commerce analytics.

    Assessment B/F: runnable Python CLI with configurable file and API ingestion.
    Use ingest-file then build-warehouse for the required structured-file flow.
    build-warehouse invokes dbt for deduplication, curated datasets and tests (C).

    Supply --config before the command. TOML supplies nonsecret settings;
    ANALYTICS_ROOT and SALEOR_URL override storage and source settings.
    See docs/ASSESSMENT_REVIEW.md for coverage, limitations and examples.
    """
    ctx.obj = Settings.load(config)


@cli.command()
@click.pass_obj
def doctor(settings: Settings) -> None:
    """Check local configuration without making network calls.

    Assessment B/F: load TOML and check page size, retry count and reject-rate
    bounds. Print the resolved data root. This does not verify dependencies,
    source credentials, Docker health, storage permissions or warehouse quality.
    """
    click.echo(f"Data root: {settings.root}\nConfiguration valid")


@cli.command("ingest-file")
@click.argument("input_path", type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.option("--snapshot-id", required=True, help="Immutable source snapshot identifier.")
@click.pass_obj
def ingest_file(settings: Settings, input_path: Path, snapshot_id: str) -> None:
    """Validate a JSONL order snapshot and write bronze plus quarantine records.

    Assessment B: read one Saleor order per nonblank line; normalize timestamps
    to UTC and monetary amounts to two decimal places. Python checks required
    fields and selected types; duplicate order records are currently retained.

    Assessment A/C: write raw and accepted JSONL beneath bronze/SNAPSHOT_ID,
    rejected rows beneath quarantine/SNAPSHOT_ID, and a checksum/count manifest.
    Empty input or a reject rate above the configured limit raises an error
    after writing the manifest. Known malformed nested types can abort ingestion
    instead of being quarantined. Existing snapshot IDs cannot be reused.

    Assessment F: print accepted/rejected counts on success. This command does
    not create curated tables or publish; follow it with build-warehouse.
    """
    manifest = ingest_jsonl(settings, input_path, snapshot_id)
    click.echo(
        f"Accepted {manifest['accepted_count']}; rejected {manifest['rejected_count']}; "
        f"quality gate {manifest['quality_gate']}"
    )


@cli.command("extract-saleor")
@click.option("--snapshot-id", required=True, help="Immutable source snapshot identifier.")
@click.pass_obj
def extract_saleor(settings: Settings, snapshot_id: str) -> None:
    """Extract Saleor orders using environment-supplied credentials.

    Assessment A/B/D: page through current orders into temporary JSONL, then use
    the same file-validation path as ingest-file. Reads retry transient transport
    errors, HTTP 429 and 5xx; GraphQL errors fail even with HTTP 200 responses.
    Credentials use SALEOR_TOKEN or SALEOR_EMAIL and SALEOR_PASSWORD.

    Selected fields exclude customer identity/address data (A/F). Full extraction
    assumes no concurrent source edits; it provides neither CDC nor historical
    as-of queries. A failed page prevents this attempt from reaching ingestion.
    No synthetic orders are generated or changed by this command.
    """
    manifest = extract_orders(settings, snapshot_id)
    click.echo(
        f"Extracted {manifest['accepted_count']} orders; quality gate {manifest['quality_gate']}"
    )


@cli.command("build-warehouse")
@click.option("--release-id", required=True, help="Immutable candidate release identifier.")
@click.option("--publish/--no-publish", default=True, show_default=True)
@click.pass_obj
def build_warehouse(settings: Settings, release_id: str, publish: bool) -> None:
    """Build and test a warehouse candidate; publish it unless --no-publish.

    Assessment A/B: replay accepted JSONL from all manifests marked passed into
    releases/RELEASE_ID/analytics.duckdb. dbt selects one order per order_id by
    updated_at DESC, snapshot_id DESC, payload_json DESC and builds daily order
    and product metrics by date, channel and currency. Same-version conflicts
    are not rejected; snapshot IDs are lexical tie-breakers, not ingestion times.

    Assessment C/D: run dbt build before copying the candidate to
    warehouse/analytics.duckdb. A dbt failure propagates before publication on
    this path. --no-publish still runs models/tests and retains the candidate.
    Release IDs must be new; automated retry/resume and writer locking are absent.

    Assessment F: emit dbt execution output and the resulting database path.
    This replays normalized accepted history, not raw files through new validation.
    """
    candidate = build_candidate(settings, release_id)
    run_dbt_build(candidate)
    if publish:
        target = publish_candidate(settings, candidate)
        click.echo(f"Published warehouse: {target}")
    else:
        click.echo(f"Built candidate warehouse: {candidate}")


@cli.command("publish-candidate")
@click.argument("database", type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.pass_obj
def publish_candidate_command(settings: Settings, database: Path) -> None:
    """Copy DATABASE into the serving warehouse without revalidating it.

    Assessment A/D: copy to warehouse/analytics.next.duckdb, then replace the
    serving file. The caller must establish that DATABASE passed dbt build.
    This command does not check test evidence, schema or candidate provenance;
    it can bypass the quality gate (C) and is not a controlled promotion gate (E).
    Prefer build-warehouse for the ordinary tested publication path.
    """
    click.echo(f"Published warehouse: {publish_candidate(settings, database)}")


@cli.command("dashboard")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8050, show_default=True, type=click.IntRange(1, 65535))
@click.pass_obj
def dashboard(settings: Settings, host: str, port: int) -> None:
    """Serve a local Dash page from the published DuckDB warehouse.

    Assessment A/F: consume daily Gold order and product aggregates using a
    read-only database connection. Data is loaded at startup; restart the server
    after publication to load a new release. Missing tables/files abort startup.

    Current limitations: the KPI and product chart combine currencies; the
    product chart ignores channel selection. Use Gold SQL grouped by currency
    for analytical checks until these reporting defects are corrected.
    """
    create_app(settings.root / "warehouse" / "analytics.duckdb").run(host=host, port=port)
