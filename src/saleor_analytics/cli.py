"""Reusable Click execution interface; requirement mapping uses assessment A-F.

Python owns parsing, normalization, duplicate handling and release control (B).
dbt owns relational models and data tests (C). Airflow invokes the same functions
(D). See docs/MODERN_DE_DEMO.md for executable scenarios and limitations (F).
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import click

from saleor_analytics.config import Settings
from saleor_analytics.dashboard import create_app
from saleor_analytics.extract import extract_orders
from saleor_analytics.mock import generate_mock
from saleor_analytics.pipeline import (
    build_candidate,
    exclude_snapshot,
    export_artifacts,
    ingest_jsonl,
    publish_candidate,
    published_database,
    read_json,
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
def cli(ctx, config):
    """Extract, validate and publish synthetic commerce analytics (Assessment B/F).

    Supply --config before the command. ANALYTICS_ROOT overrides the data root.
    Domain errors exit nonzero; no command accepts production credentials in flags.
    """
    ctx.obj = Settings.load(config)


@cli.command()
@click.pass_obj
def doctor(settings):
    """Check configuration bounds, not connectivity or service health (B/F)."""
    click.echo(f"Data root: {settings.root}\nConfiguration valid")


@cli.command("mock-data")
@click.argument("output", type=click.Path(path_type=Path))
@click.option(
    "--scenario",
    type=click.Choice(["baseline", "trend", "update", "duplicate", "invalid"]),
    default="baseline",
)
@click.option("--count", type=click.IntRange(1, 100000), default=20)
@click.pass_obj
def mock_data(settings, output, scenario, count):
    """Write deterministic local mock JSONL (B/C/D); never mutate Saleor.

    Requires allow_mock=true. Baseline has COUNT USD20 orders; update changes
    the first to USD30; duplicate repeats one row; invalid sets one quantity to
    zero. With COUNT=20, invalid is exactly the default 5% threshold; COUNT=2
    produces a blocking 50% reject rate. Ingest the output with ingest-file.
    """
    click.echo(json.dumps(generate_mock(settings, output, scenario, count), indent=2))


@cli.command("ingest-file")
@click.argument("input_path", type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.option("--snapshot-id", required=True)
@click.pass_obj
def ingest_file(settings, input_path, snapshot_id):
    """Validate JSONL into Bronze and quarantine (A/B/C).

    Normalize UTC/decimal values; collapse identical order versions. Version
    conflicts fail. Same-ID/same-bytes retries reuse completed output; changed
    bytes need a new ID. Failed thresholds write a failed manifest, never publish.
    Follow with build-warehouse for curated outputs. Only synthetic input belongs here.
    """
    click.echo(json.dumps(ingest_jsonl(settings, input_path, snapshot_id), indent=2))


@cli.command("extract-saleor")
@click.option("--snapshot-id", required=True)
@click.option(
    "--mode",
    type=click.Choice(["full", "incremental", "backfill"]),
    default="full",
    show_default=True,
)
@click.option(
    "--start", "backfill_start", default=None, help="Backfill start, inclusive, as ISO-8601 UTC."
)
@click.option(
    "--end", "backfill_end", default=None, help="Backfill end, exclusive, as ISO-8601 UTC."
)
@click.pass_obj
def extract_saleor(settings, snapshot_id, mode, backfill_start, backfill_end):
    """Extract a bounded full/API-delta window to validated JSONL (A/B/D).

    Incremental mode requires a published full baseline and overlaps its committed
    watermark by five minutes. Watermarks commit only with validated publication.
    Backfill mode requires --start/--end in UTC and reads [start, end) by updatedAt;
    it never advances the incremental watermark. Credentials: SALEOR_TOKEN or
    SALEOR_EMAIL/SALEOR_PASSWORD. This is API polling, not log CDC, deletion tracking
    or a historical as-of query.
    """
    click.echo(
        json.dumps(
            extract_orders(
                settings,
                snapshot_id,
                mode=mode,
                backfill_start=backfill_start,
                backfill_end=backfill_end,
            ),
            indent=2,
        )
    )


@cli.command("build-warehouse")
@click.option("--release-id", required=True)
@click.option("--publish/--no-publish", default=True, show_default=True)
@click.pass_obj
def build_warehouse(settings, release_id, publish):
    """Replay accepted history, run dbt gates and optionally publish (A/B/C/D).

    Newest source update wins; equal-time conflicts fail. Complete order line
    sets are selected with their order version. --no-publish still runs all tests.
    Same-release/same-input retries resume; changed inputs need a new release ID.
    Artifacts and lineage remain in releases/RELEASE_ID. Publication atomically
    commits warehouse/current.json; immutable databases are never overwritten.
    """
    database = build_candidate(settings, release_id)
    run_dbt_build(database)
    if publish:
        publish_candidate(settings, database)
    click.echo(f"{'Published' if publish else 'Validated'} release: {database}")


@cli.command("stage-warehouse")
@click.option("--release-id", required=True)
@click.pass_obj
def stage_warehouse(settings, release_id):
    """Stage approved history without dbt or publication (B/D Airflow boundary)."""
    click.echo(str(build_candidate(settings, release_id)))


@cli.command("exclude-snapshot")
@click.option("--snapshot-id", required=True)
@click.option("--reason", required=True)
@click.pass_obj
def exclude(settings, snapshot_id, reason):
    """Exclude an unpublished bad snapshot with an audit reason (C/D).

    Use after a cross-snapshot conflict fails dbt. Preserve raw evidence, correct
    input, and retry with new IDs. Inputs of the current release cannot be excluded.
    """
    exclude_snapshot(settings, snapshot_id, reason)
    click.echo(f"Excluded {snapshot_id}; raw evidence retained")


@cli.command("export-artifacts")
@click.argument("destination", required=False, type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--release-id", default=None, help="Release to export; defaults to the active release."
)
@click.option(
    "--all-snapshots",
    is_flag=True,
    help="Include every source snapshot in the release, rather than only the latest one.",
)
@click.pass_obj
def export_artifacts_command(settings, destination, release_id, all_snapshots):
    """Copy release, Bronze, quarantine, and dbt evidence for review (A/F).

    Creates DESTINATION/RELEASE_ID and never overwrites it. By default it copies
    only the latest source snapshot. --all-snapshots copies the full lineage.
    The active release includes warehouse/current.json; --release-id can export
    an older or failed candidate for investigation. The destination must be
    outside the analytics root so copied evidence cannot become pipeline input.
    """
    destination = destination or Path(os.getenv("ARTIFACT_EXPORT_ROOT", "artifacts"))
    click.echo(
        json.dumps(
            export_artifacts(settings, destination, release_id, all_snapshots=all_snapshots),
            indent=2,
        )
    )


@cli.command("validate-candidate")
@click.argument("database", type=click.Path(path_type=Path, exists=True, dir_okay=False))
def validate_candidate(database):
    """Build dbt models/tests and retain candidate-specific evidence (C/D)."""
    run_dbt_build(database)
    click.echo(f"Validated: {database}")


@cli.command("publish-candidate")
@click.argument("database", type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.pass_obj
def publish_candidate_command(settings, database):
    """Publish an unchanged validated candidate from this root (A/C/D).

    Verify dbt evidence, database/source hashes and base-release identity first.
    Stale or unvalidated candidates fail. This local gate is not a production
    IAM/approval system (E); protect that separately in CI and deployment.
    """
    click.echo(str(publish_candidate(settings, database)))


@cli.command("dashboard")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8050, type=click.IntRange(1, 65535), show_default=True)
@click.pass_obj
def dashboard(settings, host, port):
    """Serve currency/channel/date-filtered Gold reports and release status (A/F).

    Reload a consistent published release every 30 seconds. Requires current.json
    from the validated publication flow; legacy overwritten databases are not served.
    """
    create_app(settings).run(host=host, port=port)


@cli.command("status")
@click.option("--max-age-hours", default=24.0, type=click.FloatRange(min=0.01), show_default=True)
@click.pass_obj
def status(settings, max_age_hours):
    """Print serving lineage and freshness; exit nonzero if stale (A/D/F).

    Publication and source-extraction ages are separate. A fresh file-only release
    has no source watermark; completeness cannot be inferred from timestamp age.
    """
    _, pointer = published_database(settings)
    metadata = read_json(settings.root / "releases" / pointer["release_id"] / "release.json")
    publication_age = (
        datetime.now(UTC) - datetime.fromisoformat(pointer["published_at"])
    ).total_seconds() / 3600
    inputs = [datetime.fromisoformat(x["extracted_at"]) for x in metadata["inputs"]]
    extraction_age = (datetime.now(UTC) - max(inputs)).total_seconds() / 3600
    click.echo(
        json.dumps(
            pointer
            | {
                "publication_age_hours": publication_age,
                "latest_extraction_age_hours": extraction_age,
            },
            indent=2,
        )
    )
    if max(publication_age, extraction_age) > max_age_hours:
        raise click.ClickException("Freshness objective exceeded")
