"""Thin, noninteractive command-line entry points."""

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
    """Extract, validate, model and publish synthetic commerce analytics."""
    ctx.obj = Settings.load(config)


@cli.command()
@click.pass_obj
def doctor(settings: Settings) -> None:
    """Check local configuration without making network calls."""
    click.echo(f"Data root: {settings.root}\nConfiguration valid")


@cli.command("ingest-file")
@click.argument("input_path", type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.option("--snapshot-id", required=True, help="Immutable source snapshot identifier.")
@click.pass_obj
def ingest_file(settings: Settings, input_path: Path, snapshot_id: str) -> None:
    """Validate a JSONL order snapshot and write bronze plus quarantine records."""
    manifest = ingest_jsonl(settings, input_path, snapshot_id)
    click.echo(
        f"Accepted {manifest['accepted_count']}; rejected {manifest['rejected_count']}; "
        f"quality gate {manifest['quality_gate']}"
    )


@cli.command("extract-saleor")
@click.option("--snapshot-id", required=True, help="Immutable source snapshot identifier.")
@click.pass_obj
def extract_saleor(settings: Settings, snapshot_id: str) -> None:
    """Extract Saleor orders using credentials supplied through environment variables."""
    manifest = extract_orders(settings, snapshot_id)
    click.echo(
        f"Extracted {manifest['accepted_count']} orders; quality gate {manifest['quality_gate']}"
    )


@cli.command("build-warehouse")
@click.option("--release-id", required=True, help="Immutable candidate release identifier.")
@click.option("--publish/--no-publish", default=True, show_default=True)
@click.pass_obj
def build_warehouse(settings: Settings, release_id: str, publish: bool) -> None:
    """Build an isolated DuckDB warehouse candidate from approved snapshots."""
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
    """Publish a candidate that has already completed dbt build and tests."""
    click.echo(f"Published warehouse: {publish_candidate(settings, database)}")


@cli.command("dashboard")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8050, show_default=True, type=click.IntRange(1, 65535))
@click.pass_obj
def dashboard(settings: Settings, host: str, port: int) -> None:
    """Serve the dashboard from the published DuckDB warehouse."""
    create_app(settings.root / "warehouse" / "analytics.duckdb").run(host=host, port=port)
