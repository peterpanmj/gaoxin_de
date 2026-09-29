"""Serial pipeline with manual-only mock generation (assessment D).

Parameters reach subprocess argument lists, never a shell. Scheduled runs cannot
generate synthetic input. CLI code runs in its own environment, isolated from Airflow.
"""

import hashlib
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


def command(*args):
    """Run the isolated analytics CLI with a fixed config and no shell interpolation."""
    subprocess.run(
        [
            os.getenv("ANALYTICS_CLI", "/opt/analytics/bin/saleor-analytics"),
            "--config",
            os.getenv("ANALYTICS_CONFIG", "/opt/project/config/docker.toml"),
            *args,
        ],
        check=True,
        env=os.environ | {"PATH": "/opt/analytics/bin:" + os.environ["PATH"]},
    )


@dag(
    dag_id="saleor_analytics_daily",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={
        "generate_mock": Param(False, type="boolean"),
        "scenario": Param("baseline", enum=["baseline", "update", "duplicate", "invalid"]),
        "count": Param(20, type="integer", minimum=1, maximum=100000),
        "input_mode": Param("saleor", enum=["saleor", "fixture"]),
        "extract_mode": Param("full", enum=["full", "incremental"]),
    },
    tags=["saleor", "analytics", "demo"],
)
def saleor_analytics_daily():
    """Define the serial, publish-gated daily Saleor analytics pipeline."""

    @task
    def prepare():
        """Choose a safe input source and derive immutable batch-specific artifact paths."""
        context = get_current_context()
        params = context["params"]
        run = context["dag_run"]
        kind = getattr(run.run_type, "value", run.run_type)
        generate = params["generate_mock"] and kind == "manual"
        if params["generate_mock"] and not generate:
            raise ValueError("Mock generation is allowed only for manual runs")
        batch = "airflow-" + hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
        root = Path(os.getenv("ANALYTICS_ROOT", "/opt/data"))
        path = root / "mock" / f"{batch}.jsonl"
        if generate:
            command(
                "mock-data",
                str(path),
                "--scenario",
                params["scenario"],
                "--count",
                str(params["count"]),
            )
        elif params["input_mode"] == "fixture":
            path = Path("/opt/project/samples/orders-baseline.jsonl")
        return {
            "batch": batch,
            "file": str(path) if generate or params["input_mode"] == "fixture" else None,
            "extract_mode": params["extract_mode"],
            "database": str(root / "releases" / batch / "analytics.duckdb"),
        }

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def extract_and_validate(batch):
        """Ingest a local file or extract Saleor data into a validated Bronze snapshot."""
        if batch["file"]:
            command("ingest-file", batch["file"], "--snapshot-id", batch["batch"])
        else:
            command(
                "extract-saleor", "--snapshot-id", batch["batch"], "--mode", batch["extract_mode"]
            )
        return batch

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def stage(batch):
        """Build a candidate DuckDB release from all approved Bronze snapshots."""
        command("stage-warehouse", "--release-id", batch["batch"])
        return batch

    @task
    def transform_and_test(batch):
        """Run dbt models and data tests against the candidate release."""
        command("validate-candidate", batch["database"])
        return batch

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def publish(batch):
        """Atomically publish a checksum-validated candidate release and watermark state."""
        command("publish-candidate", batch["database"])
        return batch

    @task
    def monitor(batch):
        """Fail the task when the just-published release does not meet freshness policy."""
        command("status", "--max-age-hours", "24")

    monitor(publish(transform_and_test(stage(extract_and_validate(prepare())))))


saleor_analytics_daily()
