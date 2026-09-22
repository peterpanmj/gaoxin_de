"""Airflow orchestration for the Saleor analytics batch pipeline.

Deploy this file to an Airflow DAG folder after installing this package in the
Airflow image. Credentials are injected by the runtime, never stored here.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    dag_id="saleor_analytics_daily",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["saleor", "analytics", "demo"],
)
def saleor_analytics_daily():
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract(snapshot_id: str) -> None:
        from saleor_analytics.cli import cli

        cli.main(args=["extract-saleor", "--snapshot-id", snapshot_id], standalone_mode=False)

    @task
    def transform_and_publish(release_id: str) -> None:
        from saleor_analytics.cli import cli

        cli.main(args=["build-warehouse", "--release-id", release_id], standalone_mode=False)

    @task
    def run_id() -> str:
        return get_current_context()["ts_nodash"]

    batch_id = run_id()
    extract(batch_id)
    transform_and_publish(batch_id)


saleor_analytics_daily()
