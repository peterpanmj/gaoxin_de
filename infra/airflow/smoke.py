"""Run inside the Airflow image; validate real DAG tasks on isolated synthetic data."""

import json
import os
from pathlib import Path
from uuid import uuid4

import pendulum
from airflow.models import DagBag

root = Path("/opt/data") / ("smoke-" + uuid4().hex)
os.environ["ANALYTICS_ROOT"] = str(root)
bag = DagBag("/opt/airflow/dags", include_examples=False)
assert not bag.import_errors, bag.import_errors
dag = bag.get_dag("saleor_analytics_daily")
assert dag is not None
previous = None
for index, params in enumerate(
    [
        {"generate_mock": True, "scenario": "duplicate", "count": 20},
        {"generate_mock": False, "input_mode": "fixture"},
        {"generate_mock": True, "scenario": "invalid", "count": 2},
    ]
):
    # Skip retry delays in this smoke run; normal DAG retains configured retries.
    for task in dag.tasks:
        task.retries = 0
    run = dag.test(execution_date=pendulum.now("UTC").add(seconds=index), run_conf=params)
    states = {task.task_id: str(task.state) for task in run.get_task_instances()}
    pointer = json.loads((root / "warehouse/current.json").read_text())
    if index < 2:
        assert str(run.state) == "success", states
        assert states["publish"] == "success", states
        previous = pointer
    else:
        assert str(run.state) == "failed", states
        assert states["publish"] == "upstream_failed", states
        assert pointer == previous
    print("SMOKE_RESULT", json.dumps({"scenario": params, "states": states, "root": str(root)}))
