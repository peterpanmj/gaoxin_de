"""Bounded, reproducible JSON/Parquet storage experiment (assessment A/F)."""

import time

import duckdb

from saleor_analytics.common import atomic_json, identifier
from saleor_analytics.config import Settings


def benchmark_storage(settings: Settings, run_id: str, rows: int) -> dict:
    """Generate synthetic monthly data and retain query-plan/measured evidence.

    No speedup is assumed. This standalone experiment does not change warehouse
    storage or demonstrate distributed processing. Tiny files are intentional
    teaching fixtures, not a production file-size recommendation.
    """
    if not 12000 <= rows <= 1000000:
        raise ValueError("rows must be between 12000 and 1000000")
    directory = settings.root / "benchmarks" / identifier(run_id)
    directory.mkdir(parents=True, exist_ok=False)
    json_path, parquet_path = directory / "orders.jsonl", directory / "parquet"
    with duckdb.connect() as conn:
        conn.execute(
            "create table sample as select i as order_id, (i % 12 + 1)::integer as month, "
            "(i % 100 + 1)::decimal(18,2) as amount, 'USD' as currency, "
            "'Synthetic product ' || (i % 50)::varchar as product_name from range(?) t(i)",
            [rows],
        )
        conn.execute("copy sample to ? (format json)", [str(json_path)])
        conn.execute(
            "copy sample to ? (format parquet, partition_by (month), compression zstd)",
            [str(parquet_path)],
        )
        query_json = (
            "select count(*), sum(amount::decimal(18,2)) from read_json_auto(?) where month=1"
        )
        query_parquet = (
            "select count(*), sum(amount) from read_parquet(?, hive_partitioning=true) "
            "where month=1"
        )
        glob = str(parquet_path / "*" / "*.parquet")
        results = {}
        for label, query, path in [
            ("json", query_json, str(json_path)),
            ("parquet", query_parquet, glob),
        ]:
            started = time.perf_counter()
            result = conn.execute(query, [path]).fetchone()
            results[label] = {
                "seconds": time.perf_counter() - started,
                "count": result[0],
                "amount": str(result[1]),
            }
        assert results["json"]["count"] == results["parquet"]["count"]
        assert results["json"]["amount"] == results["parquet"]["amount"]
        plan = conn.execute("explain analyze " + query_parquet, [glob]).fetchall()
        (directory / "pruning-plan.txt").write_text(
            "\n".join(row[1] for row in plan), encoding="utf-8"
        )
        results.update(
            rows=rows,
            json_bytes=json_path.stat().st_size,
            parquet_bytes=sum(p.stat().st_size for p in parquet_path.rglob("*.parquet")),
            partitions=len(list(parquet_path.glob("month=*"))),
            duckdb_version=duckdb.__version__,
            note="One run, JSON first; cache/order affect timing. Inspect the query plan.",
        )
        atomic_json(directory / "results.json", results)
    return results
