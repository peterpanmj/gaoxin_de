"""PySpark Silver/Gold build and quality gates for an isolated Parquet release."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from saleor_analytics.common import atomic_json, now
from saleor_analytics.records import RecordError


def session():
    """Use a small local Spark session that also runs inside the Airflow image."""
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    spark = (
        SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]"))
        .appName("saleor-analytics")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def order_schema():
    """Pin the accepted v2 contract instead of inferring fields from each batch."""
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    line = StructType(
        [
            StructField("line_id", StringType()),
            StructField("product_name", StringType()),
            StructField("sku", StringType()),
            StructField("quantity", StringType()),
            StructField("unit_amount", StringType()),
            StructField("line_amount", StringType()),
        ]
    )
    return StructType(
        [
            StructField("order_id", StringType()),
            StructField("order_number", StringType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
            StructField("status", StringType()),
            StructField("channel", StringType()),
            StructField("currency", StringType()),
            StructField("total_amount", StringType()),
            StructField("lines", ArrayType(line)),
        ]
    )


def stage_accepted(spark, root: Path, inputs: list[dict], dataset: Path) -> None:
    """Read accepted Bronze JSONL through Spark and write versioned staging Parquet."""
    from pyspark.sql import functions as F

    frames = []
    for item in inputs:
        path = root / "bronze" / item["snapshot_id"] / "orders.accepted.jsonl"
        # Empty incremental deltas have no rows; the previous snapshots remain.
        if item["accepted_count"]:
            frame = spark.read.schema(order_schema()).json(str(path))
            frames.append(frame.withColumn("snapshot_id", F.lit(item["snapshot_id"])))
    if not frames:
        raise RecordError("No accepted orders are available for a release")
    versions = frames[0]
    for frame in frames[1:]:
        versions = versions.unionByName(frame)
    versions = versions.withColumn(
        "payload_json", F.to_json(F.struct(*[F.col(name) for name in order_schema().fieldNames()]))
    ).withColumn("payload_hash", F.sha2("payload_json", 256))
    target = dataset / "staging" / "order_versions"
    if target.exists():
        shutil.rmtree(target)
    versions.write.mode("errorifexists").parquet(str(target))


def build_and_validate(spark, dataset: Path, evidence: Path) -> int:
    """Select complete latest order versions, aggregate Gold, and gate publication."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    versions = spark.read.parquet(str(dataset / "staging" / "order_versions"))
    versions = (
        versions.withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
    )
    conflict = (
        versions.groupBy("order_id", "updated_at")
        .agg(F.countDistinct("payload_hash").alias("payload_count"))
        .filter("payload_count > 1")
        .limit(1)
        .count()
    )
    window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(), F.col("snapshot_id").desc(), F.col("payload_hash").desc()
    )
    orders = (
        versions.withColumn("version_rank", F.row_number().over(window))
        .filter("version_rank = 1")
        .drop("version_rank")
    )
    lines = (
        orders.select(
            "snapshot_id",
            "order_id",
            "payload_hash",
            F.explode("lines").alias("line"),
        )
        .select("snapshot_id", "order_id", "payload_hash", "line.*")
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_amount", F.col("unit_amount").cast("decimal(18,2)"))
        .withColumn("line_amount", F.col("line_amount").cast("decimal(18,2)"))
    )
    active = orders.filter(~F.col("status").isin("CANCELED", "DRAFT"))
    daily = (
        active.withColumn("order_date", F.to_date("created_at"))
        .groupBy("order_date", "channel", "currency")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_amount").alias("gross_amount"),
            F.avg("total_amount").alias("average_order_value"),
        )
    )
    product = (
        lines.join(active.select("order_id", "created_at", "channel", "currency"), "order_id")
        .withColumn("order_date", F.to_date("created_at"))
        .groupBy("order_date", "channel", "currency", "sku", "product_name")
        .agg(
            F.sum("quantity").alias("units_ordered"),
            F.sum("line_amount").alias("gross_amount"),
        )
    )

    checks = {
        "no_conflicting_order_versions": conflict == 0,
        "orders_present": orders.limit(1).count() == 1,
        "unique_current_orders": orders.groupBy("order_id")
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
        == 0,
        "order_keys_present": orders.filter("order_id is null or snapshot_id is null")
        .limit(1)
        .count()
        == 0,
        "order_money_valid": orders.filter("total_amount is null or total_amount < 0")
        .limit(1)
        .count()
        == 0,
        "order_dates_valid": orders.filter("created_at is null or updated_at is null")
        .limit(1)
        .count()
        == 0,
        "unique_current_lines": lines.groupBy("order_id", "line_id")
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
        == 0,
        "line_contract": lines.filter(
            "line_id is null or quantity <= 0 or unit_amount is null or unit_amount < 0 "
            "or line_amount is null or line_amount < 0"
        )
        .limit(1)
        .count()
        == 0,
        "line_parents_present": lines.join(orders.select("order_id"), "order_id", "left_anti")
        .limit(1)
        .count()
        == 0,
        "daily_order_reconciliation": (
            daily.agg(
                F.coalesce(F.sum("order_count"), F.lit(0)).alias("n"),
                F.coalesce(F.sum("gross_amount"), F.lit(0)).alias("v"),
            ).first()
            == active.agg(
                F.count("*").alias("n"), F.coalesce(F.sum("total_amount"), F.lit(0)).alias("v")
            ).first()
        ),
        "daily_product_reconciliation": (
            product.agg(
                F.coalesce(F.sum("units_ordered"), F.lit(0)).alias("n"),
                F.coalesce(F.sum("gross_amount"), F.lit(0)).alias("v"),
            ).first()
            == lines.join(active.select("order_id"), "order_id")
            .agg(
                F.coalesce(F.sum("quantity"), F.lit(0)).alias("n"),
                F.coalesce(F.sum("line_amount"), F.lit(0)).alias("v"),
            )
            .first()
        ),
    }
    evidence.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(evidence, {"engine": "pyspark", "checked_at": now(), "checks": checks})
    if not all(checks.values()):
        failed = ", ".join(key for key, passed in checks.items() if not passed)
        raise RecordError(f"Spark quality checks failed: {failed}")
    for name, frame in {
        "silver/orders": orders.drop("lines", "payload_json"),
        "silver/order_lines": lines,
        "gold/daily_order_metrics": daily,
        "gold/daily_product_metrics": product,
    }.items():
        target = dataset / name
        if target.exists():
            shutil.rmtree(target)
        frame.write.mode("errorifexists").parquet(str(target))
    return len(checks)
