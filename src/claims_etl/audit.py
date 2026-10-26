"""Run log: one row per pipeline step in ops/pipeline_runs."""

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from claims_etl.config import LakeConfig

SCHEMA = (
    "run_id string, batch_id string, step string, status string, started_at timestamp, "
    "finished_at timestamp, duration_seconds double, metrics string, error string"
)


@contextmanager
def step(spark: SparkSession, cfg: LakeConfig, run_id: str, batch_id: str, name: str):
    """Usage: with step(...) as metrics: metrics.update(layer.run(...))"""
    metrics: dict = {}
    started_at, started = datetime.now(timezone.utc), time.perf_counter()
    status, error = "SUCCEEDED", None
    try:
        yield metrics
    except Exception as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        duration = round(time.perf_counter() - started, 2)
        row = (run_id, batch_id, name, status, started_at, datetime.now(timezone.utc), duration,
               json.dumps(metrics, default=str), error)
        spark.createDataFrame([row], SCHEMA).write.format("delta").mode("append").save(cfg.ops("pipeline_runs"))
        print(f"[{status}] {name:<10} {duration:>7.1f}s  {json.dumps(metrics, default=str)}")
