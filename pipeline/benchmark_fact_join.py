"""Measure the fact-table build with and without broadcast joins.

Builds fact rows for every silver claim (written to Spark's no-op sink, so
only the computation is timed) and reports the median of several runs.

Usage (inside the runner container, after the demo has run):
    python -m pipeline.benchmark_fact_join [--runs 3]
"""

import argparse
import os
import statistics
import time

from claims_etl import gold
from claims_etl.config import LakeConfig
from claims_etl.delta_utils import read_delta
from claims_etl.spark_session import get_spark


def time_build(spark, cfg, claims, broadcast_dims: bool, runs: int) -> float:
    threshold = "10485760" if broadcast_dims else "-1"
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", threshold)
    timings = []
    for _ in range(runs):
        started = time.perf_counter()
        gold.fact_rows(spark, cfg, claims, broadcast_dims).write.format("noop").mode("overwrite").save()
        timings.append(time.perf_counter() - started)
    return statistics.median(timings)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", "/lake"))
    args = parser.parse_args()

    spark = get_spark()
    cfg = LakeConfig(args.lake_root)
    claims = read_delta(spark, cfg.silver("claims"))
    print(f"Silver claims: {claims.count():,}")

    time_build(spark, cfg, claims, True, 1)  # warm-up
    without = time_build(spark, cfg, claims, False, args.runs)
    with_broadcast = time_build(spark, cfg, claims, True, args.runs)
    print(f"Sort-merge joins : {without:6.1f}s (median of {args.runs})")
    print(f"Broadcast joins  : {with_broadcast:6.1f}s (median of {args.runs})")
    print(f"Improvement      : {100 * (without - with_broadcast) / without:5.1f}%")


if __name__ == "__main__":
    main()
