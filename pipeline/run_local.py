"""Run the pipeline locally (Docker), without Azure.

    extract   on-prem PostgreSQL -> raw Parquet + manifest (same as the ADF pipeline)
    bronze    raw -> bronze Delta
    silver    bronze -> silver (clean, consolidate, quarantine)
    gold      silver -> star schema
    quality   data quality rules
    reconcile source/bronze/silver/gold/legacy reconciliation
    optimize  OPTIMIZE + ZORDER

Usage (inside the runner container):
    python -m pipeline.run_local --batch-id 2008
    python -m pipeline.run_local --batch-id 2009 --steps bronze,silver,gold
    python -m pipeline.run_local --demo      # load 2008-2010 into the source and run every batch
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone

STEPS = ["extract", "bronze", "silver", "gold", "quality", "reconcile", "optimize"]
DEMO_YEARS = {2008: 0, 2009: 500, 2010: 500}   # year -> simulated adjustments


def run_batch(batch_id: str, steps: list[str], lake_root: str, broadcast_dims: bool = True) -> None:
    from claims_etl import audit, bronze, gold, optimize, quality, reconcile, silver
    from claims_etl.config import LakeConfig
    from claims_etl.spark_session import get_spark
    from ingestion.extract_incremental import extract_table
    from ingestion.tables import SOURCE_TABLES
    from onprem.db import connect
    from pathlib import Path

    spark = get_spark()
    cfg = LakeConfig(lake_root)
    run_id = f"{batch_id}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    print(f"=== batch {batch_id}  run {run_id}")

    for name in steps:
        with audit.step(spark, cfg, run_id, batch_id, name) as metrics:
            if name == "extract":
                with connect() as conn:
                    for spec in SOURCE_TABLES:
                        metrics[spec["table_name"]] = extract_table(conn, spec, batch_id, Path(lake_root))["row_count"]
            elif name == "bronze":
                metrics.update(bronze.run(spark, cfg, batch_id))
            elif name == "silver":
                metrics.update(silver.run(spark, cfg, batch_id))
            elif name == "gold":
                metrics.update(gold.run(spark, cfg, batch_id, broadcast_dims))
            elif name == "quality":
                results = quality.run(spark, cfg, run_id, batch_id)
                metrics.update({s: sum(r["status"] == s for r in results) for s in ("PASS", "WARN", "FAIL")})
            elif name == "reconcile":
                results = reconcile.run(spark, cfg, run_id, batch_id)
                metrics.update({s: sum(r["status"] == s for r in results) for s in ("PASS", "FAIL")})
            elif name == "optimize":
                metrics.update(optimize.run(spark, cfg))


def run_demo(lake_root: str) -> None:
    """Simulate three years: load the source, run the legacy job, run the new pipeline."""
    for year, adjustments in DEMO_YEARS.items():
        load = [sys.executable, "-m", "onprem.load_source", "--year", str(year)]
        if adjustments:
            load += ["--simulate-adjustments", str(adjustments)]
        subprocess.run(load, check=True)
        subprocess.run([sys.executable, "-m", "onprem.legacy_etl"], check=True)
        run_batch(str(year), STEPS, lake_root)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id")
    parser.add_argument("--steps", default=",".join(STEPS))
    parser.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", "/lake"))
    parser.add_argument("--no-broadcast", action="store_true", help="disable broadcast joins (for benchmarking)")
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()

    if args.demo:
        run_demo(args.lake_root)
    elif args.batch_id:
        steps = [s.strip() for s in args.steps.split(",")]
        unknown = set(steps) - set(STEPS)
        if unknown:
            parser.error(f"unknown steps: {', '.join(sorted(unknown))}")
        run_batch(args.batch_id, steps, args.lake_root, broadcast_dims=not args.no_broadcast)
    else:
        parser.error("pass --batch-id or --demo")


if __name__ == "__main__":
    main()
