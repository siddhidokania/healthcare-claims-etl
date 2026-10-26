"""Print a summary of the lake: table sizes, latest DQ and reconciliation results, run times.

Usage (inside the runner container):
    python -m pipeline.report
"""

import argparse
import os

from pyspark.sql import functions as F

from claims_etl.config import LakeConfig
from claims_etl.delta_utils import read_delta, table_exists
from claims_etl.spark_session import get_spark

TABLES = [
    ("bronze", "beneficiary"), ("bronze", "inpatient_claims"), ("bronze", "outpatient_claims"),
    ("silver", "beneficiary"), ("silver", "claims"), ("silver", "claims_quarantine"),
    ("gold", "dim_date"), ("gold", "dim_patient"), ("gold", "dim_provider"), ("gold", "dim_diagnosis"),
    ("gold", "fact_claims"), ("gold", "bridge_claim_diagnosis"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", "/lake"))
    args = parser.parse_args()
    spark = get_spark()
    cfg = LakeConfig(args.lake_root)

    print("\nTable sizes")
    for layer, table in TABLES:
        path = getattr(cfg, layer)(table)
        if table_exists(spark, path):
            print(f"  {layer}.{table:<26} {read_delta(spark, path).count():>10,}")

    patients = read_delta(spark, cfg.gold("dim_patient")).where("patient_id <> 'UNKNOWN'")
    versions = patients.groupBy("patient_id").count().groupBy("count").agg(F.count("*").alias("patients"))
    print("\nPatients by number of SCD Type 2 versions")
    for row in versions.orderBy("count").collect():
        print(f"  {row['count']} version(s): {row['patients']:,}")

    print("\nQuarantined claims by reason")
    for row in (read_delta(spark, cfg.silver("claims_quarantine"))
                .groupBy("claim_type", "quarantine_reason").count().orderBy("claim_type").collect()):
        print(f"  {row['claim_type']} {row['quarantine_reason']:<40} {row['count']:,}")

    runs = read_delta(spark, cfg.ops("pipeline_runs"))
    last_run = runs.orderBy(F.col("started_at").desc()).first()["run_id"]

    print(f"\nData quality, run {last_run} (non-passing rules)")
    dq = read_delta(spark, cfg.ops("dq_results")).where(F.col("run_id") == last_run)
    for row in dq.where("status <> 'PASS'").collect():
        print(f"  {row['status']} {row['table_name']}.{row['rule_name']}: "
              f"{row['failed_rows']:,} of {row['total_rows']:,} rows ({row['failed_pct']}%)")
    passed = dq.where(F.col("status") == "PASS").count()
    print(f"  {passed} rules passed")

    print(f"\nReconciliation, run {last_run}")
    for row in read_delta(spark, cfg.ops("reconciliation")).where(F.col("run_id") == last_run).collect():
        print(f"  {row['status']} {row['check_name']:<28} {row['scope']:<18} "
              f"{row['source_value']:>16} vs {row['target_value']:<16} {row['note']}")

    print("\nStep durations by batch (seconds)")
    durations = runs.where("status = 'SUCCEEDED'").groupBy("batch_id").pivot("step").agg(F.max("duration_seconds"))
    durations.orderBy("batch_id").show(truncate=False)


if __name__ == "__main__":
    main()
