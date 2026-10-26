"""Export gold data for BI tools (Power BI, Tableau, Excel).

    data/exports/star/    star schema as CSV (fact + dimensions) for Power BI / Tableau
    data/exports/marts/   small aggregated tables for the Excel workbook

Usage (inside the runner container, after the pipeline has run):
    python -m pipeline.export_bi
"""

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from claims_etl.config import LakeConfig
from claims_etl.delta_utils import read_delta
from claims_etl.silver import CHRONIC_CONDITIONS
from claims_etl.spark_session import get_spark

CONDITION_LABELS = {
    "has_alzheimers": "Alzheimer's / dementia",
    "has_heart_failure": "Heart failure",
    "has_chronic_kidney_disease": "Chronic kidney disease",
    "has_cancer": "Cancer",
    "has_copd": "COPD",
    "has_depression": "Depression",
    "has_diabetes": "Diabetes",
    "has_ischemic_heart_disease": "Ischemic heart disease",
    "has_osteoporosis": "Osteoporosis",
    "has_rheumatoid_osteo_arthritis": "Rheumatoid / osteo arthritis",
    "has_stroke_tia": "Stroke / TIA",
}


def write_csv(df: DataFrame, path: Path) -> int:
    pdf = df.toPandas()
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(path, index=False)
    print(f"  {path.name:<28} {len(pdf):>9,} rows")
    return len(pdf)


def star_schema(cfg: LakeConfig, spark, out: Path) -> None:
    fact = read_delta(spark, cfg.gold("fact_claims")).select(
        "claim_sk", "claim_id", "claim_type", "patient_sk", "provider_sk", "primary_diagnosis_sk",
        "from_date_key", "thru_date_key", "payment_amt", "deductible_amt", "coinsurance_amt",
        "length_of_stay_days", "diagnosis_count", "procedure_count", "segment_count", "is_adjustment", "claim_year",
    )
    patients = read_delta(spark, cfg.gold("dim_patient")).select(
        "patient_sk", "patient_id", "sex", "race", "state_code", "birth_date", "death_date", "has_esrd",
        *CHRONIC_CONDITIONS, "chronic_condition_count", "effective_from", "effective_to", "is_current",
    )
    write_csv(fact, out / "fact_claims.csv")
    write_csv(patients, out / "dim_patient.csv")
    write_csv(read_delta(spark, cfg.gold("dim_provider")).drop("first_seen_batch_id"), out / "dim_provider.csv")
    write_csv(read_delta(spark, cfg.gold("dim_diagnosis")).drop("first_seen_batch_id"), out / "dim_diagnosis.csv")
    write_csv(read_delta(spark, cfg.gold("dim_date")), out / "dim_date.csv")


def marts(cfg: LakeConfig, spark, out: Path) -> None:
    fact = read_delta(spark, cfg.gold("fact_claims"))
    patients = read_delta(spark, cfg.gold("dim_patient"))
    diagnoses = read_delta(spark, cfg.gold("dim_diagnosis"))
    dates = read_delta(spark, cfg.gold("dim_date"))

    monthly = (
        fact.join(dates, fact.from_date_key == dates.date_key)
        .groupBy(F.col("year").alias("claim_year"), "month", "claim_type")
        .agg(
            F.count("*").alias("claims"),
            F.countDistinct("patient_sk").alias("patients"),
            F.sum("payment_amt").cast("double").alias("total_payment"),
            F.sum(F.col("is_adjustment").cast("int")).alias("adjustments"),
        )
        .orderBy("claim_year", "month", "claim_type")
    )
    write_csv(monthly, out / "monthly_claims.csv")

    by_chapter = (
        fact.join(diagnoses, fact.primary_diagnosis_sk == diagnoses.diagnosis_sk)
        .groupBy("claim_year", "claim_type", "icd9_chapter")
        .agg(F.count("*").alias("claims"), F.sum("payment_amt").cast("double").alias("total_payment"))
        .orderBy("claim_year", "claim_type", F.desc("total_payment"))
    )
    write_csv(by_chapter, out / "claims_by_diagnosis_chapter.csv")

    by_state = (
        fact.join(patients.select("patient_sk", "state_code"), "patient_sk")
        .groupBy("claim_year", "state_code")
        .agg(
            F.count("*").alias("claims"),
            F.countDistinct("patient_sk").alias("patients"),
            F.sum("payment_amt").cast("double").alias("total_payment"),
        )
        .orderBy("claim_year", "state_code")
    )
    write_csv(by_state, out / "claims_by_state.csv")

    inpatient_los = (
        fact.where("claim_type = 'IP'")
        .withColumn(
            "length_of_stay_band",
            F.when(F.col("length_of_stay_days") <= 1, "0-1 days")
             .when(F.col("length_of_stay_days") <= 3, "2-3 days")
             .when(F.col("length_of_stay_days") <= 7, "4-7 days")
             .when(F.col("length_of_stay_days") <= 14, "8-14 days")
             .otherwise("15+ days"),
        )
        .groupBy("claim_year", "length_of_stay_band")
        .agg(F.count("*").alias("claims"), F.sum("payment_amt").cast("double").alias("total_payment"))
        .orderBy("claim_year", "length_of_stay_band")
    )
    write_csv(inpatient_los, out / "inpatient_length_of_stay.csv")

    # Chronic condition prevalence and annual Medicare reimbursement per patient,
    # from the yearly beneficiary snapshots.
    bene = read_delta(spark, cfg.silver("beneficiary")).withColumn(
        "annual_reimbursement",
        F.coalesce("ip_medicare_reimbursement_amt", F.lit(0))
        + F.coalesce("op_medicare_reimbursement_amt", F.lit(0))
        + F.coalesce("carrier_medicare_reimbursement_amt", F.lit(0)),
    )
    rows = None
    for column, label in CONDITION_LABELS.items():
        part = bene.groupBy("coverage_year").agg(
            F.lit(label).alias("condition"),
            F.count("*").alias("total_patients"),
            F.sum(F.col(column).cast("int")).alias("patients_with_condition"),
            F.sum(F.when(F.col(column), F.col("annual_reimbursement"))).cast("double").alias("reimbursement_with_condition"),
            F.sum(F.when(~F.col(column), F.col("annual_reimbursement"))).cast("double").alias("reimbursement_without_condition"),
        )
        rows = part if rows is None else rows.unionByName(part)
    write_csv(rows.orderBy("coverage_year", "condition"), out / "chronic_conditions.csv")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", "/lake"))
    parser.add_argument("--out", default="data/exports")
    args = parser.parse_args()

    spark = get_spark()
    cfg = LakeConfig(args.lake_root)
    out = Path(args.out)
    print("Star schema")
    star_schema(cfg, spark, out / "star")
    print("Marts")
    marts(cfg, spark, out / "marts")


if __name__ == "__main__":
    main()
