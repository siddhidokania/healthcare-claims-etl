"""Silver: cleaned, typed and de-duplicated data.

Claims:
  * inpatient and outpatient claims are mapped to one common layout
  * only the latest version of each (claim, segment) is kept, so source
    adjustments replace the earlier values
  * multi-segment claims are consolidated into one row per claim: header
    fields come from segment 1 and amounts are summed over all segments
  * rows that fail validation go to a quarantine table instead of silver
Beneficiaries:
  * one row per patient per coverage year, with decoded codes and boolean
    chronic condition flags
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from claims_etl import icd9
from claims_etl.config import CLAIM_SOURCES, LakeConfig
from claims_etl.delta_utils import merge_upsert, overwrite_partition, read_delta

AMOUNT = "decimal(12,2)"

CHRONIC_CONDITIONS = {
    "has_alzheimers": "sp_alzhdmta",
    "has_heart_failure": "sp_chf",
    "has_chronic_kidney_disease": "sp_chrnkidn",
    "has_cancer": "sp_cncr",
    "has_copd": "sp_copd",
    "has_depression": "sp_depressn",
    "has_diabetes": "sp_diabetes",
    "has_ischemic_heart_disease": "sp_ischmcht",
    "has_osteoporosis": "sp_osteoprs",
    "has_rheumatoid_osteo_arthritis": "sp_ra_oa",
    "has_stroke_tia": "sp_strketia",
}

DX_COLUMNS = [f"dx_{i}" for i in range(1, 11)]
PX_COLUMNS = [f"px_{i}" for i in range(1, 7)]
HEADER_COLUMNS = [
    "patient_id", "from_dt", "thru_dt", "provider_num", "attending_npi", "operating_npi",
    "other_npi", "admitting_dx_raw", "admission_dt", "discharge_dt", "drg_code",
    *DX_COLUMNS, *PX_COLUMNS,
]
AMOUNT_COLUMNS = [
    "payment_amt", "primary_payer_paid_amt", "deductible_amt", "coinsurance_amt",
    "blood_deductible_amt", "pass_thru_per_diem_amt",
]


def parse_yyyymmdd(col_name: str):
    """Text YYYYMMDD to date; bad values become null instead of failing the job."""
    return F.to_date(F.expr(f"try_to_timestamp({col_name}, 'yyyyMMdd')"))


def latest_per_key(df: DataFrame, keys: list[str]) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(F.col("_source_updated_at").desc(), F.col("_batch_id").desc())
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# Beneficiaries
# ---------------------------------------------------------------------------

def transform_beneficiary(df: DataFrame) -> DataFrame:
    flags = [(F.col(src) == "1").alias(name) for name, src in CHRONIC_CONDITIONS.items()]
    return df.select(
        F.col("desynpuf_id").alias("patient_id"),
        F.col("coverage_year").cast("int"),
        parse_yyyymmdd("bene_birth_dt").alias("birth_date"),
        parse_yyyymmdd("bene_death_dt").alias("death_date"),
        F.when(F.col("bene_sex_ident_cd") == "1", "Male")
         .when(F.col("bene_sex_ident_cd") == "2", "Female").otherwise("Unknown").alias("sex"),
        F.when(F.col("bene_race_cd") == "1", "White")
         .when(F.col("bene_race_cd") == "2", "Black")
         .when(F.col("bene_race_cd") == "3", "Other")
         .when(F.col("bene_race_cd") == "5", "Hispanic").otherwise("Unknown").alias("race"),
        (F.col("bene_esrd_ind") == "Y").alias("has_esrd"),
        F.lpad("sp_state_code", 2, "0").alias("state_code"),
        F.lpad("bene_county_cd", 3, "0").alias("county_code"),
        F.col("bene_hi_cvrage_tot_mons").cast("int").alias("part_a_coverage_months"),
        F.col("bene_smi_cvrage_tot_mons").cast("int").alias("part_b_coverage_months"),
        F.col("bene_hmo_cvrage_tot_mons").cast("int").alias("hmo_coverage_months"),
        F.col("plan_cvrg_mos_num").cast("int").alias("part_d_coverage_months"),
        *flags,
        F.col("medreimb_ip").cast(AMOUNT).alias("ip_medicare_reimbursement_amt"),
        F.col("benres_ip").cast(AMOUNT).alias("ip_beneficiary_responsibility_amt"),
        F.col("pppymt_ip").cast(AMOUNT).alias("ip_primary_payer_amt"),
        F.col("medreimb_op").cast(AMOUNT).alias("op_medicare_reimbursement_amt"),
        F.col("benres_op").cast(AMOUNT).alias("op_beneficiary_responsibility_amt"),
        F.col("pppymt_op").cast(AMOUNT).alias("op_primary_payer_amt"),
        F.col("medreimb_car").cast(AMOUNT).alias("carrier_medicare_reimbursement_amt"),
        F.col("benres_car").cast(AMOUNT).alias("carrier_beneficiary_responsibility_amt"),
        F.col("pppymt_car").cast(AMOUNT).alias("carrier_primary_payer_amt"),
        F.col("updated_at").alias("_source_updated_at"),
        F.col("_batch_id"),
    )


def build_beneficiary(spark: SparkSession, cfg: LakeConfig, batch_id: str) -> int:
    batch = read_delta(spark, cfg.bronze("beneficiary")).where(F.col("_batch_id") == batch_id)
    silver = latest_per_key(transform_beneficiary(batch), ["patient_id"]).withColumn(
        "_processed_at", F.current_timestamp()
    )
    merge_upsert(silver, cfg.silver("beneficiary"), ["patient_id", "coverage_year"])
    return silver.count()


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------

def standardize_claims(df: DataFrame, claim_type: str) -> DataFrame:
    """Map inpatient or outpatient source columns to one common layout."""
    is_ip = claim_type == "IP"

    def ip_only(col_name: str, cast: str):
        return F.col(col_name).cast(cast) if is_ip else F.lit(None).cast(cast)

    return df.select(
        F.col("clm_id").alias("claim_id"),
        F.lit(claim_type).alias("claim_type"),
        F.col("segment").cast("int"),
        F.col("desynpuf_id").alias("patient_id"),
        F.col("clm_from_dt").alias("from_dt"),
        F.col("clm_thru_dt").alias("thru_dt"),
        F.col("prvdr_num").alias("provider_num"),
        F.col("at_physn_npi").alias("attending_npi"),
        F.col("op_physn_npi").alias("operating_npi"),
        F.col("ot_physn_npi").alias("other_npi"),
        F.col("admtng_icd9_dgns_cd").alias("admitting_dx_raw"),
        ip_only("clm_admsn_dt", "string").alias("admission_dt"),
        ip_only("nch_bene_dschrg_dt", "string").alias("discharge_dt"),
        ip_only("clm_drg_cd", "string").alias("drg_code"),
        *[F.col(f"icd9_dgns_cd_{i}").alias(f"dx_{i}") for i in range(1, 11)],
        *[F.col(f"icd9_prcdr_cd_{i}").alias(f"px_{i}") for i in range(1, 7)],
        F.col("clm_pmt_amt").cast(AMOUNT).alias("payment_amt"),
        F.col("nch_prmry_pyr_clm_pd_amt").cast(AMOUNT).alias("primary_payer_paid_amt"),
        F.col("nch_bene_ip_ddctbl_amt" if is_ip else "nch_bene_ptb_ddctbl_amt").cast(AMOUNT).alias("deductible_amt"),
        F.col("nch_bene_pta_coinsrnc_lblty_am" if is_ip else "nch_bene_ptb_coinsrnc_amt").cast(AMOUNT).alias("coinsurance_amt"),
        F.col("nch_bene_blood_ddctbl_lblty_am").cast(AMOUNT).alias("blood_deductible_amt"),
        ip_only("clm_pass_thru_per_diem_amt", AMOUNT).alias("pass_thru_per_diem_amt"),
        ip_only("clm_utlztn_day_cnt", "int").alias("utilization_days"),
        F.col("updated_at").alias("_source_updated_at"),
        F.col("_batch_id"),
    )


def consolidate_segments(df: DataFrame) -> DataFrame:
    """One row per claim. Segment 2 rows carry amounts but no dates or diagnoses."""
    is_first = F.col("segment") == 1
    header = [
        F.coalesce(F.first(F.when(is_first, F.col(c)), ignorenulls=True), F.first(c, ignorenulls=True)).alias(c)
        for c in HEADER_COLUMNS
    ]
    amounts = [F.sum(c).cast(AMOUNT).alias(c) for c in AMOUNT_COLUMNS]
    return df.groupBy("claim_type", "claim_id").agg(
        *header,
        *amounts,
        F.sum("utilization_days").cast("int").alias("utilization_days"),
        F.count("*").cast("int").alias("segment_count"),
        F.max("_source_updated_at").alias("_source_updated_at"),
    )


def enrich_claims(df: DataFrame) -> DataFrame:
    diagnosis_codes = F.array_distinct(F.array_compact(F.array(*[icd9.normalize(F.col(c)) for c in DX_COLUMNS])))
    procedure_codes = F.array_distinct(F.array_compact(F.array(*[icd9.normalize(F.col(c)) for c in PX_COLUMNS])))
    return (
        df.withColumn("from_date", parse_yyyymmdd("from_dt"))
        .withColumn("thru_date", parse_yyyymmdd("thru_dt"))
        .withColumn("admission_date", parse_yyyymmdd("admission_dt"))
        .withColumn("discharge_date", parse_yyyymmdd("discharge_dt"))
        .withColumn("primary_dx_code", icd9.normalize(F.col("dx_1")))
        .withColumn("admitting_dx_code", icd9.normalize(F.col("admitting_dx_raw")))
        .withColumn("diagnosis_codes", diagnosis_codes)
        .withColumn("procedure_codes", procedure_codes)
        .withColumn(
            "length_of_stay_days",
            F.when(
                F.col("claim_type") == "IP",
                F.datediff(F.coalesce("discharge_date", "thru_date"), F.coalesce("admission_date", "from_date")),
            ),
        )
        .withColumn("claim_year", F.year("from_date"))
        .withColumn("is_adjustment", F.col("payment_amt") < 0)
        .drop("from_dt", "thru_dt", "admission_dt", "discharge_dt", "admitting_dx_raw", *DX_COLUMNS, *PX_COLUMNS)
    )


def validate_claims(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, quarantined). Quarantined rows carry the failed rules."""
    checks = [
        F.when(F.col("claim_id").isNull(), F.lit("missing_claim_id")),
        F.when(F.col("patient_id").isNull(), F.lit("missing_patient_id")),
        F.when(F.col("from_date").isNull(), F.lit("invalid_from_date")),
        F.when(F.col("thru_date").isNull(), F.lit("invalid_thru_date")),
        F.when(F.col("thru_date") < F.col("from_date"), F.lit("thru_before_from")),
        F.when(F.col("payment_amt").isNull(), F.lit("missing_payment")),
    ]
    checked = df.withColumn("_dq_failures", F.array_compact(F.array(*checks)))
    valid = checked.where(F.size("_dq_failures") == 0).drop("_dq_failures")
    quarantined = checked.where(F.size("_dq_failures") > 0).withColumn(
        "quarantine_reason", F.concat_ws(",", "_dq_failures")
    ).drop("_dq_failures")
    return valid, quarantined


def standardized_bronze_claims(spark: SparkSession, cfg: LakeConfig) -> DataFrame:
    frames = [
        standardize_claims(read_delta(spark, cfg.bronze(table)), claim_type)
        for table, claim_type in CLAIM_SOURCES.items()
    ]
    return frames[0].unionByName(frames[1])


def build_claims(spark: SparkSession, cfg: LakeConfig, batch_id: str) -> dict[str, int]:
    all_versions = standardized_bronze_claims(spark, cfg)
    touched = all_versions.where(F.col("_batch_id") == batch_id).select("claim_type", "claim_id").distinct()

    # Rebuild every claim touched by this batch from the latest version of all
    # its segments, so an adjustment to one segment keeps the other segment.
    latest = latest_per_key(
        all_versions.join(touched, ["claim_type", "claim_id"], "left_semi"),
        ["claim_type", "claim_id", "segment"],
    )
    claims = enrich_claims(consolidate_segments(latest)).withColumn("_batch_id", F.lit(batch_id)).cache()
    valid, quarantined = validate_claims(claims)
    processed_at = F.current_timestamp()

    merge_upsert(valid.withColumn("_processed_at", processed_at), cfg.silver("claims"), ["claim_id", "claim_type"])
    overwrite_partition(
        quarantined.withColumn("_processed_at", processed_at), cfg.silver("claims_quarantine"), "_batch_id", batch_id
    )
    metrics = {"claims_valid": valid.count(), "claims_quarantined": quarantined.count()}
    claims.unpersist()
    return metrics


def run(spark: SparkSession, cfg: LakeConfig, batch_id: str) -> dict[str, int]:
    return {"beneficiaries": build_beneficiary(spark, cfg, batch_id), **build_claims(spark, cfg, batch_id)}
