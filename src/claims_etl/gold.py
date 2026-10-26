"""Gold: star schema for claims reporting.

    dim_date, dim_provider, dim_diagnosis   type 1 dimensions
    dim_patient                             type 2 dimension (history per year)
    fact_claims                             one row per claim
    bridge_claim_diagnosis                  all diagnoses on a claim

Every dimension has an "unknown" member with key -1, so facts never lose rows
when a lookup fails.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from claims_etl import icd9
from claims_etl.config import UNKNOWN_KEY, LakeConfig
from claims_etl.delta_utils import insert_new, merge_upsert, read_delta, surrogate_key, table_exists
from claims_etl.scd2 import apply_scd2
from claims_etl.silver import CHRONIC_CONDITIONS

DATE_RANGE = ("2007-01-01", "2011-12-31")

PATIENT_TRACKED = [
    "birth_date", "death_date", "sex", "race", "has_esrd", "state_code", "county_code",
    "part_a_coverage_months", "part_b_coverage_months", "hmo_coverage_months",
    "part_d_coverage_months", *CHRONIC_CONDITIONS,
]


def date_key(col_name: str):
    return F.date_format(col_name, "yyyyMMdd").cast("int")


def maybe_broadcast(df: DataFrame, enabled: bool) -> DataFrame:
    return F.broadcast(df) if enabled else df


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

def build_dim_date(spark: SparkSession, cfg: LakeConfig) -> int:
    start, end = DATE_RANGE
    dates = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS full_date")
    dim = dates.select(
        date_key("full_date").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )
    dim.write.format("delta").mode("overwrite").save(cfg.gold("dim_date"))
    return dim.count()


def build_dim_provider(spark: SparkSession, cfg: LakeConfig, claims: DataFrame, batch_id: str) -> None:
    providers = (
        claims.where(F.col("provider_num").isNotNull())
        .select("provider_num").distinct()
        .withColumn("provider_sk", surrogate_key("provider_num"))
        # The first two characters of a CMS provider number are the state code.
        .withColumn("provider_state_code", F.substring("provider_num", 1, 2))
        .withColumn("first_seen_batch_id", F.lit(batch_id))
    )
    if not table_exists(spark, cfg.gold("dim_provider")):
        unknown = spark.createDataFrame(
            [(UNKNOWN_KEY, "UNKNOWN", None, None)],
            "provider_sk bigint, provider_num string, provider_state_code string, first_seen_batch_id string",
        )
        providers = providers.unionByName(unknown)
    insert_new(providers.select("provider_sk", "provider_num", "provider_state_code", "first_seen_batch_id"),
               cfg.gold("dim_provider"), ["provider_sk"])


def build_dim_diagnosis(spark: SparkSession, cfg: LakeConfig, claims: DataFrame, batch_id: str) -> None:
    codes = (
        claims.select(F.explode("diagnosis_codes").alias("icd9_code"))
        .unionByName(claims.select(F.col("admitting_dx_code").alias("icd9_code")))
        .where(F.col("icd9_code").isNotNull())
        .distinct()
    )
    diagnoses = codes.select(
        surrogate_key("icd9_code").alias("diagnosis_sk"),
        "icd9_code",
        icd9.formatted("icd9_code").alias("icd9_code_display"),
        icd9.chapter(F.col("icd9_code")).alias("icd9_chapter"),
        F.lit(batch_id).alias("first_seen_batch_id"),
    )
    if not table_exists(spark, cfg.gold("dim_diagnosis")):
        unknown = spark.createDataFrame(
            [(UNKNOWN_KEY, "UNKNOWN", "UNKNOWN", icd9.UNCLASSIFIED, None)],
            "diagnosis_sk bigint, icd9_code string, icd9_code_display string, icd9_chapter string, "
            "first_seen_batch_id string",
        )
        diagnoses = diagnoses.unionByName(unknown)
    insert_new(diagnoses, cfg.gold("dim_diagnosis"), ["diagnosis_sk"])


def patient_attributes(beneficiaries: DataFrame) -> DataFrame:
    condition_count = sum(F.col(c).cast("int") for c in CHRONIC_CONDITIONS)
    return beneficiaries.select(
        "patient_id",
        *PATIENT_TRACKED,
        condition_count.alias("chronic_condition_count"),
        F.col("coverage_year").alias("source_coverage_year"),
        F.make_date("coverage_year", F.lit(1), F.lit(1)).alias("effective_from"),
    )


def build_dim_patient(spark: SparkSession, cfg: LakeConfig, batch_id: str) -> dict[str, int]:
    beneficiaries = read_delta(spark, cfg.silver("beneficiary")).where(F.col("_batch_id") == batch_id)
    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, "UNKNOWN", "Unknown", "Unknown")],
        "patient_sk bigint, patient_id string, sex string, race string",
    ).select(
        "*",
        F.lit("1900-01-01").cast("date").alias("effective_from"),
        F.lit("9999-12-31").cast("date").alias("effective_to"),
        F.lit(True).alias("is_current"),
    )
    return apply_scd2(
        spark,
        cfg.gold("dim_patient"),
        patient_attributes(beneficiaries),
        key="patient_id",
        surrogate_key="patient_sk",
        tracked_cols=PATIENT_TRACKED,
        unknown_row=unknown,
    )


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------

def fact_rows(spark: SparkSession, cfg: LakeConfig, claims: DataFrame, broadcast_dims: bool = True) -> DataFrame:
    patients = read_delta(spark, cfg.gold("dim_patient")).select(
        F.col("patient_id").alias("p_patient_id"), "patient_sk", "effective_from", "effective_to"
    )
    providers = maybe_broadcast(
        read_delta(spark, cfg.gold("dim_provider")).select(F.col("provider_num").alias("p_provider_num"), "provider_sk"),
        broadcast_dims,
    )
    diagnoses = read_delta(spark, cfg.gold("dim_diagnosis")).select("icd9_code", "diagnosis_sk")
    primary_dx = maybe_broadcast(diagnoses.toDF("primary_code", "primary_diagnosis_sk"), broadcast_dims)
    admitting_dx = maybe_broadcast(diagnoses.toDF("admitting_code", "admitting_diagnosis_sk"), broadcast_dims)

    # Point-in-time lookup: the patient version that was valid on the claim date.
    joined = (
        claims.join(
            patients,
            (F.col("patient_id") == F.col("p_patient_id"))
            & F.col("from_date").between(F.col("effective_from"), F.col("effective_to")),
            "left",
        )
        .join(providers, F.col("provider_num") == F.col("p_provider_num"), "left")
        .join(primary_dx, F.col("primary_dx_code") == F.col("primary_code"), "left")
        .join(admitting_dx, F.col("admitting_dx_code") == F.col("admitting_code"), "left")
    )
    unknown = F.lit(UNKNOWN_KEY).cast("bigint")
    return joined.select(
        surrogate_key("claim_id", "claim_type").alias("claim_sk"),
        "claim_id",
        "claim_type",
        F.coalesce("patient_sk", unknown).alias("patient_sk"),
        F.coalesce("provider_sk", unknown).alias("provider_sk"),
        F.coalesce("primary_diagnosis_sk", unknown).alias("primary_diagnosis_sk"),
        F.coalesce("admitting_diagnosis_sk", unknown).alias("admitting_diagnosis_sk"),
        date_key("from_date").alias("from_date_key"),
        date_key("thru_date").alias("thru_date_key"),
        date_key("admission_date").alias("admission_date_key"),
        date_key("discharge_date").alias("discharge_date_key"),
        "drg_code",
        "attending_npi",
        "payment_amt",
        "primary_payer_paid_amt",
        "deductible_amt",
        "coinsurance_amt",
        "blood_deductible_amt",
        "pass_thru_per_diem_amt",
        "utilization_days",
        "length_of_stay_days",
        F.size("diagnosis_codes").alias("diagnosis_count"),
        F.size("procedure_codes").alias("procedure_count"),
        "segment_count",
        "is_adjustment",
        "claim_year",
        "_batch_id",
        F.current_timestamp().alias("_loaded_at"),
    )


def build_fact_claims(spark: SparkSession, cfg: LakeConfig, claims: DataFrame, broadcast_dims: bool = True) -> int:
    facts = fact_rows(spark, cfg, claims, broadcast_dims)
    merge_upsert(facts, cfg.gold("fact_claims"), ["claim_sk"], partition_by=["claim_year"])
    return claims.count()


def build_bridge_diagnosis(spark: SparkSession, cfg: LakeConfig, claims: DataFrame) -> None:
    rows = (
        claims.select(
            surrogate_key("claim_id", "claim_type").alias("claim_sk"),
            "primary_dx_code",
            F.posexplode("diagnosis_codes").alias("pos", "icd9_code"),
        )
        .select(
            "claim_sk",
            surrogate_key("icd9_code").alias("diagnosis_sk"),
            (F.col("pos") + 1).alias("diagnosis_sequence"),
            (F.col("icd9_code") == F.col("primary_dx_code")).alias("is_primary"),
        )
    )
    path = cfg.gold("bridge_claim_diagnosis")
    if table_exists(spark, path):
        # Replace the bridge rows of every claim in this batch.
        touched = claims.select(surrogate_key("claim_id", "claim_type").alias("claim_sk")).distinct()
        DeltaTable.forPath(spark, path).alias("t").merge(
            touched.alias("s"), "t.claim_sk = s.claim_sk"
        ).whenMatchedDelete().execute()
        rows.write.format("delta").mode("append").save(path)
    else:
        rows.write.format("delta").save(path)


def run(spark: SparkSession, cfg: LakeConfig, batch_id: str, broadcast_dims: bool = True) -> dict[str, int]:
    claims = read_delta(spark, cfg.silver("claims")).where(F.col("_batch_id") == batch_id).cache()
    if not table_exists(spark, cfg.gold("dim_date")):
        build_dim_date(spark, cfg)
    build_dim_provider(spark, cfg, claims, batch_id)
    build_dim_diagnosis(spark, cfg, claims, batch_id)
    patient_metrics = build_dim_patient(spark, cfg, batch_id)
    fact_count = build_fact_claims(spark, cfg, claims, broadcast_dims)
    build_bridge_diagnosis(spark, cfg, claims)
    claims.unpersist()
    return {
        "patients_new": patient_metrics["new"],
        "patient_versions_added": patient_metrics["changed"],
        "fact_claims_upserted": fact_count,
    }
