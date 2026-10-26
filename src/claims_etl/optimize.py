"""Table maintenance: compact small files and co-locate data for common filters."""

from pyspark.sql import SparkSession

from claims_etl.config import LakeConfig

ZORDER = {
    # most reports filter by patient or by date inside a claim_year partition
    "fact_claims": ["patient_sk", "from_date_key"],
    "dim_patient": ["patient_id"],
    "bridge_claim_diagnosis": ["diagnosis_sk"],
}


def run(spark: SparkSession, cfg: LakeConfig) -> dict[str, int]:
    compacted = {}
    for table, columns in ZORDER.items():
        result = spark.sql(f"OPTIMIZE delta.`{cfg.gold(table)}` ZORDER BY ({', '.join(columns)})").first()
        compacted[table] = int(result["metrics"]["numFilesRemoved"])
    spark.sql(f"OPTIMIZE delta.`{cfg.silver('claims')}`")
    return {f"{t}_files_compacted": n for t, n in compacted.items()}
