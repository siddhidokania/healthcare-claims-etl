"""Bronze: raw Parquet from the extract, stored as Delta with load metadata.

Bronze keeps every version of every source row (partitioned by batch), so
later layers can always be rebuilt from it.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from claims_etl.config import BRONZE_TABLES, LakeConfig
from claims_etl.delta_utils import overwrite_partition


def read_manifests(spark: SparkSession, cfg: LakeConfig, batch_id: str) -> DataFrame:
    return spark.read.json(cfg.manifests(batch_id))


def manifest_row_count(manifests: DataFrame, table: str) -> int:
    row = manifests.where(F.col("table_name") == table).select("row_count").first()
    if row is None:
        raise FileNotFoundError(f"No manifest for {table}; did the extract for this batch run?")
    return int(row["row_count"])


def ingest_table(spark: SparkSession, cfg: LakeConfig, table: str, batch_id: str, expected_rows: int) -> int:
    if expected_rows == 0:
        return 0
    df = (
        spark.read.parquet(cfg.raw(table, batch_id))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    overwrite_partition(df, cfg.bronze(table), "_batch_id", batch_id)
    return expected_rows


def run(spark: SparkSession, cfg: LakeConfig, batch_id: str) -> dict[str, int]:
    manifests = read_manifests(spark, cfg, batch_id)
    return {
        table: ingest_table(spark, cfg, table, batch_id, manifest_row_count(manifests, table))
        for table in BRONZE_TABLES
    }
