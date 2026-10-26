"""Small Delta Lake helpers shared by the layers."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def table_exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def surrogate_key(*cols: str):
    """Deterministic 64-bit surrogate key, so reruns produce the same keys."""
    return F.xxhash64(*[F.col(c) for c in cols])


def overwrite_partition(df: DataFrame, path: str, partition_col: str, value: str) -> None:
    """Idempotent write of one partition: rerunning a batch replaces it."""
    writer = df.write.format("delta").option("mergeSchema", "true")
    if table_exists(df.sparkSession, path):
        writer.mode("overwrite").option("replaceWhere", f"{partition_col} = '{value}'").save(path)
    else:
        writer.mode("overwrite").partitionBy(partition_col).save(path)


def merge_upsert(df: DataFrame, path: str, keys: list[str], partition_by: list[str] | None = None) -> None:
    """Insert new rows and update existing ones, matched on `keys`."""
    spark = df.sparkSession
    if not table_exists(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def insert_new(df: DataFrame, path: str, keys: list[str]) -> None:
    """Insert rows whose keys are not in the table yet (used for type 1 dims)."""
    spark = df.sparkSession
    if not table_exists(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
