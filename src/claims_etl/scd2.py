"""Slowly Changing Dimension Type 2 with a single Delta MERGE.

For every incoming record:
  * new key            -> insert a first version (effective from 1900-01-01)
  * tracked attributes changed -> close the current version and insert a new one
  * nothing changed    -> no write, so reruns are safe
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from claims_etl.delta_utils import table_exists

OPEN_END = "9999-12-31"
FIRST_START = "1900-01-01"


def with_record_hash(df: DataFrame, tracked_cols: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in tracked_cols]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("||", *parts), 256))


def apply_scd2(
    spark: SparkSession,
    path: str,
    incoming: DataFrame,
    key: str,
    surrogate_key: str,
    tracked_cols: list[str],
    unknown_row: DataFrame | None = None,
) -> dict[str, int]:
    """`incoming` needs `key`, the tracked columns and an `effective_from` date."""
    incoming = with_record_hash(incoming, tracked_cols)

    def finalize(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("effective_to", F.lit(OPEN_END).cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn(surrogate_key, F.xxhash64(key, "effective_from"))
        )

    if not table_exists(spark, path):
        first = finalize(incoming.withColumn("effective_from", F.lit(FIRST_START).cast("date")))
        if unknown_row is not None:
            first = first.unionByName(unknown_row, allowMissingColumns=True)
        first.write.format("delta").save(path)
        return {"new": incoming.count(), "changed": 0}

    target = DeltaTable.forPath(spark, path)
    current = target.toDF().where("is_current").select(
        F.col(key).alias("_t_key"),
        F.col("record_hash").alias("_t_hash"),
        F.col("effective_from").alias("_t_from"),
    )
    compared = incoming.join(current, incoming[key] == current["_t_key"], "left")

    new_rows = compared.where(F.col("_t_key").isNull()).withColumn(
        "effective_from", F.lit(FIRST_START).cast("date")
    )
    # Only move forward in time, so replaying an old batch cannot reopen history.
    changed = compared.where(
        F.col("_t_key").isNotNull()
        & (F.col("_t_hash") != F.col("record_hash"))
        & (F.col("effective_from") > F.col("_t_from"))
    )
    helper_cols = ["_t_key", "_t_hash", "_t_from"]
    new_rows, changed = new_rows.drop(*helper_cols), changed.drop(*helper_cols)
    # Count before the MERGE: afterwards the comparison would see the new versions.
    metrics = {"new": new_rows.count(), "changed": changed.count()}
    if metrics["new"] == 0 and metrics["changed"] == 0:
        return metrics

    target_cols = target.toDF().columns
    inserts = finalize(new_rows.unionByName(changed)).select(*target_cols).withColumn("_merge_key", F.lit(None).cast("string"))
    closes = finalize(changed).select(*target_cols).withColumn("_merge_key", F.col(key))
    staged = closes.unionByName(inserts)

    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current")
        .whenMatchedUpdate(set={
            "is_current": F.lit(False),
            "effective_to": F.date_sub(F.col("s.effective_from"), 1),
        })
        .whenNotMatchedInsert(values={c: F.col(f"s.{c}") for c in target_cols})
        .execute()
    )
    return metrics
