"""Reconciliation: prove that no rows or money were lost between layers.

1. source -> bronze   (per batch)  row counts and payment totals vs the extract manifest
2. bronze -> silver   (all data)   claims and payment totals, counting quarantined claims
3. silver -> gold     (all data)   fact table vs silver claims
4. legacy -> new      (migration)  gold totals vs the legacy report, explaining the known
                                    difference: the legacy job counts continuation
                                    segments as separate claims

Results are appended to ops/reconciliation. Any FAIL stops the pipeline.
"""

from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from claims_etl.bronze import read_manifests
from claims_etl.config import CLAIM_SOURCES, LEGACY_SUMMARY, LakeConfig
from claims_etl.delta_utils import read_delta
from claims_etl.silver import latest_per_key, standardized_bronze_claims

MONEY = "decimal(18,2)"


class ReconciliationError(Exception):
    pass


def money(value) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


def totals(df: DataFrame, amount_col: str, group_col: str | None = None) -> dict:
    """{group: (rows, amount)}; a single "ALL" group when group_col is None."""
    group = F.col(group_col) if group_col else F.lit("ALL")
    rows = df.groupBy(group.alias("g")).agg(F.count("*").alias("n"), F.sum(F.col(amount_col).cast(MONEY)).alias("amt"))
    return {r["g"]: (int(r["n"]), money(r["amt"])) for r in rows.collect()}


class Checks:
    def __init__(self, run_id: str, batch_id: str):
        self.run_id, self.batch_id, self.rows = run_id, batch_id, []

    def add(self, name: str, scope: str, source, target, note: str = "") -> None:
        self.rows.append({
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "check_name": name,
            "scope": scope,
            "source_value": str(source),
            "target_value": str(target),
            "status": "PASS" if source == target else "FAIL",
            "note": note,
            "checked_at": datetime.now(timezone.utc),
        })


def source_to_bronze(spark: SparkSession, cfg: LakeConfig, checks: Checks) -> None:
    manifests = {r["table_name"]: r for r in read_manifests(spark, cfg, checks.batch_id).collect()}
    for table in ["beneficiary", *CLAIM_SOURCES]:
        expected = int(manifests[table]["row_count"])
        batch = read_delta(spark, cfg.bronze(table)).where(F.col("_batch_id") == checks.batch_id)
        checks.add("source_to_bronze_rows", table, expected, batch.count())
        if table in CLAIM_SOURCES:
            amount = money(batch.agg(F.sum(F.col("clm_pmt_amt").cast(MONEY))).first()[0])
            checks.add("source_to_bronze_payment", table, money(manifests[table]["amount_sum"]), amount)


def quarantined_only(spark: SparkSession, cfg: LakeConfig, silver: DataFrame) -> DataFrame:
    """Latest quarantined version of claims that never made it into silver."""
    quarantine = read_delta(spark, cfg.silver("claims_quarantine"))
    window = Window.partitionBy("claim_type", "claim_id").orderBy(F.col("_processed_at").desc())
    latest = quarantine.withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
    return latest.join(silver.select("claim_type", "claim_id"), ["claim_type", "claim_id"], "left_anti")


def run(spark: SparkSession, cfg: LakeConfig, run_id: str, batch_id: str) -> list[dict]:
    checks = Checks(run_id, batch_id)
    source_to_bronze(spark, cfg, checks)

    bronze_rows = latest_per_key(standardized_bronze_claims(spark, cfg), ["claim_type", "claim_id", "segment"]).cache()
    bronze_claims = bronze_rows.select("claim_type", "claim_id").distinct()
    silver = read_delta(spark, cfg.silver("claims"))
    quarantined = quarantined_only(spark, cfg, silver)
    fact = read_delta(spark, cfg.gold("fact_claims"))

    bronze_amount = totals(bronze_rows, "payment_amt", "claim_type")
    bronze_claim_counts = {r["claim_type"]: r["count"] for r in bronze_claims.groupBy("claim_type").count().collect()}
    silver_t = totals(silver, "payment_amt", "claim_type")
    quarantine_t = totals(quarantined, "payment_amt", "claim_type")
    fact_t = totals(fact, "payment_amt", "claim_type")

    legacy = spark.read.parquet(cfg.raw(LEGACY_SUMMARY, batch_id))
    legacy_t = {
        r["claim_type"]: (int(r["rows"]), money(r["amt"]))
        for r in legacy.groupBy("claim_type").agg(
            F.sum("claim_count").alias("rows"), F.sum(F.col("total_payment").cast(MONEY)).alias("amt")
        ).collect()
    }

    for claim_type in CLAIM_SOURCES.values():
        s_n, s_amt = silver_t.get(claim_type, (0, money(0)))
        q_n, q_amt = quarantine_t.get(claim_type, (0, money(0)))
        f_n, f_amt = fact_t.get(claim_type, (0, money(0)))
        b_rows, b_amt = bronze_amount.get(claim_type, (0, money(0)))
        b_claims = bronze_claim_counts.get(claim_type, 0)
        continuation_segments = b_rows - b_claims

        checks.add("bronze_to_silver_claims", claim_type, b_claims, s_n + q_n, "silver + quarantined claims")
        checks.add("bronze_to_silver_payment", claim_type, b_amt, s_amt + q_amt, "silver + quarantined claims")
        checks.add("silver_to_gold_claims", claim_type, s_n, f_n)
        checks.add("silver_to_gold_payment", claim_type, s_amt, f_amt)

        l_rows, l_amt = legacy_t.get(claim_type, (0, money(0)))
        checks.add("legacy_to_gold_payment", claim_type, l_amt, f_amt + q_amt, "gold + quarantined claims")
        checks.add(
            "legacy_to_gold_claim_count", claim_type, l_rows, f_n + q_n + continuation_segments,
            f"legacy counts {continuation_segments:,} continuation segments as separate claims",
        )
    bronze_rows.unpersist()

    spark.createDataFrame(checks.rows).write.format("delta").mode("append").save(cfg.ops("reconciliation"))

    failures = [c for c in checks.rows if c["status"] == "FAIL"]
    if failures:
        names = ", ".join(f"{c['check_name']}[{c['scope']}] {c['source_value']} vs {c['target_value']}" for c in failures)
        raise ReconciliationError(f"Reconciliation failed: {names}")
    return checks.rows
