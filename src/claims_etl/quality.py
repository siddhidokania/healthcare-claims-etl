"""Data quality rules for silver and gold tables.

Results are appended to ops/dq_results. Any failed rule with severity "error"
stops the pipeline; "warn" rules are recorded only.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from claims_etl.config import LakeConfig
from claims_etl.delta_utils import read_delta


class DataQualityError(Exception):
    pass


@dataclass(frozen=True)
class Rule:
    table: str              # "<layer>.<table>", e.g. "silver.claims"
    name: str
    kind: str               # "expect": every row must satisfy `expr`; "unique": `columns` are unique
    severity: str           # "error" or "warn"
    expr: str = ""
    columns: list[str] = field(default_factory=list)
    where: str = ""         # optional filter applied before the check


RULES = [
    # silver.claims
    Rule("silver.claims", "claim_id_not_null", "expect", "error", expr="claim_id IS NOT NULL"),
    Rule("silver.claims", "claim_key_unique", "unique", "error", columns=["claim_id", "claim_type"]),
    Rule("silver.claims", "claim_type_valid", "expect", "error", expr="claim_type IN ('IP', 'OP')"),
    Rule("silver.claims", "thru_on_or_after_from", "expect", "error", expr="thru_date >= from_date"),
    Rule("silver.claims", "ip_length_of_stay_not_negative", "expect", "error",
         expr="length_of_stay_days >= 0", where="claim_type = 'IP'"),
    Rule("silver.claims", "payment_not_negative", "expect", "warn", expr="payment_amt >= 0"),
    Rule("silver.claims", "primary_diagnosis_present", "expect", "warn", expr="primary_dx_code IS NOT NULL"),
    # silver.beneficiary
    Rule("silver.beneficiary", "patient_year_unique", "unique", "error", columns=["patient_id", "coverage_year"]),
    Rule("silver.beneficiary", "birth_date_plausible", "expect", "error",
         expr="birth_date BETWEEN DATE'1900-01-01' AND DATE'2010-12-31'"),
    Rule("silver.beneficiary", "death_after_birth", "expect", "error",
         expr="death_date IS NULL OR death_date >= birth_date"),
    # gold.dim_patient (SCD Type 2 integrity)
    Rule("gold.dim_patient", "one_current_version_per_patient", "unique", "error",
         columns=["patient_id"], where="is_current"),
    Rule("gold.dim_patient", "effective_range_valid", "expect", "error", expr="effective_to >= effective_from"),
    # gold.fact_claims
    Rule("gold.fact_claims", "claim_sk_unique", "unique", "error", columns=["claim_sk"]),
    Rule("gold.fact_claims", "provider_found", "expect", "error", expr="provider_sk <> -1"),
    Rule("gold.fact_claims", "from_date_in_calendar", "expect", "error",
         expr="from_date_key BETWEEN 20070101 AND 20111231"),
    Rule("gold.fact_claims", "patient_found", "expect", "warn", expr="patient_sk <> -1"),
]


def load_table(spark: SparkSession, cfg: LakeConfig, table: str) -> DataFrame:
    layer, name = table.split(".")
    return read_delta(spark, getattr(cfg, layer)(name))


def failed_rows(df: DataFrame, rule: Rule) -> int:
    if rule.kind == "expect":
        return df.where(f"NOT coalesce({rule.expr}, false)").count()
    if rule.kind == "unique":
        duplicates = df.groupBy(*rule.columns).count().where("count > 1")
        return int(duplicates.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0])
    raise ValueError(f"Unknown rule kind {rule.kind}")


def run(spark: SparkSession, cfg: LakeConfig, run_id: str, batch_id: str, rules: list[Rule] = RULES) -> list[dict]:
    results = []
    for table in dict.fromkeys(r.table for r in rules):
        df = load_table(spark, cfg, table).cache()
        for rule in (r for r in rules if r.table == table):
            scoped = df.where(rule.where) if rule.where else df
            total = scoped.count()
            failed = failed_rows(scoped, rule)
            status = "PASS" if failed == 0 else ("FAIL" if rule.severity == "error" else "WARN")
            results.append({
                "run_id": run_id,
                "batch_id": batch_id,
                "table_name": table,
                "rule_name": rule.name,
                "severity": rule.severity,
                "total_rows": total,
                "failed_rows": failed,
                "failed_pct": round(100.0 * failed / total, 4) if total else 0.0,
                "status": status,
                "checked_at": datetime.now(timezone.utc),
            })
        df.unpersist()

    spark.createDataFrame(results).write.format("delta").mode("append").save(cfg.ops("dq_results"))

    failures = [r for r in results if r["status"] == "FAIL"]
    if failures:
        names = ", ".join(f"{r['table_name']}.{r['rule_name']} ({r['failed_rows']} rows)" for r in failures)
        raise DataQualityError(f"Data quality checks failed: {names}")
    return results
