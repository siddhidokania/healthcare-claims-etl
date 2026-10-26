# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 Gold
# MAGIC Star schema: `dim_date`, `dim_provider`, `dim_diagnosis`, `dim_patient` (SCD Type 2),
# MAGIC `fact_claims` (partitioned by `claim_year`) and `bridge_claim_diagnosis`.
# MAGIC Small dimensions are broadcast into the fact build.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from claims_etl import gold

with audit.step(spark, cfg, run_id, batch_id, "gold") as metrics:
    metrics.update(gold.run(spark, cfg, batch_id))

display(spark.createDataFrame(list(metrics.items()), "metric string, value long"))

# COMMAND ----------

# MAGIC %md Example report: payment by year, claim type and ICD-9 chapter of the primary diagnosis

# COMMAND ----------

fact = spark.read.format("delta").load(cfg.gold("fact_claims"))
dx = spark.read.format("delta").load(cfg.gold("dim_diagnosis"))
display(
    fact.join(dx, fact.primary_diagnosis_sk == dx.diagnosis_sk)
    .groupBy("claim_year", "claim_type", "icd9_chapter")
    .agg({"payment_amt": "sum", "claim_sk": "count"})
    .orderBy("claim_year", "claim_type")
)
