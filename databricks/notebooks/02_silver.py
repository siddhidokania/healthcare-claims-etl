# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 Silver
# MAGIC * Beneficiaries: decode codes, chronic condition flags, one row per patient per year
# MAGIC * Claims: common inpatient/outpatient layout, latest version per segment,
# MAGIC   segments consolidated into one row per claim, invalid claims quarantined
# MAGIC * Both are merged into silver with Delta `MERGE`, so reruns do not duplicate rows

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from claims_etl import silver

with audit.step(spark, cfg, run_id, batch_id, "silver") as metrics:
    metrics.update(silver.run(spark, cfg, batch_id))

display(spark.createDataFrame(list(metrics.items()), "metric string, value long"))

# COMMAND ----------

# MAGIC %md Quarantined claims in this batch, by reason

# COMMAND ----------

display(
    spark.read.format("delta").load(cfg.silver("claims_quarantine"))
    .where(f"_batch_id = '{batch_id}'")
    .groupBy("claim_type", "quarantine_reason").count()
)
