# Databricks notebook source
# MAGIC %md
# MAGIC ## 04 Data quality and reconciliation
# MAGIC * Data quality rules on silver and gold (`ops/dq_results`); error-level failures stop the job
# MAGIC * Reconciliation source → bronze → silver → gold, and legacy report → gold (`ops/reconciliation`)

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from claims_etl import quality, reconcile

with audit.step(spark, cfg, run_id, batch_id, "quality") as metrics:
    dq_results = quality.run(spark, cfg, run_id, batch_id)
    metrics.update({s: sum(r["status"] == s for r in dq_results) for s in ("PASS", "WARN", "FAIL")})

display(spark.createDataFrame(dq_results))

# COMMAND ----------

with audit.step(spark, cfg, run_id, batch_id, "reconcile") as metrics:
    recon_results = reconcile.run(spark, cfg, run_id, batch_id)
    metrics.update({s: sum(r["status"] == s for r in recon_results) for s in ("PASS", "FAIL")})

display(spark.createDataFrame(recon_results))
