# Databricks notebook source
# MAGIC %md
# MAGIC ## 05 Optimize
# MAGIC Compacts small files and Z-orders the fact table by `patient_sk` and `from_date_key`
# MAGIC (it is already partitioned by `claim_year`).

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from claims_etl import optimize

with audit.step(spark, cfg, run_id, batch_id, "optimize") as metrics:
    metrics.update(optimize.run(spark, cfg))

display(spark.createDataFrame(list(metrics.items()), "table string, files_compacted long"))
