# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 Bronze
# MAGIC Raw Parquet written by ADF (`raw/<table>/batch_id=<batch>/`) is stored as Delta,
# MAGIC partitioned by batch, with load metadata. Rerunning a batch replaces its partition.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from claims_etl import bronze

with audit.step(spark, cfg, run_id, batch_id, "bronze") as metrics:
    metrics.update(bronze.run(spark, cfg, batch_id))

display(spark.createDataFrame(list(metrics.items()), "table string, rows long"))
