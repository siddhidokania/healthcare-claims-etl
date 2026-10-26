# Databricks notebook source
# MAGIC %md
# MAGIC ### Shared setup
# MAGIC Run by every pipeline notebook with `%run ./_setup`. Reads the job parameters,
# MAGIC puts the repo's `src/` folder on the Python path and builds the lake config.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

# This notebook runs from a Databricks Git folder (or a job with git_source),
# so the repo's src/ folder is two levels up.
src_path = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("lake_root", "abfss://lake@<your-storage-account>.dfs.core.windows.net")
dbutils.widgets.text("run_id", "")
dbutils.widgets.dropdown("auth", "unity_catalog", ["unity_catalog", "service_principal"])

batch_id = dbutils.widgets.get("batch_id")
lake_root = dbutils.widgets.get("lake_root")
run_id = dbutils.widgets.get("run_id") or f"{batch_id}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
if not batch_id:
    raise ValueError("batch_id is required")

# COMMAND ----------

# Storage access. Preferred: a Unity Catalog external location on the lake
# container (no keys in code). Alternative: a service principal whose secret
# is in a Key Vault-backed secret scope called "claims-kv".
if dbutils.widgets.get("auth") == "service_principal":
    account = lake_root.split("@", 1)[1].split("/", 1)[0]
    tenant_id = dbutils.secrets.get("claims-kv", "sp-tenant-id")
    spark.conf.set(f"fs.azure.account.auth.type.{account}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{account}",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{account}", dbutils.secrets.get("claims-kv", "sp-client-id"))
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account}", dbutils.secrets.get("claims-kv", "sp-client-secret"))
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account}",
                   f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

from claims_etl import audit
from claims_etl.config import LakeConfig

cfg = LakeConfig(lake_root)
print(f"batch_id={batch_id}  run_id={run_id}  lake_root={lake_root}")
