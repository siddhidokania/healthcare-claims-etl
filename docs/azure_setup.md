# Azure setup guide

How to run the pipeline on Azure: ADF moves data from the on-prem PostgreSQL to ADLS Gen2, Databricks builds bronze/silver/gold, and Airflow runs both.

> **Cost:** an *Azure for Students* subscription (free credit, no card) or the Azure free trial covers this project. Use the single-node job cluster in `databricks/jobs/claims_pipeline_job.json`; it shuts down when the job ends. Delete the resource group when you are done.

## 1. Resource group and storage

1. Create a resource group, for example `rg-claims-etl` in *Central India*.
2. Create a **Storage account** with **Hierarchical namespace enabled** (that makes it ADLS Gen2), Standard / LRS.
3. In the storage account, create a container named **`lake`**.

## 2. Key Vault

1. Create a **Key Vault** in the same resource group.
2. Add a secret **`onprem-postgres-password`** with the PostgreSQL password from your `.env`.

## 3. Data Factory

1. Create a **Data Factory**. It gets a system-assigned managed identity.
2. Give that identity these roles (*Access control (IAM) → Add role assignment*):
   - **Storage Blob Data Contributor** on the storage account
   - **Key Vault Secrets User** on the Key Vault
3. **Self-hosted integration runtime** (this is what makes it "on-prem to cloud"):
   - ADF Studio → *Manage → Integration runtimes → New → Self-Hosted*, name it **`ir-onprem-shir`**.
   - Download and install the runtime on the laptop that runs Docker, and register it with the key ADF shows.
   - The runtime reaches PostgreSQL at `localhost:5433`, so keep `docker compose up -d onprem-db` running.
4. **Import the pipeline.** Either:
   - connect ADF to this GitHub repo (*Manage → Git configuration*, root folder `/adf`), or
   - create the linked services, datasets and pipeline by hand, copying the JSON from `adf/`.
5. Replace `<your-storage-account>` and `<your-key-vault>` in `adf/linkedService/*.json`.
6. Open `ls_onprem_postgres` and press **Test connection**. The JSON uses the *PostgreSQL V2* connector; if your ADF version shows different property names, keep the values and let the UI adjust the JSON.
7. **Debug** the pipeline with `batch_id = 2008`. You should see `raw/<table>/batch_id=2008/` and `raw/_manifests/batch_id=2008/` in the `lake` container.

## 4. Databricks

1. Create an **Azure Databricks** workspace (*Premium* tier gives Unity Catalog; a 14-day free trial is offered).
2. Give Databricks access to the lake. Pick one:
   - **Unity Catalog (recommended):** create an *Access Connector for Azure Databricks*, give it **Storage Blob Data Contributor** on the storage account, then in Databricks create a *storage credential* and an *external location* for `abfss://lake@<account>.dfs.core.windows.net/`. Use job parameter `auth = unity_catalog`.
   - **Service principal:** create an app registration with a client secret, give it **Storage Blob Data Contributor**, store `sp-tenant-id`, `sp-client-id` and `sp-client-secret` in a Key Vault-backed secret scope called `claims-kv`, and use `auth = service_principal`.
3. Push this repo to GitHub, then create the job with the Databricks CLI (after replacing `<your-storage-account>` in the JSON):

   ```bash
   databricks jobs create --json @databricks/jobs/claims_pipeline_job.json
   ```

4. Run the job once by hand with `batch_id = 2008` to check it.

## 5. Airflow

1. Start Airflow locally:

   ```bash
   docker compose --profile airflow up -d airflow
   ```

   Open http://localhost:8080. The admin password is printed in the container log (`docker logs claims_airflow`).
2. *Admin → Connections*:
   - **`azure_data_factory_default`** (type *Azure Data Factory*): a service principal's client id and secret, tenant id, subscription id, and in extras `resource_group_name` and `factory_name`. Give that service principal the **Data Factory Contributor** role on the factory.
   - **`databricks_default`** (type *Databricks*): the workspace URL as host and a personal access token as password.
3. Trigger **`claims_pipeline`** with `{"batch_id": "2008"}`.

## 6. Running the three yearly batches

For each year, load the source first, then trigger the DAG with the same batch id:

```bash
docker compose run --rm runner python -m onprem.load_source --year 2008
docker compose run --rm runner python -m onprem.legacy_etl
# trigger claims_pipeline with {"batch_id": "2008"}
docker compose run --rm runner python -m onprem.load_source --year 2009 --simulate-adjustments 500
docker compose run --rm runner python -m onprem.legacy_etl
# trigger claims_pipeline with {"batch_id": "2009"}
```

Batches must run in order, because watermarks and the SCD Type 2 patient history move forward in time.

## 7. Clean up

Delete the resource group `rg-claims-etl` to stop all charges.
