"""Orchestrates one batch of the claims pipeline on Azure.

    adf_ingest_to_raw          ADF: on-prem PostgreSQL -> ADLS raw (incremental, watermark based)
    databricks_bronze_to_gold  Databricks job: bronze -> silver -> gold -> DQ + reconciliation -> optimize

Connections (Admin -> Connections):
    azure_data_factory_default  type "Azure Data Factory": service principal, tenant id,
                                subscription id, and extras resource_group_name + factory_name
    databricks_default          type "Databricks": workspace URL + personal access token

Trigger with a batch id, e.g. {"batch_id": "2009"}.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="claims_pipeline",
    description="On-prem claims -> ADF -> ADLS -> Databricks medallion pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # one run per source batch; switch to "@daily" for a nightly load
    catchup=False,
    max_active_runs=1,      # batches must run in order (watermarks and SCD Type 2)
    default_args=default_args,
    params={"batch_id": Param("2008", type="string", description="Batch to process, e.g. 2008")},
    tags=["claims", "adf", "databricks"],
) as dag:

    ingest = AzureDataFactoryRunPipelineOperator(
        task_id="adf_ingest_to_raw",
        azure_data_factory_conn_id="azure_data_factory_default",
        pipeline_name="pl_ingest_claims_incremental",
        parameters={"batch_id": "{{ params.batch_id }}"},
        wait_for_termination=True,
        check_interval=30,
        timeout=60 * 60,
    )

    transform = DatabricksRunNowOperator(
        task_id="databricks_bronze_to_gold",
        databricks_conn_id="databricks_default",
        job_name="claims-pipeline",
        job_parameters={"batch_id": "{{ params.batch_id }}"},
    )

    ingest >> transform
