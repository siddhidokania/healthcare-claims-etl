"""Source tables extracted from the on-prem database.

The same list is used by the ADF pipeline (pipeline parameter `tables`) and by
the local extract script, so both write identical files to the lake.
"""

SOURCE_TABLES = [
    # incremental: only rows with updated_at after the last watermark
    {"table_name": "beneficiary", "schema": "claims_src", "mode": "incremental", "amount_column": None},
    {"table_name": "inpatient_claims", "schema": "claims_src", "mode": "incremental", "amount_column": "clm_pmt_amt"},
    {"table_name": "outpatient_claims", "schema": "claims_src", "mode": "incremental", "amount_column": "clm_pmt_amt"},
    # full snapshot of the legacy report, used only for migration reconciliation
    {"table_name": "claims_monthly_summary", "schema": "legacy_rpt", "mode": "full", "amount_column": "total_payment"},
]
