# Snowflake serving layer

The gold star schema is also served from Snowflake, so BI tools can query it without Spark. This stage covers loading with reconciliation, clustering keys, warehouse sizing with query profiles, and cost guardrails.

```bash
python -m warehouse_snowflake.make_setup_sql      # fills the service user's public key into the setup SQL
#   run warehouse_snowflake/sql/00_account_setup.local.sql in Snowsight as ACCOUNTADMIN
python -m pipeline.export_bi                      # gold -> CSV
python -m warehouse_snowflake.load                # stage, COPY INTO, reconcile, build scale-test tables
python -m warehouse_snowflake.benchmark           # clustering and sizing experiments -> docs/snowflake_results.md
python -m warehouse_snowflake.finops              # cost and settings report
```

## Setup ([sql/00_account_setup.sql](../warehouse_snowflake/sql/00_account_setup.sql))
- Role `CLAIMS_ENGINEER`, database `CLAIMS_DB` (schemas `GOLD`, `OPS`).
- Three warehouses, XSMALL / SMALL / MEDIUM, all `AUTO_SUSPEND = 60` and `AUTO_RESUME = TRUE`.
- Resource monitor `CLAIMS_MONITOR`: 20 credits a month, notifications at 50% and 80%, suspend at 90%, suspend immediately (cancel running queries) at 100%, attached to all three warehouses.
- Service user `CLAIMS_SVC` (`TYPE = SERVICE`) that signs in with an RSA key pair, so the pipeline has no password. The private key stays on the machine running the pipeline (git-ignored).

## Load ([load.py](../warehouse_snowflake/load.py))
CSV exports are `PUT` into an internal stage (compressed on upload) and loaded with `COPY INTO ... ON_ERROR = ABORT_STATEMENT`. After each table, row counts (and the payment total for the fact) are compared with the source file and logged to `OPS.LOAD_AUDIT`.

| Table | Rows | Check |
|---|---|---|
| FACT_CLAIMS | 846,242 | payment $863,403,190.00 matches the lake to the cent |
| DIM_PATIENT | 307,290 | PASS |
| DIM_DIAGNOSIS | 11,936 | PASS |
| DIM_PROVIDER | 6,799 | PASS |
| DIM_DATE | 1,826 | PASS |

`FACT_CLAIMS` is declared with `CLUSTER BY (from_date_key)`, since most reports filter on the claim date.

## Clustering
846K claims fit in a few micro-partitions, too few for pruning to matter, so the benchmark uses a 16.9M-row scale-test copy (every claim repeated 20 times) in two versions: random order, and clustered on `from_date_key`. Partition counts come from each query's profile (`GET_QUERY_OPERATOR_STATS`).

| | Unclustered | Clustered |
|---|---|---|
| Clustering depth on `from_date_key` | 32.0 | 2.0 |
| Micro-partitions | 32 | 16 |
| Storage (compressed) | 533 MB | 270 MB |
| One-month query: partitions scanned | 32 of 32 | **2 of 16** |
| One-year query: partitions scanned | 32 of 32 | **8 of 16** |
| One-month query: elapsed (median) | 84 ms | 62 ms |
| One-year query with join: elapsed (median) | 223 ms | 142 ms |

Clustering cut the partitions read by a one-month query by **94%** (2 instead of 32) and, because sorted data compresses better, **halved the storage**.

## Warehouse sizing
The same four report queries ran on XSMALL, SMALL and MEDIUM (median of 3 runs, result cache off). See [snowflake_results.md](snowflake_results.md) for every number.

At this data size every query finished in 60-270 ms on all three sizes; larger warehouses were not faster but cost 2-4 times as many credits per second. **XSMALL is the right size** for this workload. A larger warehouse would only pay off for much bigger scans or many concurrent users (and concurrency is better handled with a multi-cluster warehouse than a bigger one).

## Cost (FinOps)
`finops.py` checks every project warehouse for auto-suspend over 5 minutes and a missing resource monitor, and reports credits by warehouse and storage by table. After the full load and benchmark:

| | |
|---|---|
| Credits used | 0.21 of the 20-credit monthly quota (XSMALL 0.079, SMALL 0.045, MEDIUM 0.090) |
| Warehouse settings | all three: 60 s auto-suspend, monitor attached |
| Biggest storage cost | the unclustered scale-test table (533 MB); drop the scale-test tables after benchmarking |

The MEDIUM warehouse used the most credits while running the fewest queries, which is the sizing result above showing up in the bill.
