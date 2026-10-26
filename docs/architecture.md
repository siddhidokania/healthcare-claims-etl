# Architecture decisions

### Watermarks on `updated_at`, read as text
Incremental extraction uses a per-table watermark in `etl_control.watermark`. The extract copies rows in `(old, new]` and only moves the watermark after the data and manifest are written, so a failed run is simply rerun. ADF reads watermarks as text (`::text`) because its Lookup activity rounds timestamps, which would skip or repeat rows.

### Manifests for reconciliation
Each extract writes the source row count and payment total next to the data. The Databricks reconciliation step compares them with what landed in bronze, so lost rows are detected even if every job reports success.

### Bronze keeps every version
Bronze is append-per-batch (partitioned by `_batch_id`) and keeps every version of every source row. Silver and gold can be rebuilt from it, and the silver step uses it to rebuild a whole claim when only one segment changes.

### Segment consolidation
In DE-SynPUF a claim can have a continuation segment that carries amounts but no dates or diagnoses. Silver groups by claim: header fields come from segment 1 and amounts are summed. The legacy job counted these segments as separate claims. Reconciliation shows that difference explicitly instead of hiding it.

### Quarantine instead of drop
Claims that fail validation (no dates, end before start, missing ids) go to `silver/claims_quarantine` with a reason. 278 orphan outpatient segments land there. Reconciliation counts silver plus quarantine, so every source row is accounted for.

### Deterministic surrogate keys
Keys are `xxhash64` of the natural key (and `effective_from` for patients). Reruns produce the same keys and facts can be built without a lookup table. Each dimension has an unknown member `-1`.

### SCD Type 2 with one MERGE
`scd2.apply_scd2` compares a SHA-256 hash of the tracked attributes with the current version, then runs a single Delta `MERGE` that closes changed versions and inserts new ones. Unchanged records cause no writes, and replaying an older batch cannot reopen history.

### Point-in-time fact lookup
`fact_claims.patient_sk` is the patient version valid on the claim's `from_date`, so a 2008 claim keeps the patient's 2008 conditions even after the 2009 load.

### Idempotent writes everywhere
Bronze uses `replaceWhere` on the batch partition, silver and gold use `MERGE`, and the bridge table deletes then inserts the rows of the touched claims. Any step can be rerun safely.

### Performance
- `fact_claims` is partitioned by `claim_year` and Z-ordered by `patient_sk` and `from_date_key`.
- Small dimensions are broadcast into the fact build. Measured locally on 846,242 claims: 17.9 s with sort-merge joins, 6.8 s with broadcast joins (62% faster). Run `python -m pipeline.benchmark_fact_join`.
- `OPTIMIZE` compacts the small files that incremental MERGEs create.

### One codebase for local and Databricks
All logic lives in the `claims_etl` package. Databricks notebooks are thin wrappers that pass parameters, and the local runner calls the same functions. The local Spark and Delta versions match Databricks Runtime 15.4 LTS, so the unit tests cover the code that runs in the cloud.

### Known limitations
- Batches must run in order (watermarks and SCD Type 2 move forward in time); Airflow uses `max_active_runs=1`.
- If a claim that was valid becomes invalid in a later batch, silver keeps the last valid version and the new version is quarantined.
- The ADF JSON uses the PostgreSQL V2 connector. Check the property names in the ADF UI when importing, because the connector schema changes between ADF releases.
