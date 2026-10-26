# Data model

## Lake layout

```
lake/
├── raw/                                   written by ADF (or the local extract)
│   ├── <table>/batch_id=<batch>/          Parquet, one folder per extract
│   └── _manifests/batch_id=<batch>/       source row count + amount total per table
├── bronze/                                Delta, partitioned by _batch_id, every version of every row
│   ├── beneficiary
│   ├── inpatient_claims
│   └── outpatient_claims
├── silver/                                Delta, cleaned and de-duplicated
│   ├── beneficiary                        one row per patient per coverage year
│   ├── claims                             one row per claim (IP + OP)
│   └── claims_quarantine                  claims that failed validation, with the reason
├── gold/                                  Delta, star schema
│   ├── dim_date
│   ├── dim_patient                        SCD Type 2
│   ├── dim_provider
│   ├── dim_diagnosis
│   ├── fact_claims                        partitioned by claim_year
│   └── bridge_claim_diagnosis
└── ops/                                   pipeline_runs, dq_results, reconciliation
```

## Star schema

```
                 dim_date
                    │ from_date_key / thru_date_key / admission_date_key / discharge_date_key
                    │
dim_patient ── fact_claims ── dim_provider
 (SCD 2)            │
                    ├── dim_diagnosis (primary and admitting diagnosis)
                    │
         bridge_claim_diagnosis ── dim_diagnosis (all diagnoses on the claim)
```

### fact_claims
Grain: one row per claim (`claim_id`, `claim_type`). Claims split into two source segments are consolidated first.

| Column | Notes |
|---|---|
| `claim_sk` | xxhash64 of claim id and type |
| `patient_sk` | patient version valid on `from_date` (point-in-time lookup); -1 if unknown |
| `provider_sk`, `primary_diagnosis_sk`, `admitting_diagnosis_sk` | -1 if unknown |
| `from_date_key`, `thru_date_key`, `admission_date_key`, `discharge_date_key` | `yyyyMMdd` integers |
| `payment_amt`, `primary_payer_paid_amt`, `deductible_amt`, `coinsurance_amt`, `blood_deductible_amt`, `pass_thru_per_diem_amt` | summed over segments |
| `utilization_days`, `length_of_stay_days` | inpatient only |
| `diagnosis_count`, `procedure_count`, `segment_count` | |
| `is_adjustment` | negative payment (source adjustment) |
| `claim_year` | partition column |

### dim_patient (SCD Type 2)
One row per patient per version. A new version is added only when a tracked attribute changes: chronic condition flags, ESRD status, death date or coverage months. The first version starts on `1900-01-01`, later versions on 1 January of the coverage year, and the previous version ends the day before.

| Column | Notes |
|---|---|
| `patient_sk` | xxhash64 of patient id and `effective_from` |
| `patient_id` | DE-SynPUF `DESYNPUF_ID` |
| `birth_date`, `death_date`, `sex`, `race`, `state_code`, `county_code` | decoded from source codes |
| `has_esrd`, `has_diabetes`, `has_heart_failure`, … (11 conditions) | booleans |
| `chronic_condition_count` | |
| `effective_from`, `effective_to`, `is_current` | SCD Type 2 validity |
| `record_hash` | SHA-256 of the tracked attributes, used to detect changes |

### dim_diagnosis
ICD-9-CM codes found on claims, with a display form (`428.0`) and the ICD-9 chapter (e.g. *Circulatory system*).

### dim_provider
CMS provider numbers. The first two characters are the provider's state code.

### dim_date
Calendar from 2007-01-01 to 2011-12-31.

## Source data

[CMS 2008-2010 DE-SynPUF, Sample 1](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1): synthetic Medicare data, free to use.

| File | Rows |
|---|---|
| Beneficiary summary 2008 / 2009 / 2010 | 116,352 / 114,538 / 112,754 |
| Inpatient claims 2008-2010 | 66,773 |
| Outpatient claims 2008-2010 | 790,790 |

Data issues the pipeline handles:
- 68 inpatient and 11,253 outpatient rows are continuation segments of a claim, with amounts but no dates or diagnoses.
- 55 inpatient and 2,566 outpatient rows have negative payments (adjustments).
- 536 claims start in 2007.
- Chronic conditions, ESRD and death change from year to year, which is what SCD Type 2 captures.
