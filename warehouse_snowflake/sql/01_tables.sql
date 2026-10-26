-- Gold star schema in Snowflake. Run by warehouse_snowflake/load.py as CLAIMS_ENGINEER.

CREATE SCHEMA IF NOT EXISTS CLAIMS_DB.GOLD;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DB.OPS;
USE SCHEMA CLAIMS_DB.GOLD;

CREATE FILE FORMAT IF NOT EXISTS CSV_WITH_HEADER
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE STAGE IF NOT EXISTS LOAD_STAGE FILE_FORMAT = CSV_WITH_HEADER
    COMMENT = 'Internal stage for gold CSV exports';

CREATE OR REPLACE TABLE DIM_DATE (
    date_key      NUMBER(8,0) NOT NULL,
    full_date     DATE        NOT NULL,
    year          NUMBER(4,0),
    quarter       NUMBER(1,0),
    month         NUMBER(2,0),
    month_name    VARCHAR(10),
    day_of_month  NUMBER(2,0),
    day_name      VARCHAR(10),
    is_weekend    BOOLEAN,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

CREATE OR REPLACE TABLE DIM_PROVIDER (
    provider_sk          NUMBER(19,0) NOT NULL,
    provider_num         VARCHAR(10),
    provider_state_code  VARCHAR(2),
    CONSTRAINT pk_dim_provider PRIMARY KEY (provider_sk)
);

CREATE OR REPLACE TABLE DIM_DIAGNOSIS (
    diagnosis_sk       NUMBER(19,0) NOT NULL,
    icd9_code          VARCHAR(10),
    icd9_code_display  VARCHAR(12),
    icd9_chapter       VARCHAR(80),
    CONSTRAINT pk_dim_diagnosis PRIMARY KEY (diagnosis_sk)
);

CREATE OR REPLACE TABLE DIM_PATIENT (
    patient_sk                      NUMBER(19,0) NOT NULL,
    patient_id                      VARCHAR(16),
    sex                             VARCHAR(10),
    race                            VARCHAR(10),
    state_code                      VARCHAR(2),
    birth_date                      DATE,
    death_date                      DATE,
    has_esrd                        BOOLEAN,
    has_alzheimers                  BOOLEAN,
    has_heart_failure               BOOLEAN,
    has_chronic_kidney_disease      BOOLEAN,
    has_cancer                      BOOLEAN,
    has_copd                        BOOLEAN,
    has_depression                  BOOLEAN,
    has_diabetes                    BOOLEAN,
    has_ischemic_heart_disease      BOOLEAN,
    has_osteoporosis                BOOLEAN,
    has_rheumatoid_osteo_arthritis  BOOLEAN,
    has_stroke_tia                  BOOLEAN,
    chronic_condition_count         NUMBER(2,0),
    effective_from                  DATE,
    effective_to                    DATE,
    is_current                      BOOLEAN,
    CONSTRAINT pk_dim_patient PRIMARY KEY (patient_sk)
);

-- Fact table clustered on the claim date, the most common report filter.
CREATE OR REPLACE TABLE FACT_CLAIMS (
    claim_sk              NUMBER(19,0) NOT NULL,
    claim_id              VARCHAR(20),
    claim_type            VARCHAR(2),
    patient_sk            NUMBER(19,0),
    provider_sk           NUMBER(19,0),
    primary_diagnosis_sk  NUMBER(19,0),
    from_date_key         NUMBER(8,0),
    thru_date_key         NUMBER(8,0),
    payment_amt           NUMBER(12,2),
    deductible_amt        NUMBER(12,2),
    coinsurance_amt       NUMBER(12,2),
    length_of_stay_days   NUMBER(5,0),
    diagnosis_count       NUMBER(3,0),
    procedure_count       NUMBER(3,0),
    segment_count         NUMBER(2,0),
    is_adjustment         BOOLEAN,
    claim_year            NUMBER(4,0),
    CONSTRAINT pk_fact_claims PRIMARY KEY (claim_sk),
    CONSTRAINT fk_fact_patient FOREIGN KEY (patient_sk) REFERENCES DIM_PATIENT (patient_sk),
    CONSTRAINT fk_fact_provider FOREIGN KEY (provider_sk) REFERENCES DIM_PROVIDER (provider_sk),
    CONSTRAINT fk_fact_diagnosis FOREIGN KEY (primary_diagnosis_sk) REFERENCES DIM_DIAGNOSIS (diagnosis_sk),
    CONSTRAINT fk_fact_date FOREIGN KEY (from_date_key) REFERENCES DIM_DATE (date_key)
)
CLUSTER BY (from_date_key);

CREATE TABLE IF NOT EXISTS CLAIMS_DB.OPS.LOAD_AUDIT (
    loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    table_name      VARCHAR,
    source_rows     NUMBER,
    loaded_rows     NUMBER,
    source_payment  NUMBER(18,2),
    loaded_payment  NUMBER(18,2),
    status          VARCHAR
);

CREATE TABLE IF NOT EXISTS CLAIMS_DB.OPS.BENCHMARK_RESULTS (
    run_at              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    experiment          VARCHAR,
    query_name          VARCHAR,
    table_name          VARCHAR,
    warehouse           VARCHAR,
    run_no              NUMBER,
    query_id            VARCHAR,
    elapsed_ms          NUMBER,
    partitions_scanned  NUMBER,
    partitions_total    NUMBER,
    bytes_scanned       NUMBER,
    est_credits         FLOAT
);
