-- Simulated on-prem claims system (PostgreSQL).
-- Runs automatically when the onprem-db container is first created.

CREATE SCHEMA IF NOT EXISTS claims_src;
CREATE SCHEMA IF NOT EXISTS etl_control;
CREATE SCHEMA IF NOT EXISTS legacy_rpt;

-- ---------------------------------------------------------------------------
-- Source tables. Column names follow the CMS DE-SynPUF files. Dates are kept
-- as YYYYMMDD text, the way the legacy system stores them.
-- updated_at is maintained by the source system and drives incremental loads.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS claims_src.beneficiary (
    desynpuf_id               VARCHAR(16) PRIMARY KEY,
    coverage_year             SMALLINT    NOT NULL,
    bene_birth_dt             CHAR(8),
    bene_death_dt             CHAR(8),
    bene_sex_ident_cd         VARCHAR(1),
    bene_race_cd              VARCHAR(1),
    bene_esrd_ind             VARCHAR(1),
    sp_state_code             VARCHAR(2),
    bene_county_cd            VARCHAR(3),
    bene_hi_cvrage_tot_mons   SMALLINT,
    bene_smi_cvrage_tot_mons  SMALLINT,
    bene_hmo_cvrage_tot_mons  SMALLINT,
    plan_cvrg_mos_num         SMALLINT,
    sp_alzhdmta               VARCHAR(1),
    sp_chf                    VARCHAR(1),
    sp_chrnkidn               VARCHAR(1),
    sp_cncr                   VARCHAR(1),
    sp_copd                   VARCHAR(1),
    sp_depressn               VARCHAR(1),
    sp_diabetes               VARCHAR(1),
    sp_ischmcht               VARCHAR(1),
    sp_osteoprs               VARCHAR(1),
    sp_ra_oa                  VARCHAR(1),
    sp_strketia               VARCHAR(1),
    medreimb_ip               NUMERIC(12,2),
    benres_ip                 NUMERIC(12,2),
    pppymt_ip                 NUMERIC(12,2),
    medreimb_op               NUMERIC(12,2),
    benres_op                 NUMERIC(12,2),
    pppymt_op                 NUMERIC(12,2),
    medreimb_car              NUMERIC(12,2),
    benres_car                NUMERIC(12,2),
    pppymt_car                NUMERIC(12,2),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_beneficiary_updated_at ON claims_src.beneficiary (updated_at);

CREATE TABLE IF NOT EXISTS claims_src.inpatient_claims (
    desynpuf_id                     VARCHAR(16) NOT NULL,
    clm_id                          VARCHAR(20) NOT NULL,
    segment                         SMALLINT    NOT NULL,
    clm_from_dt                     CHAR(8),
    clm_thru_dt                     CHAR(8),
    prvdr_num                       VARCHAR(10),
    clm_pmt_amt                     NUMERIC(12,2),
    nch_prmry_pyr_clm_pd_amt        NUMERIC(12,2),
    at_physn_npi                    VARCHAR(12),
    op_physn_npi                    VARCHAR(12),
    ot_physn_npi                    VARCHAR(12),
    clm_admsn_dt                    CHAR(8),
    admtng_icd9_dgns_cd             VARCHAR(10),
    clm_pass_thru_per_diem_amt      NUMERIC(12,2),
    nch_bene_ip_ddctbl_amt          NUMERIC(12,2),
    nch_bene_pta_coinsrnc_lblty_am  NUMERIC(12,2),
    nch_bene_blood_ddctbl_lblty_am  NUMERIC(12,2),
    clm_utlztn_day_cnt              SMALLINT,
    nch_bene_dschrg_dt              CHAR(8),
    clm_drg_cd                      VARCHAR(4),
    icd9_dgns_cd_1  VARCHAR(10), icd9_dgns_cd_2  VARCHAR(10), icd9_dgns_cd_3  VARCHAR(10),
    icd9_dgns_cd_4  VARCHAR(10), icd9_dgns_cd_5  VARCHAR(10), icd9_dgns_cd_6  VARCHAR(10),
    icd9_dgns_cd_7  VARCHAR(10), icd9_dgns_cd_8  VARCHAR(10), icd9_dgns_cd_9  VARCHAR(10),
    icd9_dgns_cd_10 VARCHAR(10),
    icd9_prcdr_cd_1 VARCHAR(10), icd9_prcdr_cd_2 VARCHAR(10), icd9_prcdr_cd_3 VARCHAR(10),
    icd9_prcdr_cd_4 VARCHAR(10), icd9_prcdr_cd_5 VARCHAR(10), icd9_prcdr_cd_6 VARCHAR(10),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (clm_id, segment)
);
CREATE INDEX IF NOT EXISTS ix_inpatient_updated_at ON claims_src.inpatient_claims (updated_at);

CREATE TABLE IF NOT EXISTS claims_src.outpatient_claims (
    desynpuf_id                     VARCHAR(16) NOT NULL,
    clm_id                          VARCHAR(20) NOT NULL,
    segment                         SMALLINT    NOT NULL,
    clm_from_dt                     CHAR(8),
    clm_thru_dt                     CHAR(8),
    prvdr_num                       VARCHAR(10),
    clm_pmt_amt                     NUMERIC(12,2),
    nch_prmry_pyr_clm_pd_amt        NUMERIC(12,2),
    at_physn_npi                    VARCHAR(12),
    op_physn_npi                    VARCHAR(12),
    ot_physn_npi                    VARCHAR(12),
    nch_bene_blood_ddctbl_lblty_am  NUMERIC(12,2),
    icd9_dgns_cd_1  VARCHAR(10), icd9_dgns_cd_2  VARCHAR(10), icd9_dgns_cd_3  VARCHAR(10),
    icd9_dgns_cd_4  VARCHAR(10), icd9_dgns_cd_5  VARCHAR(10), icd9_dgns_cd_6  VARCHAR(10),
    icd9_dgns_cd_7  VARCHAR(10), icd9_dgns_cd_8  VARCHAR(10), icd9_dgns_cd_9  VARCHAR(10),
    icd9_dgns_cd_10 VARCHAR(10),
    icd9_prcdr_cd_1 VARCHAR(10), icd9_prcdr_cd_2 VARCHAR(10), icd9_prcdr_cd_3 VARCHAR(10),
    icd9_prcdr_cd_4 VARCHAR(10), icd9_prcdr_cd_5 VARCHAR(10), icd9_prcdr_cd_6 VARCHAR(10),
    nch_bene_ptb_ddctbl_amt         NUMERIC(12,2),
    nch_bene_ptb_coinsrnc_amt       NUMERIC(12,2),
    admtng_icd9_dgns_cd             VARCHAR(10),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (clm_id, segment)
);
CREATE INDEX IF NOT EXISTS ix_outpatient_updated_at ON claims_src.outpatient_claims (updated_at);

-- ---------------------------------------------------------------------------
-- Watermark control table for incremental extraction (used by ADF and by the
-- local extract script).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_control.watermark (
    table_name      TEXT PRIMARY KEY,
    last_watermark  TIMESTAMPTZ NOT NULL DEFAULT '1900-01-01',
    last_batch_id   TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO etl_control.watermark (table_name) VALUES
    ('beneficiary'), ('inpatient_claims'), ('outpatient_claims')
ON CONFLICT DO NOTHING;

-- Called by ADF through a Lookup activity (ADF has no script activity for
-- PostgreSQL), so it returns a row.
CREATE OR REPLACE FUNCTION etl_control.update_watermark(
    p_table_name TEXT, p_watermark TIMESTAMPTZ, p_batch_id TEXT)
RETURNS TABLE (table_name TEXT, last_watermark TIMESTAMPTZ, last_batch_id TEXT)
LANGUAGE sql AS $$
    UPDATE etl_control.watermark w
       SET last_watermark = p_watermark,
           last_batch_id  = p_batch_id,
           updated_at     = now()
     WHERE w.table_name = p_table_name
    RETURNING w.table_name, w.last_watermark, w.last_batch_id;
$$;

-- Log of yearly loads into the source system (written by load_source.py)
CREATE TABLE IF NOT EXISTS etl_control.source_load_log (
    load_id        SERIAL PRIMARY KEY,
    coverage_year  SMALLINT NOT NULL,
    table_name     TEXT     NOT NULL,
    rows_inserted  INTEGER  NOT NULL,
    rows_updated   INTEGER  NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
