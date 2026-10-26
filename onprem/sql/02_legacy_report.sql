-- Legacy on-prem ETL job (the workload being migrated).
--
-- Rebuilds a monthly claims summary every night with a full TRUNCATE + INSERT.
-- It has the usual problems of the old job:
--   * full reload every run, no incremental processing
--   * no data quality checks
--   * every source row is counted as a claim, so claims split into two
--     segments are counted twice (continuation segments have no dates and
--     land in an 'UNKNOWN' month)
--
-- The new Databricks pipeline replaces this, and its reconciliation step
-- compares its totals with this table.

CREATE TABLE IF NOT EXISTS legacy_rpt.claims_monthly_summary (
    claim_type     VARCHAR(2)  NOT NULL,
    claim_month    VARCHAR(7)  NOT NULL,   -- YYYY-MM or 'UNKNOWN'
    state_code     VARCHAR(2),
    claim_count    INTEGER     NOT NULL,
    patient_count  INTEGER     NOT NULL,
    total_payment  NUMERIC(16,2) NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION legacy_rpt.run_monthly_summary()
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    TRUNCATE legacy_rpt.claims_monthly_summary;

    INSERT INTO legacy_rpt.claims_monthly_summary
        (claim_type, claim_month, state_code, claim_count, patient_count, total_payment)
    SELECT c.claim_type,
           COALESCE(SUBSTRING(c.clm_from_dt, 1, 4) || '-' || SUBSTRING(c.clm_from_dt, 5, 2), 'UNKNOWN'),
           b.sp_state_code,
           COUNT(*),
           COUNT(DISTINCT c.desynpuf_id),
           COALESCE(SUM(c.clm_pmt_amt), 0)
      FROM (
            SELECT 'IP' AS claim_type, desynpuf_id, clm_from_dt, clm_pmt_amt FROM claims_src.inpatient_claims
            UNION ALL
            SELECT 'OP', desynpuf_id, clm_from_dt, clm_pmt_amt FROM claims_src.outpatient_claims
           ) c
      LEFT JOIN claims_src.beneficiary b ON b.desynpuf_id = c.desynpuf_id
     GROUP BY 1, 2, 3;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;
