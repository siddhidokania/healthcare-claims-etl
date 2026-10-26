from datetime import date
from decimal import Decimal

from claims_etl.silver import (
    consolidate_segments, enrich_claims, latest_per_key, standardize_claims, transform_beneficiary, validate_claims,
)
from tests.conftest import ts


def silver_claims(df, claim_type):
    return enrich_claims(consolidate_segments(standardize_claims(df, claim_type)))


def test_segments_are_consolidated_into_one_claim(inpatient_df):
    df = inpatient_df([
        {"clm_id": "C1", "segment": 1, "desynpuf_id": "P1", "clm_from_dt": "20090105", "clm_thru_dt": "20090110",
         "clm_admsn_dt": "20090105", "nch_bene_dschrg_dt": "20090110", "prvdr_num": "2600GD",
         "clm_pmt_amt": "4000.00", "icd9_dgns_cd_1": "4280", "icd9_dgns_cd_2": "25000", "icd9_dgns_cd_3": "4280",
         "updated_at": ts(1), "_batch_id": "2009"},
        # continuation segment: amounts only, no dates or diagnoses
        {"clm_id": "C1", "segment": 2, "desynpuf_id": "P1", "clm_pmt_amt": "1000.00",
         "updated_at": ts(1), "_batch_id": "2009"},
    ])
    claim = silver_claims(df, "IP").first()

    assert claim["segment_count"] == 2
    assert claim["payment_amt"] == Decimal("5000.00")
    assert claim["from_date"] == date(2009, 1, 5)
    assert claim["length_of_stay_days"] == 5
    assert claim["primary_dx_code"] == "4280"
    assert claim["diagnosis_codes"] == ["4280", "25000"]  # duplicates removed, order kept
    assert claim["claim_year"] == 2009


def test_latest_version_wins(outpatient_df):
    df = standardize_claims(outpatient_df([
        {"clm_id": "C2", "segment": 1, "clm_pmt_amt": "50.00", "updated_at": ts(1), "_batch_id": "2008"},
        {"clm_id": "C2", "segment": 1, "clm_pmt_amt": "60.00", "updated_at": ts(5), "_batch_id": "2009"},
    ]), "OP")
    latest = latest_per_key(df, ["claim_type", "claim_id", "segment"]).collect()
    assert len(latest) == 1 and latest[0]["payment_amt"] == Decimal("60.00")


def test_outpatient_columns_are_mapped(outpatient_df):
    df = outpatient_df([{"clm_id": "C3", "segment": 1, "nch_bene_ptb_ddctbl_amt": "20.00",
                         "nch_bene_ptb_coinsrnc_amt": "5.00", "updated_at": ts(1), "_batch_id": "2008"}])
    row = standardize_claims(df, "OP").first()
    assert row["deductible_amt"] == Decimal("20.00")
    assert row["coinsurance_amt"] == Decimal("5.00")
    assert row["drg_code"] is None and row["utilization_days"] is None


def test_invalid_claims_are_quarantined(outpatient_df):
    df = outpatient_df([
        {"clm_id": "OK", "segment": 1, "desynpuf_id": "P1", "clm_from_dt": "20080101", "clm_thru_dt": "20080102",
         "clm_pmt_amt": "10.00", "updated_at": ts(1), "_batch_id": "2008"},
        {"clm_id": "BACKWARDS", "segment": 1, "desynpuf_id": "P1", "clm_from_dt": "20080105",
         "clm_thru_dt": "20080101", "clm_pmt_amt": "10.00", "updated_at": ts(1), "_batch_id": "2008"},
        {"clm_id": "BAD_DATE", "segment": 1, "desynpuf_id": "P1", "clm_from_dt": "2008XX01",
         "clm_thru_dt": "20080101", "clm_pmt_amt": "10.00", "updated_at": ts(1), "_batch_id": "2008"},
    ])
    valid, quarantined = validate_claims(silver_claims(df, "OP"))

    assert [r["claim_id"] for r in valid.collect()] == ["OK"]
    reasons = {r["claim_id"]: r["quarantine_reason"] for r in quarantined.collect()}
    assert reasons == {"BACKWARDS": "thru_before_from", "BAD_DATE": "invalid_from_date"}


def test_beneficiary_codes_are_decoded(beneficiary_df):
    df = beneficiary_df([{"desynpuf_id": "P1", "coverage_year": 2008, "bene_birth_dt": "19230501",
                          "bene_sex_ident_cd": "2", "bene_race_cd": "5", "bene_esrd_ind": "Y",
                          "sp_state_code": "5", "sp_chf": "1", "sp_diabetes": "2",
                          "updated_at": ts(1), "_batch_id": "2008"}])
    row = transform_beneficiary(df).first()
    assert (row["sex"], row["race"], row["has_esrd"], row["state_code"]) == ("Female", "Hispanic", True, "05")
    assert row["has_heart_failure"] is True and row["has_diabetes"] is False
    assert row["birth_date"] == date(1923, 5, 1)
