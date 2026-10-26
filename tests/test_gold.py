from datetime import date

from pyspark.sql import functions as F

from claims_etl import gold, quality
from claims_etl.config import LakeConfig
from claims_etl.delta_utils import read_delta
from claims_etl.silver import consolidate_segments, enrich_claims, standardize_claims, transform_beneficiary
from tests.conftest import ts


def write_beneficiaries(beneficiary_df, cfg, rows):
    transform_beneficiary(beneficiary_df(rows)).write.format("delta").mode("append").save(cfg.silver("beneficiary"))


def bene(year, chf, batch):
    return {"desynpuf_id": "P1", "coverage_year": year, "bene_birth_dt": "19300101", "bene_sex_ident_cd": "1",
            "sp_chf": chf, "updated_at": ts(year - 2000), "_batch_id": batch}


def test_scd2_keeps_history_and_is_idempotent(spark, tmp_path, beneficiary_df):
    cfg = LakeConfig(str(tmp_path))
    write_beneficiaries(beneficiary_df, cfg, [bene(2008, "2", "2008")])
    assert gold.build_dim_patient(spark, cfg, "2008") == {"new": 1, "changed": 0}

    write_beneficiaries(beneficiary_df, cfg, [bene(2009, "1", "2009")])     # heart failure diagnosed
    assert gold.build_dim_patient(spark, cfg, "2009") == {"new": 0, "changed": 1}
    assert gold.build_dim_patient(spark, cfg, "2009") == {"new": 0, "changed": 0}   # rerun
    assert gold.build_dim_patient(spark, cfg, "2008") == {"new": 0, "changed": 0}   # replay old batch

    versions = (read_delta(spark, cfg.gold("dim_patient")).where("patient_id = 'P1'")
                .orderBy("effective_from").collect())
    assert [(v["has_heart_failure"], v["effective_from"], v["effective_to"], v["is_current"]) for v in versions] == [
        (False, date(1900, 1, 1), date(2008, 12, 31), False),
        (True, date(2009, 1, 1), date(9999, 12, 31), True),
    ]


def test_fact_uses_patient_version_valid_on_claim_date(spark, tmp_path, beneficiary_df, outpatient_df):
    cfg = LakeConfig(str(tmp_path))
    write_beneficiaries(beneficiary_df, cfg, [bene(2008, "2", "2008")])
    gold.build_dim_patient(spark, cfg, "2008")
    write_beneficiaries(beneficiary_df, cfg, [bene(2009, "1", "2009")])
    gold.build_dim_patient(spark, cfg, "2009")

    claims = enrich_claims(consolidate_segments(standardize_claims(outpatient_df([
        {"clm_id": c, "segment": 1, "desynpuf_id": p, "clm_from_dt": d, "clm_thru_dt": d, "prvdr_num": "2600GD",
         "clm_pmt_amt": "10.00", "icd9_dgns_cd_1": "4019", "updated_at": ts(1), "_batch_id": "2009"}
        for c, p, d in [("C2008", "P1", "20080610"), ("C2009", "P1", "20090610"), ("CX", "NOBODY", "20090610")]
    ]), "OP"))).withColumn("_batch_id", F.lit("2009"))
    gold.build_dim_date(spark, cfg)
    gold.build_dim_provider(spark, cfg, claims, "2009")
    gold.build_dim_diagnosis(spark, cfg, claims, "2009")
    gold.build_fact_claims(spark, cfg, claims)

    patients = read_delta(spark, cfg.gold("dim_patient"))
    fact = read_delta(spark, cfg.gold("fact_claims")).join(patients, "patient_sk", "left")
    rows = {r["claim_id"]: r for r in fact.collect()}
    assert rows["C2008"]["has_heart_failure"] is False
    assert rows["C2009"]["has_heart_failure"] is True
    assert rows["CX"]["patient_sk"] == -1          # unknown member, row is kept
    assert rows["C2009"]["provider_sk"] != -1 and rows["C2009"]["primary_diagnosis_sk"] != -1


def test_quality_rules_detect_failures(spark):
    df = spark.createDataFrame([("A", 1), ("A", 2), ("B", None)], "claim_id string, amount int")
    assert quality.failed_rows(df, quality.Rule("t.x", "u", "unique", "error", columns=["claim_id"])) == 2
    assert quality.failed_rows(df, quality.Rule("t.x", "e", "expect", "error", expr="amount > 1")) == 2
