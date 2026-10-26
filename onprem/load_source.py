"""Load one year of DE-SynPUF data into the on-prem PostgreSQL source.

Each run simulates a year of activity in the source system:
  * beneficiaries are upserted (the source keeps only the latest record per
    patient, so history is lost there; the warehouse keeps it with SCD Type 2)
  * claims that start in that year are inserted (2007 claims go with 2008,
    continuation segments go with their first segment)
  * optionally, a few earlier claims are adjusted, so the next incremental
    load has updates as well as inserts

Usage:
    python onprem/load_source.py --year 2008
    python onprem/load_source.py --year 2009 --simulate-adjustments 500
"""

import argparse
import io
import time
from pathlib import Path

import pandas as pd

from onprem.db import connect

FIRST_YEAR = 2008
CHUNK_ROWS = 100_000

BENEFICIARY_COLUMNS = [
    "desynpuf_id", "coverage_year", "bene_birth_dt", "bene_death_dt", "bene_sex_ident_cd",
    "bene_race_cd", "bene_esrd_ind", "sp_state_code", "bene_county_cd",
    "bene_hi_cvrage_tot_mons", "bene_smi_cvrage_tot_mons", "bene_hmo_cvrage_tot_mons",
    "plan_cvrg_mos_num", "sp_alzhdmta", "sp_chf", "sp_chrnkidn", "sp_cncr", "sp_copd",
    "sp_depressn", "sp_diabetes", "sp_ischmcht", "sp_osteoprs", "sp_ra_oa", "sp_strketia",
    "medreimb_ip", "benres_ip", "pppymt_ip", "medreimb_op", "benres_op", "pppymt_op",
    "medreimb_car", "benres_car", "pppymt_car",
]

DIAGNOSIS_COLUMNS = [f"icd9_dgns_cd_{i}" for i in range(1, 11)]
PROCEDURE_COLUMNS = [f"icd9_prcdr_cd_{i}" for i in range(1, 7)]

INPATIENT_COLUMNS = [
    "desynpuf_id", "clm_id", "segment", "clm_from_dt", "clm_thru_dt", "prvdr_num",
    "clm_pmt_amt", "nch_prmry_pyr_clm_pd_amt", "at_physn_npi", "op_physn_npi", "ot_physn_npi",
    "clm_admsn_dt", "admtng_icd9_dgns_cd", "clm_pass_thru_per_diem_amt",
    "nch_bene_ip_ddctbl_amt", "nch_bene_pta_coinsrnc_lblty_am", "nch_bene_blood_ddctbl_lblty_am",
    "clm_utlztn_day_cnt", "nch_bene_dschrg_dt", "clm_drg_cd",
    *DIAGNOSIS_COLUMNS, *PROCEDURE_COLUMNS,
]

OUTPATIENT_COLUMNS = [
    "desynpuf_id", "clm_id", "segment", "clm_from_dt", "clm_thru_dt", "prvdr_num",
    "clm_pmt_amt", "nch_prmry_pyr_clm_pd_amt", "at_physn_npi", "op_physn_npi", "ot_physn_npi",
    "nch_bene_blood_ddctbl_lblty_am", *DIAGNOSIS_COLUMNS, *PROCEDURE_COLUMNS,
    "nch_bene_ptb_ddctbl_amt", "nch_bene_ptb_coinsrnc_amt", "admtng_icd9_dgns_cd",
]

# Integer columns: the CSVs sometimes hold them as "3.0", which PostgreSQL rejects.
INTEGER_COLUMNS = {
    "coverage_year", "segment", "clm_utlztn_day_cnt", "bene_hi_cvrage_tot_mons",
    "bene_smi_cvrage_tot_mons", "bene_hmo_cvrage_tot_mons", "plan_cvrg_mos_num",
}

CLAIM_FILES = {
    "inpatient_claims": ("DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv", INPATIENT_COLUMNS),
    "outpatient_claims": ("DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv", OUTPATIENT_COLUMNS),
}


def read_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    df.columns = [c.lower() for c in df.columns]
    df = df.reindex(columns=columns)
    for col in INTEGER_COLUMNS.intersection(columns):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def claims_for_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Claims whose first segment starts in `year` (all earlier years go to the first load).

    Rows of a claim with no date at all (e.g. a continuation segment whose first
    segment is missing from the file) also go to the first load. They are kept on
    purpose: the pipeline must quarantine them, not lose them.
    """
    first_segment_year = (
        df.loc[df["clm_from_dt"].notna(), ["clm_id", "clm_from_dt"]]
        .assign(load_year=lambda d: d["clm_from_dt"].str[:4].astype(int).clip(lower=FIRST_YEAR))
        .drop_duplicates("clm_id")
        .set_index("clm_id")["load_year"]
    )
    load_year = df["clm_id"].map(first_segment_year).fillna(FIRST_YEAR)
    return df[load_year == year]


def copy_into_temp(cur, temp_table: str, like_table: str, columns: list[str], df: pd.DataFrame) -> None:
    cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {like_table} INCLUDING DEFAULTS) ON COMMIT DROP")
    column_list = ", ".join(columns)
    with cur.copy(f"COPY {temp_table} ({column_list}) FROM STDIN WITH (FORMAT csv, NULL '')") as copy:
        for start in range(0, len(df), CHUNK_ROWS):
            buffer = io.StringIO()
            df.iloc[start:start + CHUNK_ROWS].to_csv(buffer, index=False, header=False)
            copy.write(buffer.getvalue())


def load_beneficiaries(cur, data_dir: Path, year: int) -> tuple[int, int]:
    df = read_csv(data_dir / f"DE1_0_{year}_Beneficiary_Summary_File_Sample_1.csv", BENEFICIARY_COLUMNS)
    df["coverage_year"] = year
    copy_into_temp(cur, "tmp_beneficiary", "claims_src.beneficiary", BENEFICIARY_COLUMNS, df)

    attributes = [c for c in BENEFICIARY_COLUMNS if c != "desynpuf_id"]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in attributes)
    target_cols = ", ".join(f"t.{c}" for c in attributes)
    excluded_cols = ", ".join(f"EXCLUDED.{c}" for c in attributes)
    cur.execute(f"""
        WITH upserted AS (
            INSERT INTO claims_src.beneficiary AS t ({", ".join(BENEFICIARY_COLUMNS)})
            SELECT {", ".join(BENEFICIARY_COLUMNS)} FROM tmp_beneficiary
            ON CONFLICT (desynpuf_id) DO UPDATE
               SET {set_clause}, updated_at = now()
             WHERE ({target_cols}) IS DISTINCT FROM ({excluded_cols})
            RETURNING (xmax = 0) AS inserted
        )
        SELECT COUNT(*) FILTER (WHERE inserted), COUNT(*) FILTER (WHERE NOT inserted) FROM upserted
    """)
    return cur.fetchone()


def load_claims(cur, data_dir: Path, year: int, table: str) -> int:
    file_name, columns = CLAIM_FILES[table]
    df = claims_for_year(read_csv(data_dir / file_name, columns), year)
    copy_into_temp(cur, f"tmp_{table}", f"claims_src.{table}", columns, df)
    cur.execute(f"""
        INSERT INTO claims_src.{table} ({", ".join(columns)})
        SELECT {", ".join(columns)} FROM tmp_{table}
        ON CONFLICT (clm_id, segment) DO NOTHING
    """)
    return cur.rowcount


def simulate_adjustments(cur, year: int, count: int) -> int:
    """Re-price a few earlier outpatient claims, like a late claim adjustment."""
    cur.execute("SELECT setseed(0.42)")
    cur.execute("""
        UPDATE claims_src.outpatient_claims c
           SET clm_pmt_amt = c.clm_pmt_amt + 10, updated_at = now()
          FROM (SELECT clm_id, segment
                  FROM claims_src.outpatient_claims
                 WHERE segment = 1 AND clm_from_dt < %s
                 ORDER BY clm_id
                 LIMIT 100000) candidates
         WHERE c.clm_id = candidates.clm_id AND c.segment = candidates.segment
           AND random() < %s
    """, (f"{year}0101", count / 100_000))
    return cur.rowcount


def log_load(cur, year: int, table: str, inserted: int, updated: int) -> None:
    cur.execute(
        "INSERT INTO etl_control.source_load_log (coverage_year, table_name, rows_inserted, rows_updated) "
        "VALUES (%s, %s, %s, %s)",
        (year, table, inserted, updated),
    )
    print(f"  {table:<18} inserted={inserted:>8,}  updated={updated:>8,}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True, choices=[2008, 2009, 2010])
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--simulate-adjustments", type=int, default=0,
                        help="number of earlier outpatient claims to adjust (about)")
    args = parser.parse_args()
    data_dir = Path(args.data_dir)

    started = time.perf_counter()
    print(f"Loading {args.year} into the on-prem source")
    with connect() as conn, conn.cursor() as cur:
        inserted, updated = load_beneficiaries(cur, data_dir, args.year)
        log_load(cur, args.year, "beneficiary", inserted, updated)

        for table in CLAIM_FILES:
            log_load(cur, args.year, table, load_claims(cur, data_dir, args.year, table), 0)

        if args.simulate_adjustments:
            adjusted = simulate_adjustments(cur, args.year, args.simulate_adjustments)
            log_load(cur, args.year, "outpatient_claims (adjustments)", 0, adjusted)
    print(f"Done in {time.perf_counter() - started:.1f}s")


if __name__ == "__main__":
    main()
