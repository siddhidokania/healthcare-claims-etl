"""Load the gold star schema into Snowflake and reconcile it with the lake export.

Steps:
  1. create schemas, file format, stage and tables (sql/01_tables.sql)
  2. PUT the CSV exports into the internal stage (compressed on upload)
  3. COPY INTO each table, aborting on any bad row
  4. reconcile row counts (and payment totals for the fact) against the CSVs,
     logging the result to OPS.LOAD_AUDIT
  5. build the scale-test fact tables (sql/02_scale_test.sql)

Usage (after `python -m pipeline.export_bi`):
    python -m warehouse_snowflake.load [--exports data/exports/star] [--skip-scale-test]
"""

import argparse
import time
from decimal import Decimal
from pathlib import Path

import pandas as pd

from warehouse_snowflake.connection import connect, run_script

# Load dimensions before the fact table.
TABLES = ["dim_date", "dim_provider", "dim_diagnosis", "dim_patient", "fact_claims"]


def source_totals(csv_path: Path, table: str) -> tuple[int, Decimal | None]:
    if table != "fact_claims":
        return sum(1 for _ in open(csv_path, encoding="utf-8")) - 1, None
    payments = pd.read_csv(csv_path, usecols=["payment_amt"], dtype={"payment_amt": str})["payment_amt"]
    return len(payments), sum((Decimal(p) for p in payments.dropna()), Decimal("0")).quantize(Decimal("0.01"))


def load_table(cur, exports: Path, table: str) -> dict:
    csv_path = exports / f"{table}.csv"
    started = time.perf_counter()
    cur.execute(f"REMOVE @LOAD_STAGE/{table}/")
    cur.execute(f"PUT 'file://{csv_path.resolve().as_posix()}' @LOAD_STAGE/{table}/ AUTO_COMPRESS = TRUE OVERWRITE = TRUE")
    cur.execute(f"TRUNCATE TABLE {table}")
    cur.execute(f"""
        COPY INTO {table}
        FROM @LOAD_STAGE/{table}/
        FILE_FORMAT = (FORMAT_NAME = CSV_WITH_HEADER)
        ON_ERROR = ABORT_STATEMENT
    """)

    source_rows, source_payment = source_totals(csv_path, table)
    if table == "fact_claims":
        cur.execute(f"SELECT COUNT(*), SUM(payment_amt) FROM {table}")
        loaded_rows, loaded_payment = cur.fetchone()
    else:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        loaded_rows, loaded_payment = cur.fetchone()[0], None

    status = "PASS" if loaded_rows == source_rows and loaded_payment == source_payment else "FAIL"
    cur.execute(
        "INSERT INTO CLAIMS_DB.OPS.LOAD_AUDIT (table_name, source_rows, loaded_rows, source_payment, loaded_payment, status) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (table, source_rows, loaded_rows, source_payment, loaded_payment, status),
    )
    return {"table": table, "rows": loaded_rows, "payment": loaded_payment, "status": status,
            "seconds": round(time.perf_counter() - started, 1)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exports", default="data/exports/star")
    parser.add_argument("--skip-scale-test", action="store_true")
    args = parser.parse_args()
    exports = Path(args.exports)

    with connect() as conn:
        run_script(conn, "01_tables.sql")
        cur = conn.cursor()
        cur.execute("USE SCHEMA CLAIMS_DB.GOLD")
        results = [load_table(cur, exports, table) for table in TABLES]
        for r in results:
            payment = f"  payment={r['payment']:,.2f}" if r["payment"] is not None else ""
            print(f"[{r['status']}] {r['table']:<14} rows={r['rows']:>9,}{payment}  {r['seconds']}s")
        if any(r["status"] != "PASS" for r in results):
            raise SystemExit("Load reconciliation failed; see CLAIMS_DB.OPS.LOAD_AUDIT")

        if not args.skip_scale_test:
            started = time.perf_counter()
            run_script(conn, "02_scale_test.sql")
            cur.execute("SELECT COUNT(*) FROM FACT_CLAIMS_X20")
            print(f"Scale-test tables built: {cur.fetchone()[0]:,} rows in {time.perf_counter() - started:.0f}s")


if __name__ == "__main__":
    main()
