"""Run the legacy on-prem ETL job (full rebuild of the monthly claims summary).

Usage:
    python onprem/legacy_etl.py
"""

import time

from onprem.db import connect


def main() -> None:
    started = time.perf_counter()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT legacy_rpt.run_monthly_summary()")
        rows = cur.fetchone()[0]
        cur.execute("""
            SELECT claim_type, SUM(claim_count), SUM(total_payment)
              FROM legacy_rpt.claims_monthly_summary
             GROUP BY claim_type ORDER BY claim_type
        """)
        totals = cur.fetchall()
    print(f"Legacy summary rebuilt: {rows:,} rows in {time.perf_counter() - started:.1f}s")
    for claim_type, claims, payment in totals:
        print(f"  {claim_type}: {claims:,} claims, payment {payment:,.2f}")


if __name__ == "__main__":
    main()
