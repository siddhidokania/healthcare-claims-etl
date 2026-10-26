"""FinOps report for the project's Snowflake usage.

  * warehouse settings audit: size, auto-suspend, resource monitor attached
  * resource monitor: quota, credits used, credits remaining
  * credits used per warehouse (last 7 days, from WAREHOUSE_METERING_HISTORY)
  * storage per table (active, Time Travel and Fail-safe bytes)

Usage:
    python -m warehouse_snowflake.finops
"""

from warehouse_snowflake.connection import connect

MAX_AUTO_SUSPEND_SECONDS = 300


def rows_as_dicts(cur) -> list[dict]:
    names = [c[0].lower() for c in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def main() -> None:
    with connect() as conn:
        cur = conn.cursor()

        print("Warehouse settings")
        cur.execute("SHOW WAREHOUSES LIKE 'CLAIMS%'")
        for w in rows_as_dicts(cur):
            issues = []
            if not w.get("auto_suspend") or int(w["auto_suspend"]) > MAX_AUTO_SUSPEND_SECONDS:
                issues.append(f"auto_suspend {w.get('auto_suspend')}s is above {MAX_AUTO_SUSPEND_SECONDS}s")
            if not w.get("resource_monitor") or w["resource_monitor"] == "null":
                issues.append("no resource monitor")
            status = "OK  " if not issues else "WARN"
            print(f"  [{status}] {w['name']:<13} size={w['size']:<8} auto_suspend={w['auto_suspend']}s "
                  f"monitor={w['resource_monitor']} {'; '.join(issues)}")

        print("\nResource monitor")
        cur.execute("SHOW RESOURCE MONITORS LIKE 'CLAIMS%'")
        for m in rows_as_dicts(cur):
            print(f"  {m['name']}: quota={m['credit_quota']} used={m['used_credits']} "
                  f"remaining={m['remaining_credits']} frequency={m['frequency']}")

        print("\nCredits used, last 7 days")
        cur.execute("""
            SELECT warehouse_name, ROUND(SUM(credits_used), 4) AS credits,
                   ROUND(SUM(credits_used_compute), 4) AS compute, ROUND(SUM(credits_used_cloud_services), 4) AS cloud
              FROM TABLE(CLAIMS_DB.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(DATEADD('day', -7, CURRENT_DATE())))
             WHERE warehouse_name LIKE 'CLAIMS%'
             GROUP BY warehouse_name ORDER BY credits DESC
        """)
        for name, credits, compute, cloud in cur.fetchall():
            print(f"  {name:<13} {credits} credits (compute {compute}, cloud services {cloud})")

        # INFORMATION_SCHEMA.TABLES is current; TABLE_STORAGE_METRICS (which adds Time
        # Travel and Fail-safe bytes) can lag by a few hours.
        print("\nStorage by table (compressed)")
        cur.execute("""
            SELECT table_schema, table_name, row_count, bytes
              FROM CLAIMS_DB.INFORMATION_SCHEMA.TABLES
             WHERE table_type = 'BASE TABLE'
             ORDER BY bytes DESC
        """)
        for schema, table, rows, size in cur.fetchall():
            print(f"  {schema}.{table:<30} rows={rows:>12,}  {size / 1e6:8.1f} MB")


if __name__ == "__main__":
    main()
