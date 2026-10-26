"""Clustering and warehouse-sizing benchmarks on the 17M-row scale-test fact table.

Experiment 1, clustering: the same queries on FACT_CLAIMS_X20_UNCLUSTERED and
FACT_CLAIMS_X20 (clustered on from_date_key), on the XSMALL warehouse.

Experiment 2, warehouse sizing: the same queries on the clustered table on the
XSMALL, SMALL and MEDIUM warehouses.

For every run the script reads the query profile with GET_QUERY_OPERATOR_STATS
(partitions scanned vs total, bytes scanned) and the server-side elapsed time,
and estimates credits as size rate x elapsed time. The result cache is switched
off so every run does real work. Results go to CLAIMS_DB.OPS.BENCHMARK_RESULTS
and docs/snowflake_results.md.

Usage:
    python -m warehouse_snowflake.benchmark [--runs 3]
"""

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

from warehouse_snowflake.connection import WAREHOUSES, connect

QUERIES = {
    # selective date range: the case clustering is for
    "one_month_by_type": """
        SELECT claim_type, COUNT(*) AS claims, SUM(payment_amt) AS payment
          FROM {fact}
         WHERE from_date_key BETWEEN 20090301 AND 20090331
         GROUP BY claim_type""",
    # one year, joined to a dimension
    "year_by_diagnosis_chapter": """
        SELECT d.icd9_chapter, COUNT(*) AS claims, SUM(f.payment_amt) AS payment
          FROM {fact} f JOIN DIM_DIAGNOSIS d ON f.primary_diagnosis_sk = d.diagnosis_sk
         WHERE f.from_date_key BETWEEN 20090101 AND 20091231
         GROUP BY d.icd9_chapter""",
    # full scan with a large dimension join
    "ckd_cost_by_year": """
        SELECT f.claim_year, COUNT(DISTINCT p.patient_id) AS patients, SUM(f.payment_amt) AS payment
          FROM {fact} f JOIN DIM_PATIENT p ON f.patient_sk = p.patient_sk
         WHERE p.has_chronic_kidney_disease
         GROUP BY f.claim_year""",
    # full scan, aggregate and sort
    "top_providers": """
        SELECT pr.provider_num, SUM(f.payment_amt) AS payment
          FROM {fact} f JOIN DIM_PROVIDER pr ON f.provider_sk = pr.provider_sk
         GROUP BY pr.provider_num
         ORDER BY payment DESC
         LIMIT 10""",
}
CLUSTERING_QUERIES = ["one_month_by_type", "year_by_diagnosis_chapter"]


def profile(cur, query_id: str, fact: str) -> dict:
    """Pruning and I/O for the fact-table scan, read from the query profile."""
    cur.execute("SELECT operator_type, operator_attributes, operator_statistics "
                "FROM TABLE(GET_QUERY_OPERATOR_STATS(%s))", (query_id,))
    stats = {"partitions_scanned": 0, "partitions_total": 0, "bytes_scanned": 0}
    for operator_type, attributes, statistics_json in cur.fetchall():
        if operator_type != "TableScan":
            continue
        if fact.upper() not in json.loads(attributes or "{}").get("table_name", "").upper():
            continue
        s = json.loads(statistics_json or "{}")
        stats["partitions_scanned"] += s.get("pruning", {}).get("partitions_scanned", 0)
        stats["partitions_total"] += s.get("pruning", {}).get("partitions_total", 0)
        stats["bytes_scanned"] += s.get("io", {}).get("bytes_scanned", 0)
    cur.execute("SELECT total_elapsed_time FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION()) "
                "WHERE query_id = %s", (query_id,))
    stats["elapsed_ms"] = cur.fetchone()[0]
    return stats


def run(cur, experiment: str, query_name: str, fact: str, warehouse: str, run_no: int) -> dict:
    cur.execute(QUERIES[query_name].format(fact=fact))
    cur.fetchall()
    stats = profile(cur, cur.sfqid, fact)
    stats.update(experiment=experiment, query_name=query_name, table_name=fact, warehouse=warehouse,
                 run_no=run_no, query_id=cur.sfqid,
                 est_credits=WAREHOUSES[warehouse] * stats["elapsed_ms"] / 3_600_000)
    cur.execute(
        "INSERT INTO CLAIMS_DB.OPS.BENCHMARK_RESULTS (experiment, query_name, table_name, warehouse, run_no, query_id, "
        "elapsed_ms, partitions_scanned, partitions_total, bytes_scanned, est_credits) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (experiment, query_name, fact, warehouse, run_no, stats["query_id"], stats["elapsed_ms"],
         stats["partitions_scanned"], stats["partitions_total"], stats["bytes_scanned"], stats["est_credits"]),
    )
    return stats


def median_by(results: list[dict], *keys: str) -> dict:
    groups = defaultdict(list)
    for r in results:
        groups[tuple(r[k] for k in keys)].append(r)
    return {
        key: {
            "elapsed_ms": statistics.median(r["elapsed_ms"] for r in rows),
            "est_credits": statistics.median(r["est_credits"] for r in rows),
            "partitions_scanned": rows[0]["partitions_scanned"],
            "partitions_total": rows[0]["partitions_total"],
            "bytes_scanned": rows[0]["bytes_scanned"],
        }
        for key, rows in groups.items()
    }


def clustering_info(cur, table: str) -> dict:
    cur.execute(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table}', '(from_date_key)')")
    info = json.loads(cur.fetchone()[0])
    return {"total_partitions": info["total_partition_count"], "average_depth": info["average_depth"]}


def write_report(path: Path, clustering: dict, sizing: dict, cluster_info: dict, runs: int) -> None:
    lines = [
        "# Snowflake benchmark results",
        "",
        f"Generated by `python -m warehouse_snowflake.benchmark`. Scale-test fact table: about 17M rows "
        f"(every claim repeated 20 times). Median of {runs} runs, result cache off. "
        "Estimated credits = warehouse credits per hour x elapsed time (Snowflake bills a 60-second minimum "
        "each time a warehouse resumes, so real cost for single short queries is higher).",
        "",
        "## Clustering (XSMALL warehouse)",
        "",
        f"Clustering depth on `from_date_key`: unclustered {cluster_info['unclustered']['average_depth']:.1f} vs "
        f"clustered {cluster_info['clustered']['average_depth']:.1f} "
        f"(lower is better; {cluster_info['clustered']['total_partitions']} micro-partitions).",
        "",
        "| Query | Table | Partitions scanned | Elapsed (ms) |",
        "|---|---|---|---|",
    ]
    for (query, table), r in sorted(clustering.items()):
        lines.append(f"| {query} | {table} | {r['partitions_scanned']} of {r['partitions_total']} | {r['elapsed_ms']:,.0f} |")
    lines += ["", "## Warehouse sizing (clustered table)", "",
              "| Query | Warehouse | Elapsed (ms) | Est. credits |", "|---|---|---|---|"]
    for (query, warehouse), r in sorted(sizing.items(), key=lambda kv: (kv[0][0], WAREHOUSES[kv[0][1]])):
        lines.append(f"| {query} | {warehouse} | {r['elapsed_ms']:,.0f} | {r['est_credits']:.5f} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--report", default="docs/snowflake_results.md")
    args = parser.parse_args()

    clustering_results, sizing_results = [], []
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("USE SCHEMA CLAIMS_DB.GOLD")
        cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        cluster_info = {
            "unclustered": clustering_info(cur, "FACT_CLAIMS_X20_UNCLUSTERED"),
            "clustered": clustering_info(cur, "FACT_CLAIMS_X20"),
        }

        cur.execute("USE WAREHOUSE CLAIMS_WH_XS")
        for table in ["FACT_CLAIMS_X20_UNCLUSTERED", "FACT_CLAIMS_X20"]:
            for query in CLUSTERING_QUERIES:
                for n in range(1, args.runs + 1):
                    clustering_results.append(run(cur, "clustering", query, table, "CLAIMS_WH_XS", n))

        for warehouse in WAREHOUSES:
            cur.execute(f"USE WAREHOUSE {warehouse}")
            cur.execute(QUERIES["top_providers"].format(fact="FACT_CLAIMS_X20"))   # warm the warehouse
            cur.fetchall()
            for query in QUERIES:
                for n in range(1, args.runs + 1):
                    sizing_results.append(run(cur, "sizing", query, "FACT_CLAIMS_X20", warehouse, n))
            cur.execute(f"ALTER WAREHOUSE {warehouse} SUSPEND")

    clustering = median_by(clustering_results, "query_name", "table_name")
    sizing = median_by(sizing_results, "query_name", "warehouse")
    write_report(Path(args.report), clustering, sizing, cluster_info, args.runs)
    for (query, table), r in sorted(clustering.items()):
        print(f"clustering  {query:<26} {table:<28} {r['partitions_scanned']:>5}/{r['partitions_total']:<5} "
              f"{r['elapsed_ms']:>7,.0f} ms")
    for (query, warehouse), r in sorted(sizing.items()):
        print(f"sizing      {query:<26} {warehouse:<13} {r['elapsed_ms']:>7,.0f} ms  {r['est_credits']:.5f} credits")
    print(f"Report written to {args.report}")


if __name__ == "__main__":
    main()
