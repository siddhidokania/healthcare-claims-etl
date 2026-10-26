"""Incremental extract from the on-prem PostgreSQL source to the raw zone.

Local equivalent of the ADF pipeline `pl_ingest_claims_incremental`. For each
table it:
  1. reads the last watermark from etl_control.watermark
  2. reads the new watermark (max updated_at)
  3. copies rows in (old, new] to raw/<table>/batch_id=<batch>/ as Parquet
  4. writes a manifest with the source row count and amount total, used later
     for source-to-target reconciliation
  5. moves the watermark forward (only after the files are written)

Usage:
    python -m ingestion.extract_incremental --batch-id 2008 [--lake-root data/lake]
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.tables import SOURCE_TABLES
from onprem.db import connect

FETCH_ROWS = 50_000


def arrow_type(data_type: str, precision, scale) -> pa.DataType:
    if data_type in ("character varying", "character", "text"):
        return pa.string()
    if data_type == "smallint":
        return pa.int16()
    if data_type == "integer":
        return pa.int32()
    if data_type == "bigint":
        return pa.int64()
    if data_type == "numeric":
        return pa.decimal128(precision or 38, scale or 0)
    if data_type == "timestamp with time zone":
        return pa.timestamp("us", tz="UTC")
    raise ValueError(f"Unmapped PostgreSQL type: {data_type}")


def table_schema(cur, schema: str, table: str) -> pa.Schema:
    cur.execute("""
        SELECT column_name, data_type, numeric_precision, numeric_scale
          FROM information_schema.columns
         WHERE table_schema = %s AND table_name = %s
         ORDER BY ordinal_position
    """, (schema, table))
    return pa.schema([pa.field(name, arrow_type(t, p, s)) for name, t, p, s in cur.fetchall()])


def extract_table(conn, spec: dict, batch_id: str, lake_root: Path) -> dict:
    schema, table, amount_col = spec["schema"], spec["table_name"], spec["amount_column"]
    with conn.cursor() as cur:
        arrow_schema = table_schema(cur, schema, table)

        if spec["mode"] == "incremental":
            cur.execute("SELECT last_watermark FROM etl_control.watermark WHERE table_name = %s", (table,))
            old_wm = cur.fetchone()[0]
            cur.execute(f"SELECT COALESCE(MAX(updated_at), %s) FROM {schema}.{table}", (old_wm,))
            new_wm = cur.fetchone()[0]
            where, params = "WHERE updated_at > %s AND updated_at <= %s", (old_wm, new_wm)
        else:
            old_wm = new_wm = None
            where, params = "", ()

        amount_expr = f"COALESCE(SUM({amount_col}), 0)" if amount_col else "NULL::numeric"
        cur.execute(f"SELECT COUNT(*), {amount_expr} FROM {schema}.{table} {where}", params)
        source_rows, source_amount = cur.fetchone()

    out_dir = lake_root / "raw" / table / f"batch_id={batch_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    for old_file in out_dir.glob("*.parquet"):
        old_file.unlink()  # rerunning a batch replaces its files

    written = 0
    with conn.cursor(name=f"extract_{table}") as stream, \
            pq.ParquetWriter(out_dir / "part-00000.parquet", arrow_schema) as writer:
        stream.execute(f"SELECT {', '.join(arrow_schema.names)} FROM {schema}.{table} {where}", params)
        while rows := stream.fetchmany(FETCH_ROWS):
            columns = list(zip(*rows))
            writer.write_table(pa.Table.from_arrays(
                [pa.array(col, type=field.type) for col, field in zip(columns, arrow_schema)],
                schema=arrow_schema,
            ))
            written += len(rows)

    if written != source_rows:
        raise RuntimeError(f"{table}: wrote {written} rows but the source count was {source_rows}")

    manifest = {
        "table_name": table,
        "batch_id": batch_id,
        "row_count": source_rows,
        "amount_sum": None if source_amount is None else str(source_amount),
        "watermark_from": old_wm.isoformat() if old_wm else None,
        "watermark_to": new_wm.isoformat() if new_wm else None,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_dir = lake_root / "raw" / "_manifests" / f"batch_id={batch_id}"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / f"{table}.json").write_text(json.dumps(manifest) + "\n")

    if spec["mode"] == "incremental":
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM etl_control.update_watermark(%s, %s, %s)", (table, new_wm, batch_id))
        conn.commit()
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", "data/lake"))
    args = parser.parse_args()

    lake_root = Path(args.lake_root)
    with connect() as conn:
        for spec in SOURCE_TABLES:
            started = time.perf_counter()
            manifest = extract_table(conn, spec, args.batch_id, lake_root)
            print(f"  {spec['table_name']:<24} rows={manifest['row_count']:>9,}  "
                  f"amount={manifest['amount_sum'] or '-':>16}  {time.perf_counter() - started:.1f}s")


if __name__ == "__main__":
    main()
