"""Pull from OLTP into bronze using etl.control_table (full vs incremental)."""
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from . import config as cfg
from .loggingutil import header, log
from .ops import log_run
from .sqlclient import Sql

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None


OLTP_COLUMNS = {
    "customers": ["customer_id", "customer_name", "email", "city", "signup_date", "updated_at"],
    "products": ["product_id", "product_name", "category", "unit_price", "updated_at"],
    "orders": ["order_id", "customer_id", "order_date", "status", "order_amount", "created_at", "updated_at"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "item_price", "created_at"],
    "payments": ["payment_id", "order_id", "amount", "method", "paid_at", "updated_at"],
    "supplier_price_list": ["product_id", "product_name", "supplier_name", "supplier_cost", "effective_date"],
    "marketing_spend": ["spend_date", "channel", "campaign", "spend_amount", "clicks"],
}


def _write_parquet(table: str, ingest_date: str, rows: list[dict], incremental: bool) -> Path | None:
    if pd is None:
        return None
    if incremental:
        target = cfg.BRONZE_DIR / table / f"ingest_date={ingest_date}"
    else:
        target = cfg.BRONZE_DIR / table
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=OLTP_COLUMNS[table])
    out = target / "part-00000.parquet"
    df.to_parquet(out, index=False)
    return out


def _rows_as_dicts(raw: list[dict], columns: list[str]) -> list[dict]:
    out = []
    for r in raw:
        lower = {str(k).lower(): v for k, v in r.items()}
        out.append({c: lower.get(c) for c in columns})
    return out


def run(run_id: str, run_start_at: datetime | None = None, verbose: bool = True) -> list[tuple]:
    run_start_at = run_start_at or datetime.now()
    run_date = run_start_at.date()
    ingest_date = run_date.isoformat()
    if verbose:
        header("Bronze ingest")
        log(f"run_start_at {run_start_at:%Y-%m-%d %H:%M:%S}  (watermark upper bound)")

    oltp = Sql(cfg.OLTP_DB)
    dw = Sql(cfg.DW_DB)
    summary = []
    try:
        sources = oltp.query(
            """
            SELECT table_name, load_type, watermark_column, last_loaded_at
            FROM etl.control_table
            ORDER BY table_name
            """
        )
        for src in sources:
            table = src["table_name"]
            load_type = src["load_type"]
            wm_col = src["watermark_column"]
            last_loaded = src["last_loaded_at"]
            started = datetime.now()
            cols = OLTP_COLUMNS[table]
            try:
                if load_type == "incremental":
                    raw = oltp.query(
                        f"""
                        SELECT * FROM dbo.{table}
                        WHERE {wm_col} > %s AND {wm_col} <= %s
                        """,
                        (last_loaded, run_start_at),
                    )
                    dw.execute(
                        f"DELETE FROM bronze.{table} WHERE ingest_date = %s",
                        (run_date,),
                    )
                else:
                    raw = oltp.query(f"SELECT * FROM dbo.{table}")
                    dw.execute(f"TRUNCATE TABLE bronze.{table}")

                rows = _rows_as_dicts(raw, cols)
                for r in rows:
                    r["ingest_date"] = run_date
                n = dw.insert_dicts(f"bronze.{table}", cols + ["ingest_date"], rows)
                _write_parquet(table, ingest_date, rows, incremental=(load_type == "incremental"))
                oltp.execute(
                    "UPDATE etl.control_table SET last_loaded_at = %s WHERE table_name = %s",
                    (run_start_at, table),
                )
                log_run(dw, run_id, run_date, "ingest_bronze", table, n, n, "SUCCESS", started)
                summary.append((table, load_type, n))
                if verbose:
                    where = f"{table}/ingest_date={ingest_date}" if load_type == "incremental" else f"{table}/"
                    log(f"{load_type:<12} {table:<16} {n:>6,} rows  bronze.{table}  {where}")
            except Exception as exc:
                log_run(dw, run_id, run_date, "ingest_bronze", table, 0, 0, "FAILED", started, str(exc))
                raise

        for name in cfg.CSV_SOURCES:
            started = datetime.now()
            path = cfg.CSV_DIR / f"{name}.csv"
            if pd is None:
                raise RuntimeError("pandas is required to load the external CSV sources")
            df = pd.read_csv(path)
            cols = OLTP_COLUMNS[name]
            rows = df[cols].to_dict(orient="records")
            for r in rows:
                r["ingest_date"] = run_date
            dw.execute(f"TRUNCATE TABLE bronze.{name}")
            n = dw.insert_dicts(f"bronze.{name}", cols + ["ingest_date"], rows)
            _write_parquet(name, ingest_date, rows, incremental=False)
            log_run(dw, run_id, run_date, "ingest_bronze", name, n, n, "SUCCESS", started)
            summary.append((name, "full", n))
            if verbose:
                log(f"{'full':<12} {name:<16} {n:>6,} rows  bronze.{name}  (csv)")
    finally:
        oltp.close()
        dw.close()
    return summary
