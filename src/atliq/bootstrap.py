"""Create the OLTP database, load seed data, and stand up the warehouse."""
from __future__ import annotations

from . import config as cfg
from .loggingutil import header, log
from .sqlclient import Sql, run_sqlcmd

SEED_ORDER = [
    "01_schema_ddl.sql",
    "02_insert_customers.sql",
    "03_insert_products.sql",
    "04_insert_orders.sql",
    "05_insert_order_items.sql",
    "06_insert_payments.sql",
    "07_etl_control_table.sql",
]


def setup(verbose: bool = True) -> dict:
    if verbose:
        header("Create databases")
    run_sqlcmd(cfg.SQL_DIR / "create_databases.sql", database="master")
    if verbose:
        log(f"OLTP      {cfg.OLTP_DB}")
        log(f"warehouse {cfg.DW_DB}")

    if verbose:
        header(f"Load OLTP seed into {cfg.OLTP_DB}")
    for name in SEED_ORDER:
        run_sqlcmd(cfg.SEED_SQL / name, database=cfg.OLTP_DB)
        if verbose:
            log(f"ran {name}")

    run_sqlcmd(cfg.SQL_DIR / "warehouse.sql", database=cfg.DW_DB)
    run_sqlcmd(cfg.SQL_DIR / "streaming.sql", database=cfg.DW_DB)
    if verbose:
        log("warehouse schemas bronze/silver/gold/ops/stream")

    counts = oltp_counts()
    if verbose:
        header("OLTP row counts")
        for table, n in counts.items():
            expect = cfg.EXPECTED_OLTP.get(table)
            mark = "ok" if expect is None or n == expect else f"expected {expect}"
            log(f"{table:<14} {n:>6,}   {mark}")
    return counts


def oltp_counts() -> dict:
    with Sql(cfg.OLTP_DB) as db:
        out = {}
        for table in ("customers", "products", "orders", "order_items", "payments"):
            out[table] = db.scalar(f"SELECT COUNT(*) FROM dbo.{table}")
        return out
