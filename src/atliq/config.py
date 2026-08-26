"""Paths and connection settings. Override with environment variables."""
from __future__ import annotations

import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SQL_DIR = Path(__file__).resolve().parent / "sql"
SEED_SQL = REPO / "phase1" / "sql"
CSV_DIR = REPO / "phase1" / "data" / "csv"
LAKEHOUSE = Path(os.getenv("ATLIQ_LAKEHOUSE", REPO / "lakehouse"))
BRONZE_DIR = LAKEHOUSE / "bronze"

SQL_SERVER = os.getenv("ATLIQ_SQL_SERVER", "127.0.0.1")
SQL_PORT = int(os.getenv("ATLIQ_SQL_PORT", "1433"))
SQL_USER = os.getenv("ATLIQ_SQL_USER", "sa")
SQL_PASSWORD = os.getenv("ATLIQ_SQL_PASSWORD", "LocalMssql#2026")
OLTP_DB = os.getenv("ATLIQ_OLTP_DB", "atliq_commerce")
DW_DB = os.getenv("ATLIQ_DW_DB", "atliq_dw")

SQLCMD = os.getenv(
    "ATLIQ_SQLCMD",
    str(Path.home() / "mssql" / "bin" / "sqlcmd"),
)

OLTP_FULL = ("customers", "products")
OLTP_INCREMENTAL = {
    "orders": "updated_at",
    "order_items": "created_at",
    "payments": "updated_at",
}
CSV_SOURCES = ("supplier_price_list", "marketing_spend")

EXPECTED_OLTP = {
    "customers": 40,
    "products": 25,
    "orders": 300,
    "order_items": 783,
    "payments": 246,
}
