"""Gold star schema. Rebuilt from Silver each run — identical grain to the dbt models."""
from __future__ import annotations

from datetime import date, datetime

from . import config as cfg
from .loggingutil import header, log
from .ops import log_run
from .sqlclient import Sql

GOLD_BUILD = [
    ("dim_date", [
        "IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL DROP TABLE gold.dim_date",
        """
        WITH n AS (
            SELECT CAST('2024-01-01' AS date) AS d
            UNION ALL
            SELECT DATEADD(DAY, 1, d) FROM n WHERE d < '2026-12-31'
        )
        SELECT
            CONVERT(int, CONVERT(char(8), d, 112)) AS date_key,
            d AS full_date,
            YEAR(d) AS year,
            DATEPART(QUARTER, d) AS quarter,
            MONTH(d) AS month,
            DATENAME(MONTH, d) AS month_name,
            CONVERT(char(7), d, 126) AS year_month,
            DAY(d) AS day_of_month,
            DATEPART(ISO_WEEK, d) AS week_of_year,
            DATENAME(WEEKDAY, d) AS weekday_name,
            CAST(CASE WHEN DATENAME(WEEKDAY, d) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS bit) AS is_weekend
        INTO gold.dim_date
        FROM n
        OPTION (MAXRECURSION 1200)
        """,
        "ALTER TABLE gold.dim_date ALTER COLUMN date_key INT NOT NULL",
        "ALTER TABLE gold.dim_date ADD CONSTRAINT PK_gold_dim_date PRIMARY KEY (date_key)",
    ]),
    ("dim_customer", [
        "IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL DROP TABLE gold.dim_customer",
        """
        SELECT
            customer_id, customer_name, email, city, signup_date,
            CONVERT(char(7), signup_date, 126) AS signup_cohort,
            YEAR(signup_date) AS signup_year,
            updated_at
        INTO gold.dim_customer
        FROM silver.customers
        """,
        "ALTER TABLE gold.dim_customer ALTER COLUMN customer_id INT NOT NULL",
        "ALTER TABLE gold.dim_customer ADD CONSTRAINT PK_gold_dim_customer PRIMARY KEY (customer_id)",
    ]),
    ("dim_product", [
        "IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL DROP TABLE gold.dim_product",
        """
        SELECT
            p.product_id, p.product_name, p.category, p.unit_price,
            s.supplier_name, s.supplier_cost,
            p.unit_price - s.supplier_cost AS unit_margin,
            CASE WHEN s.supplier_cost IS NULL OR p.unit_price = 0 THEN NULL
                 ELSE ROUND((p.unit_price - s.supplier_cost) / p.unit_price * 100, 2)
            END AS margin_pct,
            p.updated_at
        INTO gold.dim_product
        FROM silver.products p
        LEFT JOIN silver.supplier_price_list s ON p.product_id = s.product_id
        """,
        "ALTER TABLE gold.dim_product ALTER COLUMN product_id INT NOT NULL",
        "ALTER TABLE gold.dim_product ADD CONSTRAINT PK_gold_dim_product PRIMARY KEY (product_id)",
    ]),
    ("fact_sales", [
        "IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL DROP TABLE gold.fact_sales",
        """
        SELECT
            oi.order_item_id,
            o.order_id,
            o.customer_id,
            oi.product_id,
            o.order_date,
            CONVERT(int, CONVERT(char(8), o.order_date, 112)) AS date_key,
            o.status,
            oi.quantity,
            oi.item_price,
            oi.quantity * oi.item_price AS gross_revenue,
            oi.quantity * COALESCE(dp.supplier_cost, 0) AS total_cost,
            CASE WHEN dp.supplier_cost IS NULL THEN NULL
                 ELSE oi.quantity * (oi.item_price - dp.supplier_cost)
            END AS gross_profit,
            CAST(CASE WHEN o.status <> 'Cancelled' THEN 1 ELSE 0 END AS bit) AS is_revenue,
            CAST(CASE WHEN o.status = 'Returned' THEN 1 ELSE 0 END AS bit) AS is_returned,
            oi.created_at
        INTO gold.fact_sales
        FROM silver.order_items oi
        INNER JOIN silver.orders o ON oi.order_id = o.order_id
        LEFT JOIN gold.dim_product dp ON oi.product_id = dp.product_id
        """,
        "ALTER TABLE gold.fact_sales ALTER COLUMN order_item_id INT NOT NULL",
        "ALTER TABLE gold.fact_sales ADD CONSTRAINT PK_gold_fact_sales PRIMARY KEY (order_item_id)",
    ]),
    ("fact_marketing_spend", [
        "IF OBJECT_ID('gold.fact_marketing_spend', 'U') IS NOT NULL DROP TABLE gold.fact_marketing_spend",
        """
        SELECT
            CONVERT(int, CONVERT(char(8), spend_date, 112)) AS date_key,
            spend_date, channel, campaign, spend_amount, clicks,
            CASE WHEN clicks > 0 THEN ROUND(spend_amount / clicks, 2) END AS cost_per_click
        INTO gold.fact_marketing_spend
        FROM silver.marketing_spend
        """,
    ]),
]


def run(run_id: str, run_date: date, verbose: bool = True) -> None:
    dw = Sql(cfg.DW_DB)
    try:
        if verbose:
            header("Gold star schema")
        for name, statements in GOLD_BUILD:
            started = datetime.now()
            for sql in statements:
                dw.execute(sql)
            rows = dw.scalar(f"SELECT COUNT(*) FROM gold.{name}") or 0
            log_run(dw, run_id, run_date, "gold_build", name, rows, rows, "SUCCESS", started)
            if verbose:
                log(f"built  gold.{name:<22} {rows:>6,} rows")
    finally:
        dw.close()


def snapshot(dw: Sql | None = None) -> dict:
    own = dw is None
    if own:
        dw = Sql(cfg.DW_DB)
    try:
        q = dw.scalar
        return {
            "silver_orders": q("SELECT COUNT(*) FROM silver.orders"),
            "silver_order_items": q("SELECT COUNT(*) FROM silver.order_items"),
            "silver_payments": q("SELECT COUNT(*) FROM silver.payments"),
            "fact_sales_rows": q("SELECT COUNT(*) FROM gold.fact_sales"),
            "gross_revenue": float(q("SELECT CAST(ROUND(SUM(gross_revenue), 2) AS DECIMAL(14,2)) FROM gold.fact_sales") or 0),
            "net_revenue": float(q("SELECT CAST(ROUND(SUM(CASE WHEN is_revenue = 1 THEN gross_revenue ELSE 0 END), 2) AS DECIMAL(14,2)) FROM gold.fact_sales") or 0),
            "dim_customer_rows": q("SELECT COUNT(*) FROM gold.dim_customer"),
            "dim_product_rows": q("SELECT COUNT(*) FROM gold.dim_product"),
        }
    finally:
        if own:
            dw.close()
