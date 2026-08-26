"""Data quality gate. Every check must return zero failing rows."""
from __future__ import annotations

from . import config as cfg
from .loggingutil import header, log
from .sqlclient import Sql

CHECKS = [
    ("fact_sales.order_item_id is unique",
     "SELECT order_item_id FROM gold.fact_sales GROUP BY order_item_id HAVING COUNT(*) > 1"),
    ("fact_sales.order_item_id not null",
     "SELECT 1 FROM gold.fact_sales WHERE order_item_id IS NULL"),
    ("silver.orders.order_id is unique",
     "SELECT order_id FROM silver.orders GROUP BY order_id HAVING COUNT(*) > 1"),
    ("silver.payments.payment_id is unique",
     "SELECT payment_id FROM silver.payments GROUP BY payment_id HAVING COUNT(*) > 1"),
    ("fact_sales -> dim_customer relationship",
     """SELECT f.customer_id FROM gold.fact_sales f
        LEFT JOIN gold.dim_customer d ON f.customer_id = d.customer_id
        WHERE d.customer_id IS NULL"""),
    ("fact_sales -> dim_product relationship",
     """SELECT f.product_id FROM gold.fact_sales f
        LEFT JOIN gold.dim_product d ON f.product_id = d.product_id
        WHERE d.product_id IS NULL"""),
    ("fact_sales -> dim_date relationship",
     """SELECT f.date_key FROM gold.fact_sales f
        LEFT JOIN gold.dim_date d ON f.date_key = d.date_key WHERE d.date_key IS NULL"""),
    ("status in accepted values",
     """SELECT DISTINCT status FROM gold.fact_sales
        WHERE status NOT IN ('Placed','Shipped','Delivered','Cancelled','Returned')"""),
    ("quantity >= 1",
     "SELECT order_item_id FROM gold.fact_sales WHERE quantity < 1"),
    ("gross_revenue >= 0",
     "SELECT order_item_id FROM gold.fact_sales WHERE gross_revenue < 0"),
    ("order amount reconciles to fact rows",
     """SELECT o.order_id FROM silver.orders o
        JOIN gold.fact_sales f ON o.order_id = f.order_id
        GROUP BY o.order_id, o.order_amount
        HAVING ABS(o.order_amount - SUM(f.gross_revenue)) > 0.01"""),
    ("payment amount reconciles to order amount",
     """SELECT p.payment_id FROM silver.payments p
        JOIN silver.orders o ON p.order_id = o.order_id
        WHERE ABS(p.amount - o.order_amount) > 0.01"""),
    ("every order has at least one line item",
     """SELECT o.order_id FROM silver.orders o
        LEFT JOIN silver.order_items i ON o.order_id = i.order_id
        WHERE i.order_item_id IS NULL"""),
]


def run(verbose: bool = True) -> list[tuple]:
    dw = Sql(cfg.DW_DB)
    try:
        if verbose:
            header("Data quality")
        results = []
        for name, sql in CHECKS:
            failing = len(dw.fetchall(sql))
            results.append((name, failing, failing == 0))
            if verbose:
                flag = "PASS" if failing == 0 else "FAIL"
                log(f"[{flag}] {name:<44} {failing} failing")
        passed = sum(1 for _, _, ok in results if ok)
        if verbose:
            log(f"{passed}/{len(results)} checks passed")
        return results
    finally:
        dw.close()
