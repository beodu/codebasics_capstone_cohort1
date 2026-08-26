"""Bronze → Silver. Full snapshot tables are overwritten. Incremental tables MERGE."""
from __future__ import annotations

from datetime import date, datetime

from . import config as cfg
from .loggingutil import header, log
from .ops import log_run
from .sqlclient import Sql

FULL_OVERWRITE = {
    "customers": [
        "TRUNCATE TABLE silver.customers",
        """
        INSERT INTO silver.customers (customer_id, customer_name, email, city, signup_date, updated_at)
        SELECT DISTINCT
            customer_id,
            LTRIM(RTRIM(customer_name)),
            LOWER(LTRIM(RTRIM(email))),
            UPPER(LEFT(LTRIM(RTRIM(city)), 1)) + LOWER(SUBSTRING(LTRIM(RTRIM(city)), 2, 80)),
            signup_date,
            updated_at
        FROM bronze.customers
        WHERE customer_id IS NOT NULL
        """,
    ],
    "products": [
        "TRUNCATE TABLE silver.products",
        """
        INSERT INTO silver.products (product_id, product_name, category, unit_price, updated_at)
        SELECT DISTINCT
            product_id,
            LTRIM(RTRIM(product_name)),
            UPPER(LEFT(LTRIM(RTRIM(category)), 1)) + LOWER(SUBSTRING(LTRIM(RTRIM(category)), 2, 80)),
            unit_price,
            updated_at
        FROM bronze.products
        WHERE product_id IS NOT NULL
        """,
    ],
    "supplier_price_list": [
        "TRUNCATE TABLE silver.supplier_price_list",
        """
        ;WITH ranked AS (
            SELECT product_id, supplier_name, supplier_cost, effective_date,
                   ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC) AS rn
            FROM bronze.supplier_price_list
            WHERE product_id IS NOT NULL
        )
        INSERT INTO silver.supplier_price_list (product_id, supplier_name, supplier_cost, effective_date)
        SELECT product_id, supplier_name, CAST(supplier_cost AS DECIMAL(10,2)), CAST(effective_date AS DATE)
        FROM ranked WHERE rn = 1
        """,
    ],
    "marketing_spend": [
        "TRUNCATE TABLE silver.marketing_spend",
        """
        INSERT INTO silver.marketing_spend (spend_date, channel, campaign, spend_amount, clicks)
        SELECT DISTINCT
            CAST(spend_date AS DATE),
            LTRIM(RTRIM(channel)),
            campaign,
            CAST(spend_amount AS DECIMAL(12,2)),
            CAST(clicks AS INT)
        FROM bronze.marketing_spend
        WHERE spend_date IS NOT NULL
        """,
    ],
}

# update_when_newer: orders and payments yes; order_items insert-only.
MERGE_SQL = {
    "orders": (
        "order_id",
        "updated_at",
        True,
        """
        ;WITH src AS (
            SELECT order_id, customer_id, CAST(order_date AS date) AS order_date,
                   LTRIM(RTRIM(status)) AS status, order_amount, created_at, updated_at,
                   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
            FROM bronze.orders
            WHERE ingest_date = %s AND order_id IS NOT NULL
        )
        MERGE silver.orders AS t
        USING (SELECT * FROM src WHERE rn = 1) AS s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET
            customer_id = s.customer_id,
            order_date  = s.order_date,
            status      = s.status,
            order_amount = s.order_amount,
            created_at  = s.created_at,
            updated_at  = s.updated_at
        WHEN NOT MATCHED THEN INSERT
            (order_id, customer_id, order_date, status, order_amount, created_at, updated_at)
            VALUES (s.order_id, s.customer_id, s.order_date, s.status, s.order_amount,
                    s.created_at, s.updated_at);
        """,
    ),
    "payments": (
        "payment_id",
        "updated_at",
        True,
        """
        ;WITH src AS (
            SELECT payment_id, order_id, amount, LTRIM(RTRIM(method)) AS method,
                   paid_at, updated_at,
                   ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY updated_at DESC) AS rn
            FROM bronze.payments
            WHERE ingest_date = %s AND payment_id IS NOT NULL
        )
        MERGE silver.payments AS t
        USING (SELECT * FROM src WHERE rn = 1) AS s
        ON t.payment_id = s.payment_id
        WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET
            order_id = s.order_id, amount = s.amount, method = s.method,
            paid_at = s.paid_at, updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT
            (payment_id, order_id, amount, method, paid_at, updated_at)
            VALUES (s.payment_id, s.order_id, s.amount, s.method, s.paid_at, s.updated_at);
        """,
    ),
    "order_items": (
        "order_item_id",
        "created_at",
        False,
        """
        ;WITH src AS (
            SELECT order_item_id, order_id, product_id, quantity, item_price, created_at,
                   ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY created_at DESC) AS rn
            FROM bronze.order_items
            WHERE ingest_date = %s AND order_item_id IS NOT NULL AND quantity > 0
        )
        MERGE silver.order_items AS t
        USING (SELECT * FROM src WHERE rn = 1) AS s
        ON t.order_item_id = s.order_item_id
        WHEN NOT MATCHED THEN INSERT
            (order_item_id, order_id, product_id, quantity, item_price, created_at)
            VALUES (s.order_item_id, s.order_id, s.product_id, s.quantity, s.item_price, s.created_at);
        """,
    ),
}


def run(run_id: str, run_date: date, verbose: bool = True) -> None:
    dw = Sql(cfg.DW_DB)
    try:
        if verbose:
            header("Silver (clean + MERGE)")
        for table, statements in FULL_OVERWRITE.items():
            started = datetime.now()
            rows_in = dw.scalar(f"SELECT COUNT(*) FROM bronze.{table}") or 0
            for sql in statements:
                dw.execute(sql)
            rows_after = dw.scalar(f"SELECT COUNT(*) FROM silver.{table}") or 0
            log_run(dw, run_id, run_date, "bronze_to_silver", table, rows_in, rows_after, "SUCCESS", started)
            if verbose:
                log(f"overwrite    {table:<20} {rows_in:>6,} in -> {rows_after:>6,} rows")

        for table, (_key, _order, _upd, sql) in MERGE_SQL.items():
            started = datetime.now()
            before = dw.scalar(f"SELECT COUNT(*) FROM silver.{table}") or 0
            rows_in = dw.scalar(
                f"SELECT COUNT(*) FROM bronze.{table} WHERE ingest_date = %s",
                (run_date,),
            ) or 0
            if rows_in:
                dw.execute(sql, (run_date,))
            after = dw.scalar(f"SELECT COUNT(*) FROM silver.{table}") or 0
            log_run(dw, run_id, run_date, "bronze_to_silver", table, rows_in, after, "SUCCESS", started)
            if verbose:
                delta = after - before
                note = f"  (+{delta} new)" if delta else "  (no change)"
                log(f"merge        {table:<20} {rows_in:>6,} in -> {after:>6,} rows{note}")
    finally:
        dw.close()
