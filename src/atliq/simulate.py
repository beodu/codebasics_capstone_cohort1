"""Insert new orders and bump status on a few existing ones."""
from __future__ import annotations

import random
from datetime import datetime, timedelta

from . import config as cfg
from .loggingutil import header, log
from .sqlclient import Sql

STATUSES = ["Placed", "Placed", "Shipped", "Delivered"]
METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]
PROGRESSION = {"Placed": "Shipped", "Shipped": "Delivered"}


def run(n_orders: int = 8, n_updates: int = 5, seed=None, verbose: bool = True) -> dict:
    if seed is not None:
        random.seed(seed)
    db = Sql(cfg.OLTP_DB)
    now = datetime.now()
    try:
        if verbose:
            header("OLTP activity")
        customers = [r["customer_id"] for r in db.query("SELECT customer_id FROM dbo.customers")]
        products = db.query("SELECT product_id, unit_price FROM dbo.products")

        new_revenue = 0.0
        for _ in range(n_orders):
            customer_id = random.choice(customers)
            status = random.choice(STATUSES)
            order_date = (now - timedelta(days=random.randint(0, 3))).date()
            inserted = db.query(
                """
                INSERT INTO dbo.orders (customer_id, order_date, status, order_amount, created_at, updated_at)
                OUTPUT INSERTED.order_id
                VALUES (%s, %s, %s, 0, %s, %s)
                """,
                (customer_id, order_date, status, now, now),
            )
            order_id = list(inserted[0].values())[0]
            total = 0.0
            sample = random.sample(products, k=random.randint(1, min(3, len(products))))
            for p in sample:
                qty = random.randint(1, 3)
                price = float(p["unit_price"])
                db.execute(
                    """
                    INSERT INTO dbo.order_items (order_id, product_id, quantity, item_price, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (order_id, p["product_id"], qty, price, now),
                )
                total += qty * price
            db.execute(
                "UPDATE dbo.orders SET order_amount = %s, updated_at = %s WHERE order_id = %s",
                (total, now, order_id),
            )
            if status != "Placed" or random.random() < 0.7:
                db.execute(
                    """
                    INSERT INTO dbo.payments (order_id, amount, method, paid_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (order_id, total, random.choice(METHODS), now, now),
                )
            new_revenue += total

        n = int(n_updates)
        movable = db.query(
            f"""
            SELECT TOP ({n}) order_id, status
            FROM dbo.orders
            WHERE status IN ('Placed', 'Shipped')
            ORDER BY NEWID()
            """
        )
        updated = []
        for row in movable:
            new_status = PROGRESSION[row["status"]]
            db.execute(
                "UPDATE dbo.orders SET status = %s, updated_at = %s WHERE order_id = %s",
                (new_status, now, row["order_id"]),
            )
            updated.append((row["order_id"], row["status"], new_status))

        if verbose:
            log(f"inserted {n_orders} orders  (+{new_revenue:,.2f} gross)")
            log(f"updated  {len(updated)} existing orders")
            for oid, old, new in updated:
                log(f"  order {oid}: {old} -> {new}")
        return {
            "new_orders": n_orders,
            "updated_orders": len(updated),
            "new_revenue": round(new_revenue, 2),
        }
    finally:
        db.close()
