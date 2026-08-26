"""Speed lane: order events into stream.* with de-duplication and 5-minute windows."""
from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta

from . import config as cfg
from .loggingutil import header, log
from .sqlclient import Sql

WINDOW_MINUTES = 5
WATERMARK_MINUTES = 10
PRICES = {
    1: 2499, 2: 3299, 3: 1799, 4: 1499, 5: 4999, 6: 899, 7: 1299, 8: 549,
    9: 749, 10: 999, 11: 599, 12: 2199, 13: 1099, 14: 1599, 15: 899,
    16: 299, 17: 449, 18: 549, 19: 799, 20: 1899, 21: 249, 22: 699,
    23: 799, 24: 599, 25: 499,
}
CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat"]
METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]


def _partition(order_id: int, n: int = 3) -> int:
    return order_id % n


def produce(dw: Sql, n_events: int, replay_pct: int = 0, span_minutes: int = 45) -> tuple[int, int]:
    now = datetime.now()
    start = now - timedelta(minutes=span_minutes)
    sent, produced, open_orders, next_id = 0, [], [], 500_000
    while sent < n_events:
        offset = span_minutes * (sent / max(1, n_events))
        base_ts = start + timedelta(minutes=offset)
        pid = random.choice(list(PRICES))
        qty = random.randint(1, 3)
        order = {
            "order_id": next_id,
            "customer_id": random.randint(1, 40),
            "city": random.choice(CITIES),
            "product_id": pid,
            "quantity": qty,
            "order_amount": PRICES[pid] * qty,
            "payment_method": random.choice(METHODS),
        }
        next_id += 1
        if random.random() < 0.55 or not open_orders:
            types = ["order_placed"]
            if order["payment_method"] != "COD":
                types.append("payment_received")
            open_orders.append(order)
        else:
            prev = random.choice(open_orders)
            order = dict(prev)
            types = ["order_shipped"] if random.random() < 0.8 else ["order_cancelled"]
            if types[0] == "order_cancelled":
                open_orders.remove(prev)
        for event_type in types:
            if sent >= n_events:
                break
            event_ts = base_ts
            if random.random() < 0.15:
                event_ts = base_ts - timedelta(minutes=random.randint(1, 8))
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
                **order,
            }
            produced.append(event)
            sent += 1

    replayed = produced[: max(0, int(len(produced) * replay_pct / 100))]
    all_events = produced + replayed
    rows = []
    for ev in all_events:
        payload = json.dumps(ev, default=str)
        rows.append((
            ev["event_id"], ev["event_type"], ev["order_id"], ev["event_ts"],
            payload, _partition(ev["order_id"]),
        ))
    dw.executemany(
        """
        INSERT INTO stream.events_raw
            (event_id, event_type, order_id, event_ts, payload_json, partition_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    return len(produced), len(replayed)


def process(dw: Sql) -> dict:
    dw.execute(
        """
        INSERT INTO stream.events
            (event_id, event_type, order_id, customer_id, city, product_id,
             quantity, order_amount, payment_method, event_ts)
        SELECT r.event_id, r.event_type, r.order_id,
               TRY_CAST(JSON_VALUE(r.payload_json, '$.customer_id') AS INT),
               JSON_VALUE(r.payload_json, '$.city'),
               TRY_CAST(JSON_VALUE(r.payload_json, '$.product_id') AS INT),
               TRY_CAST(JSON_VALUE(r.payload_json, '$.quantity') AS INT),
               TRY_CAST(JSON_VALUE(r.payload_json, '$.order_amount') AS DECIMAL(12,2)),
               JSON_VALUE(r.payload_json, '$.payment_method'),
               r.event_ts
        FROM (
            SELECT r.*,
                   ROW_NUMBER() OVER (PARTITION BY r.event_id ORDER BY r.raw_id) AS rn
            FROM stream.events_raw r
        ) r
        WHERE r.rn = 1
          AND NOT EXISTS (
              SELECT 1 FROM stream.events s WHERE s.event_id = r.event_id
          )
        """
    )
    latest = dw.scalar("SELECT MAX(event_ts) FROM stream.events")
    if latest is None:
        return {"bronze": 0, "silver": 0, "windows": 0, "latest": None, "watermark": None}
    watermark = latest - timedelta(minutes=WATERMARK_MINUTES)
    dw.execute("TRUNCATE TABLE stream.revenue_windows")
    dw.execute(
        """
        INSERT INTO stream.revenue_windows (window_start, window_end, paid_events, revenue, closed_at)
        SELECT
            DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, event_ts) / %s) * %s, 0) AS window_start,
            DATEADD(MINUTE, %s, DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, event_ts) / %s) * %s, 0)) AS window_end,
            COUNT(*),
            SUM(order_amount),
            SYSUTCDATETIME()
        FROM stream.events
        WHERE event_type = 'payment_received'
        GROUP BY DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, event_ts) / %s) * %s, 0)
        HAVING DATEADD(MINUTE, %s, DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, event_ts) / %s) * %s, 0)) <= %s
        """,
        (WINDOW_MINUTES, WINDOW_MINUTES, WINDOW_MINUTES, WINDOW_MINUTES, WINDOW_MINUTES,
         WINDOW_MINUTES, WINDOW_MINUTES, WINDOW_MINUTES, WINDOW_MINUTES, WINDOW_MINUTES,
         watermark),
    )
    return {
        "bronze": dw.scalar("SELECT COUNT(*) FROM stream.events_raw"),
        "silver": dw.scalar("SELECT COUNT(*) FROM stream.events"),
        "windows": dw.scalar("SELECT COUNT(*) FROM stream.revenue_windows"),
        "latest": latest,
        "watermark": watermark,
    }


def dq_fresh(dw: Sql, max_age_minutes: int = 120) -> tuple[bool, str]:
    latest = dw.scalar("SELECT MAX(event_ts) FROM stream.events")
    if latest is None:
        return False, "no events in stream.events"
    age = (datetime.now() - latest).total_seconds() / 60
    if age > max_age_minutes:
        return False, f"latest event {latest} is {age:.1f} min old (threshold {max_age_minutes})"
    return True, f"latest event {latest} ({age:.1f} min ago)"


def run(events: int = 400, replay: int = 25, reset: bool = False, verbose: bool = True) -> dict:
    dw = Sql(cfg.DW_DB)
    try:
        if reset:
            dw.execute("TRUNCATE TABLE stream.revenue_windows")
            dw.execute("DELETE FROM stream.events")
            dw.execute("DELETE FROM stream.events_raw")
        if verbose:
            header("Streaming producer")
        unique, replayed = produce(dw, events, replay_pct=replay)
        if verbose:
            log(f"published {unique} events, replayed {replayed} duplicates")
            header("Streaming processor")
        stats = process(dw)
        if verbose:
            log(f"bronze {stats['bronze']} raw  silver {stats['silver']} unique  "
                f"windows {stats['windows']} closed")
            log(f"watermark {stats['watermark']}  (latest minus {WATERMARK_MINUTES} min)")
            types = dw.query(
                """
                SELECT event_type, COUNT(*) AS n
                FROM stream.events
                GROUP BY event_type
                ORDER BY COUNT(*) DESC
                """
            )
            for t in types:
                log(f"  {t['event_type']:<22} {t['n']}")
            windows = dw.query(
                """
                SELECT TOP 8 window_start, window_end, paid_events, revenue
                FROM stream.revenue_windows
                ORDER BY window_start DESC
                """
            )
            for w in windows:
                log(f"  {w['window_start']:%H:%M}–{w['window_end']:%H:%M}  "
                    f"{int(w['paid_events']):>4} paid  {float(w['revenue']):>12,.2f}")
        ok, msg = dq_fresh(dw, 120)
        if verbose:
            header("Freshness gate")
            log(("PASS" if ok else "FAIL") + " — " + msg)
        stats.update({"unique": unique, "replayed": replayed, "fresh": ok, "fresh_msg": msg})
        return stats
    finally:
        dw.close()
