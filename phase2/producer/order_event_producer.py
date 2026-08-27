"""
AtliQ Commerce — Phase 2
Live Order Event Producer — streams simulated order events to Kafka.

Prereqs:
    pip install -r ../requirements.txt
.env (next to this file — copy .env.example):
    KAFKA_BOOTSTRAP=pkc-xxxxx.region.provider.confluent.cloud:9092
    KAFKA_API_KEY=...
    KAFKA_API_SECRET=...
    KAFKA_TOPIC=atliq.orders.events

Run:  python order_event_producer.py --rate 2 --duration 300
"""
import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat"]
METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]
PRODUCT_PRICES = {1: 2499, 2: 3299, 3: 1799, 4: 1499, 5: 4999, 6: 899, 7: 1299,
                  8: 549, 9: 749, 10: 999, 11: 599, 12: 2199, 13: 1099, 14: 1599,
                  15: 899, 16: 299, 17: 449, 18: 549, 19: 799, 20: 1899,
                  21: 249, 22: 699, 23: 799, 24: 599, 25: 499}


def make_producer() -> Producer:
    """Producer configured for Confluent Cloud (SASL_SSL + PLAIN)."""
    missing = [v for v in ("KAFKA_BOOTSTRAP", "KAFKA_API_KEY", "KAFKA_API_SECRET")
               if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}. "
                         f"Copy .env.example to .env and fill it in.")

    return Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        # Durability: wait for all in-sync replicas, retry, and refuse to
        # silently reorder or duplicate on retry.
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "client.id": "atliq-order-event-producer",
    })


def delivery_report(err, msg):
    """Called once per message, on success or on permanent failure."""
    if err is not None:
        print(f"  DELIVERY FAILED  key={msg.key()} error={err}")
    else:
        print(f"  delivered key={msg.key().decode()} -> "
              f"{msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def base_event(event_type: str, order: dict) -> dict:
    """One event. event_id is the de-duplication key used in Silver."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,      # order_placed | payment_received | order_shipped | order_cancelled
        "event_ts": now_iso(),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "city": order["city"],
        "product_id": order["product_id"],
        "quantity": order["quantity"],
        "order_amount": order["order_amount"],
        "payment_method": order["payment_method"] if event_type == "payment_received" else None,
    }


def new_order(order_id: int) -> dict:
    product_id = random.choice(list(PRODUCT_PRICES))
    qty = random.randint(1, 3)
    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 40),
        "city": random.choice(CITIES),
        "product_id": product_id,
        "quantity": qty,
        "order_amount": PRODUCT_PRICES[product_id] * qty,
        "payment_method": random.choice(METHODS),
    }


def run(rate: float, duration: int):
    producer = make_producer()
    topic = os.environ.get("KAFKA_TOPIC", "atliq.orders.events")
    open_orders, next_order_id, sent = [], 100_000, 0
    deadline = time.time() + duration

    print(f"Producing to '{topic}' at ~{rate} events/sec for {duration}s ...")
    try:
        while time.time() < deadline:
            roll = random.random()
            if roll < 0.55 or not open_orders:
                order = new_order(next_order_id); next_order_id += 1
                events = [base_event("order_placed", order)]
                if order["payment_method"] != "COD":
                    events.append(base_event("payment_received", order))
                open_orders.append(order)
            elif roll < 0.90:
                events = [base_event("order_shipped", open_orders.pop(random.randrange(len(open_orders))))]
            else:
                events = [base_event("order_cancelled", open_orders.pop(random.randrange(len(open_orders))))]

            for ev in events:
                # Keyed by order_id: Kafka guarantees ordering within a partition
                # and equal keys hash to the same partition, so every event in one
                # order's lifecycle (placed -> paid -> shipped) stays in order.
                try:
                    producer.produce(
                        topic=topic,
                        key=str(ev["order_id"]),
                        value=json.dumps(ev),
                        on_delivery=delivery_report,
                    )
                except BufferError:
                    # Local queue full — drain it, then retry this one event.
                    producer.poll(1.0)
                    producer.produce(
                        topic=topic,
                        key=str(ev["order_id"]),
                        value=json.dumps(ev),
                        on_delivery=delivery_report,
                    )
                sent += 1

            producer.poll(0)
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print("\nStopping ...")
    finally:
        # Block until every buffered message has been delivered (or failed).
        # Without this, in-flight events are lost when the process exits.
        remaining = producer.flush(30)
        if remaining:
            print(f"WARNING: {remaining} message(s) still undelivered after flush timeout.")
        print(f"Done. {sent} events sent.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--duration", type=int, default=300)
    args = ap.parse_args()
    run(args.rate, args.duration)
