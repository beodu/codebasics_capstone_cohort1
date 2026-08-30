# AtliQ Commerce — Data Engineering Capstone

Codebasics DE bootcamp, Phase 1 + Phase 2.

AtliQ is a small e-commerce store. I built a nightly warehouse so we can report
on revenue without hitting the live orders database, and a Kafka stream for
near-real-time order events.

![Phase 1](docs/images/phase1_architecture.svg)

**Phase 1:** Azure SQL (OLTP) → ADF → Bronze (Parquet) → Silver (MERGE) → Gold (dbt star schema) → Fabric

![Phase 2](docs/images/phase2_architecture.png)

**Phase 2:** order events → Kafka → Databricks Structured Streaming → Airflow for the hourly jobs

## Repo

```
phase1/sql/            OLTP tables + seed + etl.control_table
phase1/simulator/      inserts a few new orders each day
phase1/adf/            one generic ingest pipeline (not one per table)
phase1/databricks/     Bronze → Silver MERGE
phase1/dbt/atliq_gold/ star schema + tests
phase2/producer/       Kafka producer (keyed by order_id)
phase2/databricks/     bronze / silver / gold streams
phase2/airflow/        freshness check → OPTIMIZE → daily summary
src/atliq/             local run on SQL Server (same logic, so I could test MERGE)
```

Seed data is the pack from the course: 40 customers, 25 products, 300 orders,
783 line items, 246 payments.

I treated **net revenue as excluding Cancelled**. Returned is shown on the
dashboard but not subtracted. After a clean load that comes to:

- fact_sales: 783 rows
- gross: 2,076,041
- net: 1,745,772

## Run locally

I used the SQL Server instance on my laptop (`localhost,1433`) to test before
wiring Azure.

```bash
python3 -m pip install -r requirements.txt
python3 run.py setup
python3 run.py run
python3 run.py run --twice          # should not change the numbers
python3 run.py simulate --orders 8
python3 run.py run                  # MERGE: 13 rows in, only 8 new orders
python3 run.py serve                # http://127.0.0.1:8080
python3 run.py stream --reset --events 400 --replay 25
```

VS Code: open `atliq.code-workspace` and connect to `atliq_commerce` / `atliq_dw`.

When I simulated 8 new orders and 5 status changes, Silver did:

```
merge  orders  13 in ->  308 rows  (+8 new)
```

The 5 status changes updated in place. If this was append-only those 5 would
have been extra rows and revenue would be wrong.

Streaming: 500 raw events, 400 unique (`event_id`), 100 duplicates dropped.

## Azure / Confluent

1. Create `rg-atliq-capstone`, load `phase1/sql/01`–`07` into Azure SQL
2. Import ADF JSON, start `tr_nightly_0100`
3. Run the Databricks notebook + `dbt build`
4. OneLake shortcut to Gold, four visuals (trend, products, cities, new vs returning)
5. Confluent topic `atliq.orders.events`, start the three streams
6. Airflow DAG — also trigger it with the producer stopped so the DQ task fails

Notes: [architecture](docs/architecture.md) · [phase 2](docs/phase2_writeup.md) · [runbook](docs/runbook.md)
