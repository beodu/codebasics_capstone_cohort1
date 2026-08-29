# Phase 1 — Architecture

![Phase 1 architecture](images/phase1_architecture.svg)

## Why two databases

The storefront writes to Azure SQL (`atliq_commerce`). That database is 3NF —
good for inserts and updates, bad for “revenue by city by month”, which needs
a bunch of joins and a full scan. If I run that on OLTP, checkout traffic
slows down.

So I made a second copy, shaped for reading, refreshed every night:

| | OLTP | OLAP (Gold) |
|---|---|---|
| Purpose | take orders | reports / dashboard |
| Shape | 5 normalised tables | fact_sales + 3 dimensions |
| Example query | one order by id | 12 months of revenue by category |

`fact_sales` is **one row per order item**. I put tests on that because if the
grain is wrong, every chart is wrong.

## Nightly job

**1. ADF → Bronze (Parquet)**

One pipeline. It reads `etl.control_table` (full vs incremental, watermark
column, last loaded time). ForEach + If on `load_type`. Adding a table is an
INSERT into the control table, not a new pipeline.

- incremental (`orders`, `order_items`, `payments`): only rows after the
  watermark, land in `bronze/<table>/ingest_date=YYYY-MM-DD/`
- full (`customers`, `products`, CSVs): overwrite the folder

I capture `run_start_at` once at the start and only bump the watermark after
that table copies. If I used “now” at the end, rows that arrived during the
run would be skipped. If the copy fails, the watermark stays put and the next
run retries the same range.

Bronze stays raw Parquet. ADF Copy doesn’t write Delta anyway.

**2. Databricks → Silver (Delta, Unity Catalog)**

Small tables: clean and overwrite.

`orders` / `payments`: **MERGE on the business key**. Status can change later
(Placed → Delivered), so the same `order_id` shows up in more than one night.
Append would double-count. I also keep only the latest row *inside* the batch
(window + row_number) in case an order changed twice between runs.

`order_items` never updates, so the merge only inserts.

Access is Unity Catalog + access connector. No `/mnt/` mounts, no storage keys
in the notebook.

**3. dbt → Gold**

Staging models on Silver sources, then `ref()` into the marts. Full table
rebuild each night — Silver already has the merged history, and the data is
small. Tests run with `dbt build` (unique/not null, relationships, accepted
status values, order amount = sum of fact rows).

**4. Fabric**

Gold is external Delta under `.../gold/`. Fabric uses a OneLake shortcut
(pointer, not a copy).

## Idempotency

Run the job twice with no new source data:

- Bronze same-day folder is overwritten
- Silver MERGE matches existing keys
- Gold rebuilds from Silver

Numbers I got locally (same seed): 783 fact rows, gross 2,076,041, both runs.
Details in `idempotency_proof.md`.

## Revenue

- Gross = `qty * item_price` on all fact rows
- Net = exclude Cancelled (`is_revenue = 0`), keep the rows so I can still
  report cancellation rate
- Returned is shown separately, not deducted

On the seed that’s 2,076,041 gross vs 1,745,772 net. Difference is cancelled
orders (~18%). I put this on the dashboard so the next report doesn’t pick a
different definition.
