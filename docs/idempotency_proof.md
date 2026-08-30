# Idempotency

I ran the pipeline twice on the same data. If Silver was appending, run 2
would roughly double the counts.

```bash
python3 run.py setup
python3 run.py run --twice
```

## After seed load (no simulator)

| Table | Rows |
|---|---|
| customers | 40 |
| products | 25 |
| orders | 300 |
| order_items | 783 |
| payments | 246 |

| Metric | Run 1 | Run 2 |
|---|---:|---:|
| silver.orders | 300 | 300 |
| silver.order_items | 783 | 783 |
| silver.payments | 246 | 246 |
| gold.fact_sales | 783 | 783 |
| gross revenue | 2,076,041.00 | 2,076,041.00 |
| net revenue | 1,745,772.00 | 1,745,772.00 |

Same numbers. 13/13 quality checks passed.

I also ran the simulator (`8` new orders + `5` status updates) and then the
nightly job:

```
merge  orders  13 in ->  308 rows  (+8 new)
```

13 rows came from OLTP, only 8 were inserts. The other 5 were status changes
merged in place.

## Why re-running is safe

- Bronze: same-day path is overwritten
- Silver: MERGE on order_id / payment_id / order_item_id
- Gold: rebuilt from Silver

## Azure

After deploying, run `phase1/databricks/02_idempotency_check.py` twice with no
new source data and keep both outputs. They should match the table above on a
fresh seed.
