# Runbook

## Schedule

- 01:00 IST — `tr_nightly_0100` → `pl_nightly_master`
  1. ADF ingest to Bronze (watermarks per table)
  2. Databricks Bronze → Silver
  3. `dbt build`
- hourly — Airflow `atliq_streaming_ops` (freshness, OPTIMIZE, daily rollup)

## If something looks wrong

1. Did the nightly job fail? Check ADF / Databricks alerts.
2. Did it succeed but load 0 rows? Check `ops.pipeline_run_log`.
3. Did it succeed but numbers look stale/wrong? dbt tests and source freshness.

```sql
SELECT run_date, stage, table_name, rows_in, rows_after, status
FROM atliq.ops.pipeline_run_log
WHERE run_date >= current_date() - 3
ORDER BY started_at DESC;
```

## Common issues

**Watermark jumped and we skipped rows**  
Don’t set last_loaded_at to GETDATE() at the end. Use the run_start_at captured
at the beginning, and only after that table copied.

**Revenue doubled after a re-run**  
Silver is appending instead of MERGE. Check the notebook.

**Empty Gold windows in streaming**  
Windows close only after the 10 min watermark. Wait, or check the producer.

**Airflow DQ red**  
That’s the point if the producer is down. If the producer is running, check
the Kafka stream and Silver.

## Local

```bash
python3 run.py run
python3 run.py quality
```
