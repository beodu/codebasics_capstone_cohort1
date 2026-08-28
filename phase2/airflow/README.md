# Airflow — Streaming Ops

## Start

```bash
cd phase2/airflow
docker compose up -d          # UI at http://localhost:8080  (airflow / airflow)
```

## Connection

Airflow UI → **Admin → Connections → +**

| Field | Value |
|---|---|
| Conn Id | `databricks_default` |
| Conn Type | Databricks |
| Host | `https://<workspace>.cloud.databricks.com` |
| Password | your Databricks PAT |

Then paste your SQL warehouse HTTP path into `SQL_WAREHOUSE_HTTP_PATH` in
`dags/atliq_streaming_ops_dag.py`.

## Demonstrating the quality gate

A DQ check you cannot make fail is not a DQ check. To prove this one works:

1. With the producer running, trigger the DAG — all three tasks go green.
2. Stop the producer (`Ctrl-C`), wait past the 2-hour freshness window, trigger again.
3. `check_fresh_events` fails and the downstream tasks never run.

Screenshot both states — the brief requires them.

To demo the failure faster without waiting two hours, temporarily drop
`FRESHNESS_WINDOW_HOURS` to 1 and stop the producer for an hour, or point the check at
a filtered window. Restore it to 2 before submitting.
