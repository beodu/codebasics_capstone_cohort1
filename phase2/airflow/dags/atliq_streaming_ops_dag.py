"""
AtliQ Phase 2 — Streaming Ops DAG
=================================
The stream never stops, but the scheduled work around it still has to run:
a data-quality gate, table maintenance, and the daily rollup.

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Setup:
1. In docker-compose.yaml:  _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
   then: docker compose down && docker compose up -d
2. Airflow UI -> Admin -> Connections -> +
   Conn Id: databricks_default | Type: Databricks
   Host: https://<workspace>.cloud.databricks.com | Password: <PAT token>
3. Paste your SQL warehouse HTTP path below.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx"   # <-- yours

CATALOG = "atliq"
SCHEMA = "streaming"
FRESHNESS_WINDOW_HOURS = 2

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # 1. Data-quality gate.
    #    assert_true() raises inside Databricks SQL, so the statement itself
    #    errors and the task goes red. A silent stream is the failure mode that
    #    matters here: the tables still answer queries, they are just frozen in
    #    time, and nobody notices until someone questions a number.
    # ------------------------------------------------------------------
    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=f"""
            SELECT assert_true(
                COUNT(*) > 0,
                'DQ FAIL: no events in {CATALOG}.{SCHEMA}.silver_order_events in the '
                || 'last {FRESHNESS_WINDOW_HOURS} hours - the stream or the producer is down'
            ) AS freshness_check
            FROM {CATALOG}.{SCHEMA}.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL {FRESHNESS_WINDOW_HOURS} HOURS
        """,
        retries=0,   # a stale stream will still be stale in 5 minutes; fail fast and loud
    )

    # ------------------------------------------------------------------
    # 2. Table maintenance.
    #    A 30-second micro-batch trigger writes a new file every 30 seconds.
    #    Left alone, Silver becomes thousands of tiny files and every read pays
    #    for it. OPTIMIZE compacts them; ZORDER co-locates by the columns the
    #    dashboard filters on.
    # ------------------------------------------------------------------
    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            f"OPTIMIZE {CATALOG}.{SCHEMA}.silver_order_events ZORDER BY (event_ts, event_type)",
            f"OPTIMIZE {CATALOG}.{SCHEMA}.gold_revenue_5min ZORDER BY (window_start)",
        ],
    )

    # ------------------------------------------------------------------
    # 3. Daily rollup.
    #    CREATE OR REPLACE rather than an append, so the task is idempotent:
    #    running it five times in one hour leaves exactly the same table.
    # ------------------------------------------------------------------
    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=f"""
            CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_daily_summary AS
            SELECT
                event_date,
                COUNT_IF(event_type = 'order_placed')     AS orders_placed,
                COUNT_IF(event_type = 'payment_received') AS orders_paid,
                COUNT_IF(event_type = 'order_shipped')    AS orders_shipped,
                COUNT_IF(event_type = 'order_cancelled')  AS orders_cancelled,
                ROUND(SUM(CASE WHEN event_type = 'payment_received'
                               THEN order_amount ELSE 0 END), 2) AS revenue,
                COUNT(DISTINCT customer_id)               AS distinct_customers,
                current_timestamp()                       AS refreshed_at
            FROM {CATALOG}.{SCHEMA}.silver_order_events
            WHERE event_date IS NOT NULL
            GROUP BY event_date
        """,
    )

    # 4. Order matters: do not compact or roll up numbers the gate has not cleared.
    check_fresh_events >> optimize_tables >> refresh_daily_summary
