# Databricks notebook source
# MAGIC %md
# MAGIC # Idempotency proof
# MAGIC
# MAGIC Run this, run the full nightly job a second time, then run it again. The two
# MAGIC printouts must be identical. If they are not, a stage is appending where it
# MAGIC should be merging or overwriting.
# MAGIC
# MAGIC Save both outputs into `docs/idempotency_proof.md`.

# COMMAND ----------
from pyspark.sql import functions as F

snapshot = spark.sql("""
    SELECT
        (SELECT COUNT(*) FROM atliq.silver.orders)        AS silver_orders,
        (SELECT COUNT(*) FROM atliq.silver.order_items)   AS silver_order_items,
        (SELECT COUNT(*) FROM atliq.silver.payments)      AS silver_payments,
        (SELECT COUNT(*) FROM atliq.gold.fact_sales)      AS fact_rows,
        (SELECT ROUND(SUM(gross_revenue), 2) FROM atliq.gold.fact_sales) AS total_gross_revenue,
        (SELECT COUNT(*) FROM atliq.gold.dim_customer)    AS dim_customers,
        (SELECT COUNT(*) FROM atliq.gold.dim_product)     AS dim_products,
        current_timestamp() AS captured_at
""")
display(snapshot)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Duplicate detectors — all three must return zero rows

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 1. fact_sales grain: exactly one row per order item
# MAGIC SELECT order_item_id, COUNT(*) AS n
# MAGIC FROM atliq.gold.fact_sales GROUP BY order_item_id HAVING COUNT(*) > 1;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 2. Silver orders: the MERGE key must be unique
# MAGIC SELECT order_id, COUNT(*) AS n
# MAGIC FROM atliq.silver.orders GROUP BY order_id HAVING COUNT(*) > 1;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 3. Reconciliation: order header amount must equal the sum of its line items.
# MAGIC -- A mismatch here is the signature of a double-counted line item.
# MAGIC SELECT o.order_id, o.order_amount, SUM(oi.quantity * oi.item_price) AS line_total
# MAGIC FROM atliq.silver.orders o
# MAGIC JOIN atliq.silver.order_items oi ON o.order_id = oi.order_id
# MAGIC GROUP BY o.order_id, o.order_amount
# MAGIC HAVING ABS(o.order_amount - SUM(oi.quantity * oi.item_price)) > 0.01;

# COMMAND ----------
# MAGIC %md ## Run history — did last night actually run?

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT run_date, stage, table_name, rows_in, rows_after, status,
# MAGIC        ROUND(duration_secs, 1) AS secs
# MAGIC FROM atliq.ops.pipeline_run_log
# MAGIC ORDER BY started_at DESC LIMIT 30;
