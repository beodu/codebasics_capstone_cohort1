# Databricks notebook source
# MAGIC %md
# MAGIC # AtliQ Phase 1 — Bronze (Parquet) to Silver (Delta, Unity Catalog)
# MAGIC
# MAGIC Two loading patterns, chosen per table by how the source behaves:
# MAGIC
# MAGIC | Pattern | Tables | Why |
# MAGIC |---|---|---|
# MAGIC | Clean + overwrite | customers, products, supplier_price_list, marketing_spend | Small reference data; ADF lands a full snapshot each night |
# MAGIC | MERGE on business key | orders, order_items, payments | Rows can be updated and can arrive in more than one batch |
# MAGIC
# MAGIC The MERGE is what makes this idempotent. Appending incremental data straight
# MAGIC into Silver would double-count every order whose status changed after it was
# MAGIC first ingested.
# MAGIC
# MAGIC Called by the orchestrator with a `run_date` widget so it reads exactly the
# MAGIC Bronze batch ADF just landed.

# COMMAND ----------
dbutils.widgets.text("run_date", "", "Run date (yyyy-MM-dd)")
dbutils.widgets.text("storage_account", "atliqlakehouse", "ADLS Gen2 account")

import uuid
from datetime import datetime

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

RUN_DATE = dbutils.widgets.get("run_date")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

if not RUN_DATE:
    raise ValueError("run_date widget is empty. The orchestrator must pass the "
                     "ADF run_start_at date so Silver reads the right batch.")

BRONZE = f"abfss://lakehouse@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze"
RUN_ID = str(uuid.uuid4())

print(f"run_id={RUN_ID}  run_date={RUN_DATE}")
print(f"bronze root = {BRONZE}")

# COMMAND ----------
# MAGIC %md ## Audit helper — every table logs its own outcome

# COMMAND ----------
def log_run(stage, table_name, rows_in, rows_after, status, started_at,
            error_message=None):
    finished_at = datetime.utcnow()
    row = [(RUN_ID, datetime.strptime(RUN_DATE, "%Y-%m-%d").date(), stage,
            table_name, rows_in, rows_after, status, error_message,
            started_at, finished_at,
            (finished_at - started_at).total_seconds())]
    cols = ["run_id", "run_date", "stage", "table_name", "rows_in", "rows_after",
            "status", "error_message", "started_at", "finished_at", "duration_secs"]
    (spark.createDataFrame(row, cols)
        .write.format("delta").mode("append")
        .saveAsTable("atliq.ops.pipeline_run_log"))
    print(f"  [{status}] {table_name}: {rows_in} in -> {rows_after} rows in target")


# COMMAND ----------
# MAGIC %md ## Pattern A — full snapshot tables: clean, then overwrite
# MAGIC Overwrite is trivially idempotent: the target is whatever the latest
# MAGIC snapshot says, no matter how many times it runs.

# COMMAND ----------
def load_full(table_name, clean_fn, target):
    started = datetime.utcnow()
    try:
        raw = spark.read.parquet(f"{BRONZE}/{table_name}")
        rows_in = raw.count()
        cleaned = clean_fn(raw)
        (cleaned.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target))
        rows_after = spark.table(target).count()
        log_run("bronze_to_silver", table_name, rows_in, rows_after, "SUCCESS", started)
    except Exception as exc:
        log_run("bronze_to_silver", table_name, 0, 0, "FAILED", started, str(exc))
        raise


load_full(
    "customers",
    lambda df: (df
        .withColumn("customer_name", F.trim("customer_name"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("signup_date", F.to_date("signup_date"))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])),
    "atliq.silver.customers",
)

load_full(
    "products",
    lambda df: (df
        .withColumn("product_name", F.trim("product_name"))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])),
    "atliq.silver.products",
)

load_full(
    "supplier_price_list",
    lambda df: (df
        .withColumn("supplier_cost", F.col("supplier_cost").cast("decimal(10,2)"))
        .withColumn("effective_date", F.to_date("effective_date"))
        .filter(F.col("product_id").isNotNull())
        # One product can appear more than once; keep the latest effective price.
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("product_id").orderBy(F.col("effective_date").desc())))
        .filter("rn = 1").drop("rn")),
    "atliq.silver.supplier_price_list",
)

load_full(
    "marketing_spend",
    lambda df: (df
        .withColumn("spend_date", F.to_date("spend_date"))
        .withColumn("spend_amount", F.col("spend_amount").cast("decimal(12,2)"))
        .withColumn("clicks", F.col("clicks").cast("int"))
        .withColumn("channel", F.trim("channel"))
        .filter(F.col("spend_date").isNotNull())
        .dropDuplicates(["spend_date", "channel", "campaign"])),
    "atliq.silver.marketing_spend",
)

# COMMAND ----------
# MAGIC %md ## Pattern B — incremental tables: collapse the batch, then MERGE
# MAGIC Two de-duplication steps, and both are needed:
# MAGIC 1. **Within the batch** — one order can change twice between two runs, so the
# MAGIC    batch itself can hold two rows for one `order_id`. A window function keeps
# MAGIC    only the newest.
# MAGIC 2. **Against the target** — MERGE on the business key updates in place instead
# MAGIC    of appending a second row.

# COMMAND ----------
def load_incremental(table_name, key, order_col, target, clean_fn,
                     update_when_newer=True):
    started = datetime.utcnow()
    src_path = f"{BRONZE}/{table_name}/ingest_date={RUN_DATE}"
    try:
        try:
            batch = spark.read.parquet(src_path)
        except Exception:
            # An empty night is a normal outcome, not a failure.
            print(f"  no batch at {src_path} - nothing to merge for {table_name}")
            rows_after = (spark.table(target).count()
                          if spark.catalog.tableExists(target) else 0)
            log_run("bronze_to_silver", table_name, 0, rows_after, "SUCCESS", started)
            return

        rows_in = batch.count()

        w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
        src = (clean_fn(batch)
               .withColumn("rn", F.row_number().over(w))
               .filter("rn = 1").drop("rn")
               .filter(F.col(key).isNotNull()))

        (DeltaTable.createIfNotExists(spark)
            .tableName(target).addColumns(src.schema).execute())

        merger = (DeltaTable.forName(spark, target).alias("t")
                  .merge(src.alias("s"), f"t.{key} = s.{key}"))
        if update_when_newer:
            merger = merger.whenMatchedUpdateAll(
                condition=f"s.{order_col} > t.{order_col}")
        merger.whenNotMatchedInsertAll().execute()

        rows_after = spark.table(target).count()
        log_run("bronze_to_silver", table_name, rows_in, rows_after, "SUCCESS", started)
    except Exception as exc:
        log_run("bronze_to_silver", table_name, 0, 0, "FAILED", started, str(exc))
        raise


load_incremental(
    "orders", key="order_id", order_col="updated_at",
    target="atliq.silver.orders",
    clean_fn=lambda df: (df
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("status", F.initcap(F.trim("status")))
        .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))),
)

load_incremental(
    "payments", key="payment_id", order_col="updated_at",
    target="atliq.silver.payments",
    clean_fn=lambda df: (df
        .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
        .withColumn("method", F.trim("method"))
        .withColumn("paid_at", F.to_timestamp("paid_at"))),
)

# Line items are insert-only: once written they never change, so there is
# nothing to update. Insert-only MERGE still protects against a replayed batch.
load_incremental(
    "order_items", key="order_item_id", order_col="created_at",
    target="atliq.silver.order_items",
    update_when_newer=False,
    clean_fn=lambda df: (df
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("item_price", F.col("item_price").cast("decimal(10,2)"))
        .filter(F.col("quantity") > 0)),
)

# COMMAND ----------
# MAGIC %md ## Maintenance and run summary

# COMMAND ----------
for t in ["orders", "order_items", "payments", "customers", "products"]:
    spark.sql(f"OPTIMIZE atliq.silver.{t}")

# COMMAND ----------
display(spark.sql(f"""
    SELECT table_name, rows_in, rows_after, status, ROUND(duration_secs, 1) AS secs
    FROM atliq.ops.pipeline_run_log
    WHERE run_id = '{RUN_ID}'
    ORDER BY table_name
"""))

# COMMAND ----------
# Fail the job loudly if any table failed, so the orchestrator alert fires.
failures = spark.sql(f"""
    SELECT table_name FROM atliq.ops.pipeline_run_log
    WHERE run_id = '{RUN_ID}' AND status = 'FAILED'
""").collect()
if failures:
    raise Exception(f"Silver load failed for: {[r.table_name for r in failures]}")

dbutils.notebook.exit(f"SUCCESS run_id={RUN_ID}")
