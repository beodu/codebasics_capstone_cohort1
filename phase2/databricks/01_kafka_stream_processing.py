# Databricks notebook source
# MAGIC %md
# MAGIC # AtliQ Phase 2 — Kafka to Delta with Structured Streaming
# MAGIC Bronze -> Silver -> Gold, all three as continuous streams.
# MAGIC
# MAGIC **Free Edition (serverless) rules honoured here:** checkpoints live in a Unity
# MAGIC Catalog **Volume** (no DBFS), streams write to **managed tables**, and every
# MAGIC stream has its **own** checkpoint folder.

# COMMAND ----------
# Credentials: in a real deployment these come from a Databricks secret scope, e.g.
#   dbutils.secrets.get(scope="atliq", key="kafka_api_key")
# They are inlined here only because Free Edition has no secret scope UI.
KAFKA_BOOTSTRAP = "pkc-xxxxx.region.provider.confluent.cloud:9092"   # <-- yours
KAFKA_API_KEY   = "YOUR_API_KEY"
KAFKA_API_SECRET = "YOUR_API_SECRET"
TOPIC = "atliq.orders.events"

CATALOG, SCHEMA = "atliq", "streaming"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# One-time setup:
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

# Databricks ships a shaded Kafka client, so the JAAS login module is the
# kafkashaded one — the plain org.apache.kafka class will not resolve.
JAAS = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";'
)

# COMMAND ----------
# MAGIC %md ## TASK 1 — Bronze: raw events off Kafka, no parsing
# MAGIC Everything the broker gave us is kept verbatim. If the JSON schema turns out
# MAGIC to be wrong, or Silver has a bug, Bronze can be replayed — nothing is lost
# MAGIC because nothing was interpreted.

# COMMAND ----------
bronze_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", JAAS)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("event_key"),
            F.col("value").cast("string").alias("event_value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.current_timestamp().alias("ingested_at"),
        )
)

bronze_query = (
    bronze_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/bronze")   # own checkpoint
        .queryName("bronze_order_events")
        .trigger(processingTime="30 seconds")
        .toTable(f"{CATALOG}.{SCHEMA}.bronze_order_events")
)

# COMMAND ----------
# MAGIC %md ## TASK 2 — Silver: parse, de-duplicate, handle late data
# MAGIC Reads *from the Bronze table*, not from Kafka again. Bronze is the replayable
# MAGIC record of what arrived; Silver is the cleaned, exactly-once view of it.
# MAGIC
# MAGIC The 10-minute watermark tells Spark how long to hold `event_id`s in state for
# MAGIC duplicate checking. Without it, that state would grow without bound; with it,
# MAGIC an event replayed inside 10 minutes is dropped and state older than that is
# MAGIC released.

# COMMAND ----------
event_schema = StructType([
    StructField("event_id",       StringType()),
    StructField("event_type",     StringType()),
    StructField("event_ts",       StringType()),
    StructField("order_id",       IntegerType()),
    StructField("customer_id",    IntegerType()),
    StructField("city",           StringType()),
    StructField("product_id",     IntegerType()),
    StructField("quantity",       IntegerType()),
    StructField("order_amount",   DoubleType()),
    StructField("payment_method", StringType()),
])

silver_stream = (
    spark.readStream
        .table(f"{CATALOG}.{SCHEMA}.bronze_order_events")
        .select(F.from_json(F.col("event_value"), event_schema).alias("e"),
                F.col("kafka_ts"))
        .select("e.*", "kafka_ts")
        .filter(F.col("event_id").isNotNull())          # unparseable rows stay in Bronze
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["event_id"])                   # exactly-once on the business key
)

silver_query = (
    silver_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/silver")   # own checkpoint
        .queryName("silver_order_events")
        .trigger(processingTime="30 seconds")
        .toTable(f"{CATALOG}.{SCHEMA}.silver_order_events")
)

# COMMAND ----------
# MAGIC %md ## TASK 3 — Gold: the live revenue ticker
# MAGIC Five-minute tumbling windows over `payment_received` events only — an order
# MAGIC that was placed but never paid is not revenue.
# MAGIC
# MAGIC A window is emitted only once the watermark has passed its end, because until
# MAGIC then a late event could still change the total. That is why rows appear
# MAGIC 10-15 minutes behind wall-clock time: 5 minutes of window plus 10 minutes of
# MAGIC watermark slack. Waiting is the correct behaviour, not lag.

# COMMAND ----------
gold_stream = (
    spark.readStream
        .table(f"{CATALOG}.{SCHEMA}.silver_order_events")
        .filter(F.col("event_type") == "payment_received")
        .withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "5 minutes"))
        .agg(
            F.count("*").alias("orders_paid"),
            F.sum("order_amount").alias("revenue"),
            F.approx_count_distinct("customer_id").alias("distinct_customers"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "orders_paid",
            F.round("revenue", 2).alias("revenue"),
            "distinct_customers",
        )
)

gold_query = (
    gold_stream.writeStream
        .outputMode("append")                            # append = closed windows only
        .option("checkpointLocation", f"{CKPT}/gold")     # own checkpoint
        .queryName("gold_revenue_5min")
        .trigger(processingTime="1 minute")
        .toTable(f"{CATALOG}.{SCHEMA}.gold_revenue_5min")
)

# COMMAND ----------
# MAGIC %md ## Stream health (run while the streams are up)

# COMMAND ----------
for q in spark.streams.active:
    print(f"{q.name:24} id={q.id}  status={q.status['message']}")
    print(f"    last progress: {q.lastProgress.get('numInputRows') if q.lastProgress else 'n/a'} rows in")

# COMMAND ----------
# MAGIC %md ## Verify

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS events
# MAGIC FROM atliq.streaming.silver_order_events GROUP BY event_type;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM atliq.streaming.gold_revenue_5min ORDER BY window_start DESC LIMIT 12;

# COMMAND ----------
# MAGIC %md
# MAGIC ### De-duplication proof
# MAGIC Bronze counts every physical arrival; Silver counts distinct `event_id`s.
# MAGIC If the producer is restarted with `startingOffsets=earliest`, Bronze grows
# MAGIC and Silver does not.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM atliq.streaming.bronze_order_events)              AS bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM atliq.streaming.silver_order_events)              AS silver_rows,
# MAGIC   (SELECT COUNT(DISTINCT event_id) FROM atliq.streaming.silver_order_events) AS silver_distinct_ids;

# COMMAND ----------
# MAGIC %md ### Stop all streams (end of a demo session)

# COMMAND ----------
# for q in spark.streams.active:
#     q.stop()
