# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean, upsert, SCD Type 2
# MAGIC * `silver.orders` - cleaned orders, **MERGE-upserted** on `order_id` (late corrections overwrite, no duplicates)
# MAGIC * `silver.dim_customer` - **SCD Type 2** history of customer attributes (city / segment changes)

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))   # repo `src/` is synced next to `notebooks/` by the bundle

dbutils.widgets.text("catalog", "lakehouse_dev")
catalog = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

from lakehouse import delta_ops, transforms

# COMMAND ----------

# MAGIC %md ## Orders -> MERGE upsert

# COMMAND ----------

orders_target = f"{catalog}.silver.orders"

clean = transforms.clean_orders(spark.table(f"{catalog}.bronze.orders"))
delta_ops.upsert_orders(spark, clean, orders_target)

# Table-level data quality: Delta rejects any future write that violates these.
for name, rule in {"quantity_positive": "quantity > 0", "unit_price_positive": "unit_price > 0"}.items():
    try:
        spark.sql(f"ALTER TABLE {orders_target} ADD CONSTRAINT {name} CHECK ({rule})")
    except Exception as e:  # constraint already exists on re-runs
        print(f"constraint {name}: {type(e).__name__} (probably already added)")

print(f"silver.orders: {spark.table(orders_target).count()} rows")

# COMMAND ----------

# MAGIC %md ## Customers -> SCD Type 2

# COMMAND ----------

dim = f"{catalog}.silver.dim_customer"
delta_ops.ensure_dim_customer(spark, dim)

# Only process change records newer than what the dimension has already absorbed,
# otherwise re-running would replay an old snapshot as if it were a new change.
watermark = spark.table(dim).agg(F.max("start_date")).first()[0]

incoming = (
    spark.table(f"{catalog}.bronze.customers")
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("name").alias("name"),
        F.lower(F.trim("email")).alias("email"),
        F.trim("city").alias("city"),
        F.trim("segment").alias("segment"),
        F.expr("try_cast(updated_at AS date)").alias("effective_date"),
        "_ingested_at",
        "_source_file",
    )
    .dropna(subset=["customer_id", "effective_date"])
)
if watermark is not None:
    incoming = incoming.filter(F.col("effective_date") > F.lit(watermark))

# Apply change dates oldest -> newest so history is built in the right order (supports backfills).
effective_dates = [r[0] for r in incoming.select("effective_date").distinct().orderBy("effective_date").collect()]

for d in effective_dates:
    batch = transforms.dedupe_latest(incoming.filter(F.col("effective_date") == F.lit(d)), "customer_id")
    staged = transforms.stage_scd2_updates(batch, spark.table(dim), "customer_id")
    delta_ops.apply_scd2(spark, staged, dim)
    print(f"applied customer changes effective {d}")

print(f"silver.dim_customer: {spark.table(dim).count()} rows "
      f"({spark.table(dim).filter('is_current').count()} current)")
