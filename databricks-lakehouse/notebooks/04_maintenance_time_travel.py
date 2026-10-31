# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Delta maintenance & time travel
# MAGIC Keeps tables fast and shows lakehouse features: file compaction, Z-ordering, history, time travel.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md ## Compact small files & co-locate data for common filters

# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.silver.orders ZORDER BY (customer_id, order_date)")
spark.sql(f"OPTIMIZE {catalog}.silver.dim_customer")
for t in ["daily_revenue", "revenue_by_segment", "customer_360"]:
    spark.sql(f"OPTIMIZE {catalog}.gold.{t}")

# COMMAND ----------

# MAGIC %md ## Clean up files no longer referenced (keeps 7 days so time travel still works)

# COMMAND ----------

spark.sql(f"VACUUM {catalog}.silver.orders RETAIN 168 HOURS")
spark.sql(f"VACUUM {catalog}.silver.dim_customer RETAIN 168 HOURS")

# COMMAND ----------

# MAGIC %md ## Audit trail: every write is a table version

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {catalog}.silver.orders")
        .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md ## Time travel: compare today with the first version
# MAGIC To undo a bad write: `RESTORE TABLE <t> TO VERSION AS OF <n>`

# COMMAND ----------

now_cnt = spark.table(f"{catalog}.silver.orders").count()
v0_cnt = spark.sql(f"SELECT count(*) AS c FROM {catalog}.silver.orders VERSION AS OF 0").first()["c"]
print(f"silver.orders rows: version 0 = {v0_cnt}, latest = {now_cnt}")

# SCD2 history for one customer that changed:
display(spark.sql(f"""
    SELECT * FROM {catalog}.silver.dim_customer
    WHERE customer_id IN (SELECT customer_id FROM {catalog}.silver.dim_customer GROUP BY customer_id HAVING count(*) > 1 LIMIT 1)
    ORDER BY start_date
"""))
