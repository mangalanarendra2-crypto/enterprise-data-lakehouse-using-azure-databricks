# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: business marts
# MAGIC Rebuilt from Silver on every run (small data). Registered in Unity Catalog so BI tools
# MAGIC (Power BI, Databricks SQL dashboards) can query them by name.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

dbutils.widgets.text("catalog", "lakehouse_dev")
catalog = dbutils.widgets.get("catalog")

from lakehouse import transforms

# COMMAND ----------

orders = spark.table(f"{catalog}.silver.orders")
dim = spark.table(f"{catalog}.silver.dim_customer")

marts = {
    "daily_revenue": transforms.daily_revenue(orders),
    "revenue_by_segment": transforms.revenue_by_segment(orders, dim),   # point-in-time segment (SCD2 join)
    "customer_360": transforms.customer_360(orders, dim),
}

for name, df in marts.items():
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.gold.{name}"))
    print(f"gold.{name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Quick data-quality gate
# MAGIC Fails the job (and therefore the workflow run) if basic invariants are broken.

# COMMAND ----------

silver_orders = spark.table(f"{catalog}.silver.orders")

assert silver_orders.count() > 0, "silver.orders is empty"
assert silver_orders.filter("customer_id IS NULL OR order_id IS NULL").count() == 0, "null keys in silver.orders"
assert silver_orders.groupBy("order_id").count().filter("count > 1").count() == 0, "duplicate order_ids in silver"
assert dim.filter("is_current").groupBy("customer_id").count().filter("count > 1").count() == 0, \
    "a customer has more than one current dimension row"
print("All data-quality checks passed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: revenue trend by segment
# MAGIC -- SELECT * FROM lakehouse_dev.gold.revenue_by_segment ORDER BY order_month, segment
