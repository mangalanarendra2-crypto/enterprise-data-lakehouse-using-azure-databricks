"""Pure PySpark DataFrame transformations (no Delta / Unity Catalog dependency).

Kept separate from the notebooks so the business rules can be unit-tested with a local
SparkSession and reused unchanged on Databricks.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CUSTOMER_TRACKED_COLS = ["name", "email", "city", "segment"]

ORDER_COLUMNS = [
    "order_id", "customer_id", "product", "category", "quantity", "unit_price",
    "order_date", "country", "status", "total_amount", "_ingested_at", "_source_file",
]


# ---------------------------------------------------------------- shared helpers
def dedupe_latest(df: DataFrame, key: str) -> DataFrame:
    """Keep one row per key: the most recently ingested (file name breaks ties within a batch)."""
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_row_hash(df: DataFrame, cols: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


# ---------------------------------------------------------------- Silver: orders
def clean_orders(bronze: DataFrame) -> DataFrame:
    """Bronze (all strings) -> typed, validated, de-duplicated orders.

    try_cast is used so malformed values become NULL (and are then filtered out)
    instead of failing the job when ANSI mode is on.
    """
    df = (
        bronze
        .withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("customer_id", F.when(F.col("customer_id") == "", F.lit(None)).otherwise(F.col("customer_id")))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("quantity", F.expr("try_cast(quantity AS int)"))
        .withColumn("unit_price", F.expr("try_cast(unit_price AS double)"))
        .withColumn("order_date", F.expr("try_cast(order_date AS date)"))
        .dropna(subset=["order_id", "customer_id", "order_date", "quantity", "unit_price"])
        .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
    )
    df = dedupe_latest(df, "order_id")
    df = df.withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    return df.select(*ORDER_COLUMNS)


# ---------------------------------------------------------------- Silver: customer SCD Type 2
def stage_scd2_updates(incoming: DataFrame, current_dim: DataFrame, key: str,
                       tracked_cols: list[str] = CUSTOMER_TRACKED_COLS) -> DataFrame:
    """Build the 'staged updates' DataFrame for a Delta SCD2 MERGE.

    `incoming` must contain: key, tracked_cols, effective_date (one row per key).
    Each changed customer produces TWO rows:
      * merge_key = key   -> matches the current dimension row so MERGE can expire it
      * merge_key = NULL  -> never matches, so MERGE inserts it as the new current version
    New customers produce only the NULL-key (insert) row. Unchanged customers produce nothing.
    """
    inc = add_row_hash(incoming, tracked_cols)
    cur = current_dim.filter("is_current").select(key, F.col("row_hash").alias("_cur_hash"))

    changed = (
        inc.join(cur, key, "left")
        .filter(F.col("_cur_hash").isNull() | (F.col("_cur_hash") != F.col("row_hash")))
    )
    to_expire = changed.filter(F.col("_cur_hash").isNotNull()).withColumn("merge_key", F.col(key).cast("string"))
    to_insert = changed.withColumn("merge_key", F.lit(None).cast("string"))
    return to_expire.unionByName(to_insert).drop("_cur_hash")


# ---------------------------------------------------------------- Gold
def _active(orders: DataFrame) -> DataFrame:
    return orders.filter(F.col("status") != "cancelled")


def daily_revenue(orders: DataFrame) -> DataFrame:
    return (
        _active(orders).groupBy("order_date")
        .agg(F.countDistinct("order_id").alias("total_orders"),
             F.round(F.sum("total_amount"), 2).alias("total_revenue"))
        .orderBy("order_date")
    )


def revenue_by_segment(orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Monthly revenue by the customer's segment *as it was on the order date* (point-in-time join)."""
    o, d = _active(orders).alias("o"), dim_customer.alias("d")
    in_version = (
        (F.col("o.customer_id") == F.col("d.customer_id"))
        & (F.col("o.order_date") >= F.col("d.start_date"))
        & (F.col("d.end_date").isNull() | (F.col("o.order_date") < F.col("d.end_date")))
    )
    return (
        o.join(d, in_version, "left")
        .withColumn("segment", F.coalesce(F.col("d.segment"), F.lit("UNKNOWN")))
        .withColumn("order_month", F.trunc(F.col("o.order_date"), "month"))
        .groupBy("order_month", "segment")
        .agg(F.countDistinct("o.order_id").alias("orders"),
             F.countDistinct("o.customer_id").alias("customers"),
             F.round(F.sum("o.total_amount"), 2).alias("revenue"))
        .orderBy("order_month", "segment")
    )


def customer_360(orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """One row per current customer with lifetime order metrics."""
    agg = (
        _active(orders).groupBy("customer_id")
        .agg(F.countDistinct("order_id").alias("total_orders"),
             F.round(F.sum("total_amount"), 2).alias("lifetime_value"),
             F.min("order_date").alias("first_order_date"),
             F.max("order_date").alias("last_order_date"))
    )
    current = dim_customer.filter("is_current").select("customer_id", "name", "email", "city", "segment")
    return current.join(agg, "customer_id", "left").fillna({"total_orders": 0, "lifetime_value": 0.0})
