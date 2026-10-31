"""Delta Lake write operations (MERGE upsert, SCD Type 2). Requires a Delta-enabled Spark (Databricks)."""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DIM_CUSTOMER_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    customer_id STRING,
    name        STRING,
    email       STRING,
    city        STRING,
    segment     STRING,
    row_hash    STRING,
    start_date  DATE,
    end_date    DATE,
    is_current  BOOLEAN
) USING DELTA
"""


def ensure_dim_customer(spark: SparkSession, table: str) -> None:
    spark.sql(DIM_CUSTOMER_DDL.format(table=table))


def upsert_orders(spark: SparkSession, df: DataFrame, target: str) -> None:
    """Idempotent upsert on order_id. A re-sent order only overwrites the stored row if it is newer."""
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        return

    newer = (
        "s._ingested_at > t._ingested_at OR "
        "(s._ingested_at = t._ingested_at AND s._source_file >= t._source_file)"
    )
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(df.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition=newer)
        .whenNotMatchedInsertAll()
        .execute()
    )


def apply_scd2(spark: SparkSession, staged: DataFrame, target: str, key: str = "customer_id") -> None:
    """Apply the output of transforms.stage_scd2_updates to a Delta dimension table."""
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": F.lit(False), "end_date": F.col("s.effective_date")},
        )
        .whenNotMatchedInsert(values={
            "customer_id": F.col("s.customer_id"),
            "name": F.col("s.name"),
            "email": F.col("s.email"),
            "city": F.col("s.city"),
            "segment": F.col("s.segment"),
            "row_hash": F.col("s.row_hash"),
            "start_date": F.col("s.effective_date"),
            "end_date": F.lit(None).cast("date"),
            "is_current": F.lit(True),
        })
        .execute()
    )
