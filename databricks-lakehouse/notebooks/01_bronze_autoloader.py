# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Auto Loader ingestion
# MAGIC Incrementally picks up **new** files from the landing volume and appends them to Delta tables
# MAGIC exactly once (checkpointed). Everything stays as strings; only metadata columns are added.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse_dev")
catalog = dbutils.widgets.get("catalog")

landing = f"/Volumes/{catalog}/bronze/landing"
checkpoints = f"{landing}/_checkpoints"

# COMMAND ----------

from pyspark.sql import functions as F


def ingest(entity: str) -> None:
    """Auto Loader: landing/<entity>/*.csv  ->  <catalog>.bronze.<entity>"""
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("cloudFiles.inferColumnTypes", False)          # keep raw strings in Bronze
        .option("cloudFiles.schemaLocation", f"{checkpoints}/{entity}/schema")
        .load(f"{landing}/{entity}/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", f"{checkpoints}/{entity}/data")
        .option("mergeSchema", True)                           # tolerate new upstream columns
        .trigger(availableNow=True)                            # process what's there, then stop (batch-style)
        .toTable(f"{catalog}.bronze.{entity}")
        .awaitTermination()
    )
    print(f"{entity}: bronze now has {spark.table(f'{catalog}.bronze.{entity}').count()} rows")


for entity in ["customers", "orders"]:
    ingest(entity)
