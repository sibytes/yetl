# Databricks notebook source
# MAGIC %pip install regex pyaml

# COMMAND ----------

from yetl.flow import (
    yetl_flow,
    IDataflow,
    IContext,
    Timeslice,
    TimesliceUtcNow,
    OverwriteSave,
    OverwriteSchemaSave,
    MergeSave,
    Save,
)
from pyspark.sql.functions import *
from typing import Type
import json


@yetl_flow(log_level="ERROR")
def batch_text_csv_to_delta_permissive_1(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table.

    this is a test pipeline that can be run just to check everything is setup and configured
    correctly.
    """

    # the config for this dataflow has 2 landing sources that are joined
    # and written to delta table
    # delta tables are automatically created and if configured schema exceptions
    # are loaded syphened into a schema exception table
    df_cust = dataflow.source_df("landing.customer")
    df_prefs = dataflow.source_df("landing.customer_preferences")

    context.log.info("Joining customers with customer_preferences")
    df = df_cust.join(df_prefs, "id", "inner")
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    dataflow.destination_df("raw.customer", df, save=save)


# COMMAND ----------


# incremental load
timeslice = Timeslice(2021, 1, 1)
# timeslice = Timeslice(2022, 7, 12)
# timeslice = Timeslice(2022, 7, "*")
results = batch_text_csv_to_delta_permissive_1(timeslice=timeslice)

# reload load
# timeslice = Timeslice(2022, "*", "*")


# COMMAND ----------

results

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from raw.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from landing.exceptions

# COMMAND ----------

dbutils.notebook.exit("YETL!")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC #clear down
# MAGIC
# MAGIC spark.sql("drop database if exists landing cascade")
# MAGIC spark.sql("drop database if exists raw cascade")
# MAGIC files = dbutils.fs.ls("/mnt/datalake/yetl_data")
# MAGIC print(files)
# MAGIC
# MAGIC for f in files:
# MAGIC
# MAGIC   if f.name != "landing/":
# MAGIC     print(f"deleting the path {f.path}")
# MAGIC     dbutils.fs.rm(f.path, True)

# COMMAND ----------

dbutils.fs.rm("file:/yetl/", True)

# COMMAND ----------

dbutils.fs.ls("file:/yetl/runs")
