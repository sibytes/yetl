# Databricks notebook source
# MAGIC %pip install regex pyaml

# COMMAND ----------

from yetl_flow import (
  yetl_flow, 
  IDataflow, 
  Context, 
  Timeslice, 
  TimesliceUtcNow, 
  OverwriteSave, 
  Save
)
from pyspark.sql.functions import *
import logging
from typing import Type

@yetl_flow()
def customer_landing_to_rawdb_csv(
  context: Context, 
  dataflow: IDataflow, 
  timeslice: Timeslice = TimesliceUtcNow(), 
  save_type: Type[Save] = None
):
    context.log.info(
        f"Executing Dataflow {context.app_name} with timeslice={timeslice}, retries={dataflow.retries}"
    )

    # the config for this dataflow has 2 landing sources that are joined and written to delta table
    # delta tables are automatically created and if configured schema exceptions
    # are loaded syphened into a schema exception table
    df_cust = dataflow.source_df("landing.customer")
    df_prefs = dataflow.source_df("landing.customer_preferences")
    df = df_cust.join(df_prefs, "id", "inner")
    df = df_cust
    dataflow.destination_df("raw.customer", df)


# if timeslice isn't provided will default to TimesliceUtcNow
timeslice = Timeslice(2022, '*', '*')
results = customer_landing_to_rawdb_csv(
    timeslice=timeslice
)



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
