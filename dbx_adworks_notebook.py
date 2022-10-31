# Databricks notebook source
# MAGIC %pip install regex pyaml

# COMMAND ----------

from yetl.flow import (
    yetl_flow,
    IDataflow,
    IContext,
    Timeslice,
    TimesliceUtcNow,
    Save,
)
from yetl.workflow import multithreaded as yetl_wf
from pyspark.sql.functions import *
from typing import Type
import json
import yaml

# from yetl import async_load
project = "adworks"


@yetl_flow(project=project, pipeline_name="landing_to_raw")
def landing_to_raw(
    table: str,
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load raw delta tables"""

    df = dataflow.source_df(f"{context.project}_landing.{table}")

    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )
    dataflow.destination_df(f"{context.project}_raw.{table}", df, save=save)


# COMMAND ----------


with open(
    f"./config/project/{project}/{project}_tables.yml", "r", encoding="utf-8"
) as f:
    metdata = yaml.safe_load(f)

tables: list = [t["table"] for t in metdata.get("tables")]
timeslice = Timeslice(2011, 1, 1)

yetl_wf.load(project, tables, landing_to_raw, timeslice)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from adworks_raw.productionproduct

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from demo_landing.exceptions

# COMMAND ----------

dbutils.notebook.exit("YETL!")

# COMMAND ----------

dbutils.fs.ls("/mnt/datalake/yetl_data/landing")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC #clear down
# MAGIC
# MAGIC spark.sql("drop database if exists adworks_landing cascade")
# MAGIC spark.sql("drop database if exists adworks_raw cascade")
# MAGIC files = dbutils.fs.ls("/mnt/datalake/yetl_data")
# MAGIC print(files)
# MAGIC
# MAGIC for f in files:
# MAGIC
# MAGIC   if f.name != "landing/":
# MAGIC     print(f"deleting the path {f.path}")
# MAGIC     dbutils.fs.rm(f.path, True)
