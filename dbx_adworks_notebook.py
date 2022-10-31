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


# def load():

#     with open(
#         f"./config/project/{project}/{project}_tables.yml", "r", encoding="utf-8"
#     ) as f:
#         metdata = yaml.safe_load(f)

#     tables: list = [t["table"] for t in metdata.get("tables")]
#     failed = []

#     for table in tables:

#         timeslice = Timeslice(2011, 1, 1)
#         results = landing_to_raw(timeslice=timeslice, table=table)
#         if results["error"].get("count", 0) > 0:
#             failed.append(results)
#         else:
#             print(f"Loaded {project}.{table}")

#     print(json.dumps(failed, indent=4, default=str))






# COMMAND ----------



def load(tables:list, function:Callable, timeslice:Timeslice):

  def _load(q):
    table, function, timeslice = q.get()
    print(f"Loading {project}.{table}")
    timeslice = Timeslice(2011, 1, 1)
    results = function(timeslice=timeslice, table=table)
    if results["error"].get("count", 0) > 0:
        print(results)
    else:
        print(f"Loaded {project}.{table}")
    q.task_done()


  q = Queue(maxsize=0)
  num_threads = 4

  for i in range(num_threads):
    worker = Thread(target=_load, args=(q,))
    worker.setDaemon(True)
    worker.start()

  for table in tables:
    q.put((table, function, timeslice))

  q.join()


timeslice = Timeslice(2011, 1, 1)
with open(
    f"./config/project/{project}/{project}_tables.yml", "r", encoding="utf-8"
) as f:
    metdata = yaml.safe_load(f)

tables: list = [t["table"] for t in metdata.get("tables")]
load(tables, landing_to_raw, timeslice)


# COMMAND ----------

# from queue import Queue
# from threading import Thread

# def load(q):
#   while True:
#     table = q.get()
#     print(f"Loading {project}.{table}")
#     timeslice = Timeslice(2011, 1, 1)
#     results = landing_to_raw(timeslice=timeslice, table=table)
#     if results["error"].get("count", 0) > 0:
#         print(results)
#     else:
#         print(f"Loaded {project}.{table}")
#     q.task_done()

# q = Queue(maxsize=0)
# num_threads = 4

# for i in range(num_threads):
#   worker = Thread(target=load, args=(q,))
#   worker.setDaemon(True)
#   worker.start()


# with open(
#     f"./config/project/{project}/{project}_tables.yml", "r", encoding="utf-8"
# ) as f:
#     metdata = yaml.safe_load(f)

# tables: list = [t["table"] for t in metdata.get("tables")]
# failed = []

# for table in tables:
#   q.put(table)

# q.join()

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

# COMMAND ----------

dbutils.fs.rm("file:/yetl/", True)

# COMMAND ----------

dbutils.fs.ls("file:/yetl/runs")
