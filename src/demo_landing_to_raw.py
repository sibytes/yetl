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
import yaml
from yetl.workflow import multithreaded as yetl_wf

project = "demo"
pipeline_name = "landing_to_raw"
timeslice = Timeslice(2022, 7, 11)
maxparallel = 2


@yetl_flow(project=project, pipeline_name=pipeline_name)
def landing_to_raw(
    table: str,
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load raw delta tables"""

    df = dataflow.source_df(f"{project}_landing.{table}")

    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )
    dataflow.destination_df(f"{project}_raw.{table}", df, save=save)


with open(
    f"./config/project/{project}/{project}_tables.yml", "r", encoding="utf-8"
) as f:
    metdata = yaml.safe_load(f)
tables: list = [t["table"] for t in metdata.get("tables")]

yetl_wf.load(project, tables, landing_to_raw, timeslice, maxparallel)
