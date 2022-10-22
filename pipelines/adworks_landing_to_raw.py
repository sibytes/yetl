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
from yetl import async_load

@yetl_flow(log_level="ERROR", name="landing_to_raw")
def landing_to_raw(
    table: str,
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load raw delta tables"""

    df = dataflow.source_df(f"landing.{table}")

    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )
    dataflow.destination_df(f"adworks_raw.{table}", df, save=save)


results = async_load(tables="./adworks/adworks_tables.yml", parallelism=4)
print(json.dumps(results, indent=4, default=str))


















def load():

    with open("./adworks/adworks_tables.yml", "r", encoding="utf-8") as f:
        metdata = yaml.safe_load(f)

    tables: list = [t["table"] for t in metdata.get("tables")]
    failed = []

    for table in tables:

        timeslice = Timeslice(2011, 1, 1)
        results = landing_to_raw(timeslice=timeslice, table=table)
        if results["error"].get("count", 0) > 0:
            failed.append(results)
        else:
            print(f"Loaded adworks.{table}")

    print(json.dumps(failed, indent=4, default=str))