from yetl.flow import (
    yetl_flow,
    IDataflow,
    IContext,
    Timeslice,
    TimesliceUtcNow,
    OverwriteSave,
    Save,
)
from pyspark.sql.functions import *
from typing import Type
import json


@yetl_flow(project="demo")
def dim_customer_raw_to_dw(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:

    """Load the demo customer data as is into a raw delta hive registered table."""

    df = dataflow.source_df(f"{context.project}_dw.src_dim_customer")

    context.log.info("Loading customer preferences")
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )
    df.show()

    dataflow.destination_df(f"{context.project}_dw.dim_customer", df, save=save)


# incremental load
# timeslice = Timeslice(year=2021, month=1, day=1)
# timeslice = Timeslice(year=2021, month=1, day=2)
# timeslice = Timeslice(year=2021, month=1, day="*")
# results = customer_preferences_landing_to_raw(timeslice=timeslice)
# print(results)

# reload 2021
# timeslice = Timeslice(year=2021, month="*", day="*")
# results = customer_preferences_landing_to_raw(timeslice=timeslice, save=OverwriteSave)
# results = json.dumps(results, indent=4, default=str)
# print(results)


# reload all
timeslice = Timeslice(year="*", month="*", day="*")
results = dim_customer_raw_to_dw(timeslice=timeslice, save=OverwriteSave)
results = json.dumps(results, indent=4, default=str)
print(results)
