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
def customer_preferences_landing_to_raw(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table."""

    df = dataflow.source_df(f"{context.project}_landing.customer_preferences")

    # context.log.info("Loading customer preferences")
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )
    df.show()

    dataflow.destination_df(
        f"{context.project}_raw.customer_preferences", df, save=save
    )


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
results = customer_preferences_landing_to_raw(timeslice=timeslice, save=OverwriteSave)
results = json.dumps(results, indent=4, default=str)
print(results)
