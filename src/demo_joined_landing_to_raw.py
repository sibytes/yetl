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
def demo_joined_landing_to_raw(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table.

    The config for this dataflow has 2 landing sources that are joined
    and written to delta table
    """

    df_cust = dataflow.source_df(f"{context.project}_landing.customer_details")
    df_prefs = dataflow.source_df(f"{context.project}_landing.customer_preferences")

    context.log.info("Joining customers with customer_preferences")
    df = df_cust.join(df_prefs, "id", "inner")
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    dataflow.destination_df(f"{context.project}_raw.customer", df, save=save)


# incremental load
# timeslice = Timeslice(2021, 1, 1)
# timeslice = Timeslice(2021, 1, 2)
# timeslice = Timeslice(2021, 1, "*")
# results = demo_joined_landing_to_raw(timeslice=timeslice)
# print(results)

# reload 2021
# timeslice = Timeslice(2021, "*", "*")
# results = customer_preferences_landing_to_raw(timeslice=timeslice, save=OverwriteSave)
# results = json.dumps(demo_joined_landing_to_raw, indent=4, default=str)
# print(results)


# reload all
timeslice = Timeslice("*", "*", "*")
results = demo_joined_landing_to_raw(timeslice=timeslice, save=OverwriteSave)
results = json.dumps(results, indent=4, default=str)
print(results)
