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

@yetl_flow(log_level="ERROR", project="demo")
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

    context.project

    # the config for this dataflow has 2 landing sources that are joined
    # and written to delta table
    # delta tables are automatically created and if configured schema exceptions
    # are loaded syphened into a schema exception table
    df_cust = dataflow.source_df(f"{context.project}_landing.customer")
    df_prefs = dataflow.source_df(f"{context.project}_landing.customer_preferences")

    context.log.info("Joining customers with customer_preferences")
    df = df_cust.join(df_prefs, "id", "inner")
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    dataflow.destination_df(f"{context.project}_raw.customer", df, save=save)


# incremental load
# timeslice = Timeslice(2021, 1, 1)
# timeslice = Timeslice(2022, 7, 12)
# timeslice = Timeslice(2022, 7, "*")
# results = batch_text_csv_to_delta_permissive_1(timeslice=timeslice)
# print(results)

# reload load
timeslice = Timeslice(2022, "*", "*")
results = batch_text_csv_to_delta_permissive_1(timeslice=timeslice, save=OverwriteSave)
results = json.dumps(results, indent=4, default=str)
print(results)
