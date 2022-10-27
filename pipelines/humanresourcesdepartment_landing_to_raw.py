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


@yetl_flow(log_level="ERROR", project="adworks")
def humanresourcesdepartment_landing_to_raw(
    table: str,
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load raw delta tables"""

    df = dataflow.source_df(f"adworks_landing.{table}")

    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    # print(str(df.schema))
    # print("************************************************************************")
    # print(create_table_dll(df.schema, partition_fields=['_partition_key'], always_identity_column="key"))

    dataflow.destination_df(f"adworks_raw.{table}", df, save=save)


# incremental load
timeslice = Timeslice(2011, 1, 1)
results = humanresourcesdepartment_landing_to_raw(
    timeslice=timeslice, table="humanresourcesdepartment"
)
# print(json.dumps(results, indent=4, default=str))

# reload load
# timeslice = Timeslice(2022, "*", "*")
# results = humanresourcesdepartment_landing_to_raw(timeslice=timeslice, save=OverwriteSave)
# results = json.dumps(results, indent=4, default=str)
# print(results)
