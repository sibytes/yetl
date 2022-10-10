from yetl_flow import (
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


@yetl_flow(log_level="ERROR")
def batch_sql_to_delta(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load the demo customer data from Raw delta table using SQL to a DW dimension delta table"""

    df_dim_cust = dataflow.source_df("raw.customer")
    df_dim_cust.show()
    df_dim_cust = df_dim_cust.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    dataflow.destination_df("dw.dim_customer", df_dim_cust, save=save)


# incremental load
# timeslice = Timeslice(2021, 1, 1)
timeslice = Timeslice(2022, 7, 12)
# timeslice = Timeslice(2022, 7, "*")
results = batch_sql_to_delta(timeslice=timeslice)
print(results)

# reload load
# timeslice = Timeslice(2022, "*", "*")
# results = batch_text_csv_to_delta_permissive_1(timeslice=timeslice, save=OverwriteSave)
# results = json.dumps(results, indent=4, default=str)
# print(results)
