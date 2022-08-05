
from yetl_flow import yetl_flow, IDataflow, Context, Timeslice, TimesliceUtcNow, OverwriteSave, Save
from pyspark.sql.functions import *
from typing import Type

@yetl_flow(log_level="ERROR")
def cdc_customer_landing_to_rawdb_csv(
    context: Context, 
    dataflow: IDataflow, 
    timeslice: Timeslice = TimesliceUtcNow(), 
    save_type: Type[Save] = None
) -> dict:

    df_cust = dataflow.source_df("landing.cdc_customer")

    df = df_cust

    dataflow.destination_df("raw.cdc_customer", df)


# incremental load
results = cdc_customer_landing_to_rawdb_csv(
    timeslice = Timeslice(2022, 8, '*')
)



