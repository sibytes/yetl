from yetl.flow import (
    yetl_flow,
    IDataflow,
    IContext,
    Timeslice,
    TimesliceUtcNow,
    Save,
    OverwriteSave,
)
from pyspark.sql.functions import *
from typing import Type
import json


@yetl_flow(project="demo")
def batch_sql_to_delta(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load the demo customer data from Raw delta table using SQL to a DW dimension delta table"""

    df_dim_cust = dataflow.source_df(f"{context.project}_raw.customer")
    # context.log.info(f"Loading {context.project}_dw.dim_customer type 2 dimension")
    dataflow.destination_df(
        f"{context.project}_dw.dim_customer", df_dim_cust, save=save
    )


# incremental load
results = batch_sql_to_delta(save=OverwriteSave)
results = json.dumps(results, indent=4, default=str)
print(results)
