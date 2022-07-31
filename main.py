from yetl_flow import yetl_flow, IDataflow, Context, Timeslice, TimesliceUtcNow
from pyspark.sql.functions import *
import logging

@yetl_flow(log_level="ERROR")
def customer_landing_to_rawdb_csv(
    context: Context, dataflow: IDataflow, timeslice: Timeslice=TimesliceUtcNow()
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table.

    this is a test pipeline that can be run just to check everything is setup and configured
    correctly.
    """
    context.log.info(f"Executing Dataflow {context.app_name} with timeslice={timeslice} retries={dataflow.retries}")

    # the config for this dataflow has 2 landing sources that are joined
    # and written to delta table
    # delta tables are automatically created and if configured schema exceptions
    # are loaded syphened into a schema exception table
    df_cust = dataflow.source_df("landing.customer")
    df_prefs = dataflow.source_df("landing.customer_preferences")

    df = df_cust.join(df_prefs, "id", "inner")
    df = df_cust

    dataflow.destination_df("raw.customer", df)



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info(f"Executing pipeline test_customer_landing_to_rawdb")

# if timeslice not provided will default to utcnow
timeslice = Timeslice(2022, 7, 11)
results = customer_landing_to_rawdb_csv(
    timeslice=timeslice
)
# results = pipeline.test_customer_landing_to_rawdb_csv()



