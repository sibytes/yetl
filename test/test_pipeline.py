from sys import excepthook
from yetl_flow import yetl_flow, IDataflow, Reader, Writer, Context
from pyspark.sql.functions import *
import logging
from datetime import datetime

@yetl_flow(log_level="ERROR", auto_read=True)
def customer_landing_to_rawdb_csv(
    context: Context, dataflow: IDataflow, timeslice: datetime, timeslice_mask: str
) -> dict:
    """Load the demo customer data as is into a raw delta hive registered table.

    this is a test pipeline that can be run just to check everything is setup and configured
    correctly.
    """
    context.log.info(
        f"Executing Dataflow {context.app_name} with timeslice={timeslice}, retries={dataflow.retries}"
    )

    # the config for this dataflow has 2 landing sources that are joined
    # and written to delta table
    # delta tables are automatically created and if configured schema exceptions
    # are loaded syphened into a schema exception table
    df_cust = dataflow.source_df("landing.customer")
    df_prefs = dataflow.source_df("landing.customer_preferences")

    dst: Writer = dataflow.destinations["raw.customer"]

    # df = df_cust.join(df_prefs, "id", "inner")
    df = df_cust

    dataflow.log.info(f"Writing data to {dst.path}")
    (df.write.format(dst.format).options(**dst.options).mode(dst.mode).save(dst.path))



def test_integration():
    """Bare bones integration test until the project refactoring settles down
    
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.info(f"Executing pipeline test_customer_landing_to_rawdb")

    try:
        timeslice = datetime(2022, 1, 1, 0, 0, 0, 0)
        timeslice_mask = "*/*/*"
        results = customer_landing_to_rawdb_csv(
            timeslice=timeslice, timeslice_mask=timeslice_mask
        )
        # results = test_pipeline.test_customer_landing_to_rawdb_csv()
        assert True
    except Exception as e:
        raise