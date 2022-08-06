
from yetl_flow import yetl_flow, IDataflow, Context, Timeslice, TimesliceUtcNow, OverwriteSave, Save
from yetl_flow.dataset import Destination
from pyspark.sql.functions import *
from typing import Type
from delta.tables import DeltaTable



@yetl_flow(log_level="ERROR")
def cdc_customer_landing_to_rawdb_csv(
    context: Context, 
    dataflow: IDataflow, 
    timeslice: Timeslice = TimesliceUtcNow(), 
    save_type: Type[Save] = None
) -> dict:

    df = dataflow.source_df("landing.cdc_customer")

    dst_name = "raw.cdc_customer"
    dst:Destination = dataflow.destinations[dst_name]

    context.log.info(f"Fetching dst delta table {dst_name} from {dst.path}")
    dst_table = DeltaTable.forPath(context.spark, dst.path)
    context.log.info(f"Fetched dst delta table {dst_name} from {dst.path}")

    # result =(
    #     dst_table.alias('people') \
    #     .merge(
    #         dfUpdates.alias('updates'),
    #         'people.id = updates.id'
    #     ) \
    #     .whenMatchedUpdate(set =
    #         {
    #         "id": "updates.id",
    #         "firstName": "updates.firstName",
    #         "middleName": "updates.middleName",
    #         "lastName": "updates.lastName",
    #         "gender": "updates.gender",
    #         "birthDate": "updates.birthDate",
    #         "ssn": "updates.ssn",
    #         "salary": "updates.salary"
    #         }
    #     ) \
    #     .whenNotMatchedInsert(values =
    #         {
    #         "id": "updates.id",
    #         "firstName": "updates.firstName",
    #         "middleName": "updates.middleName",
    #         "lastName": "updates.lastName",
    #         "gender": "updates.gender",
    #         "birthDate": "updates.birthDate",
    #         "ssn": "updates.ssn",
    #         "salary": "updates.salary"
    #         }
    #     ) \
    #     .execute()
    # )



# incremental load
results = cdc_customer_landing_to_rawdb_csv(
    timeslice = Timeslice(2022, 8, '*')
)



