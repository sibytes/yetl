
from yetl_flow import yetl_flow, IDataflow, Context, Timeslice, TimesliceUtcNow, OverwriteSave, Save
from yetl_flow.dataset import Destination
from pyspark.sql import functions as fn
from typing import Type
from delta.tables import DeltaTable



@yetl_flow(log_level="ERROR")
def cdc_customer_landing_to_rawdb_csv(
    context: Context, 
    dataflow: IDataflow, 
    timeslice: Timeslice = TimesliceUtcNow(), 
    save_type: Type[Save] = None
) -> dict:

    # get the source feed.
    df = dataflow.source_df("landing.cdc_customer")
    df = (df.alias("src")
          .withColumn("from_date", fn.expr("cast(extract_date as date)"))
          .withColumn("to_date", fn.expr("cast(extract_date as date)"))
          .withColumn("active", fn.lit(True))
          .withColumn("data_name", fn.lit("src"))
    )

    # get the current destination
    dst_name = "raw.cdc_customer"
    dst:Destination = dataflow.destinations[dst_name]
    dst_table = DeltaTable.forPath(context.spark, dst.path)
    context.log.info(f"Fetched dst delta table {dst_name} from {dst.path}")


    # get the history destination
    dst_history_name = "raw.cdc_customer_history"
    dst_history:Destination = dataflow.destinations[dst_history_name]
    dst_table_history = DeltaTable.forPath(context.spark, dst_history.path)
    context.log.info(f"Fetched dst delta table {dst_history_name} from {dst_history.path}")


    # get all the incoming, matching current and historical recrods
    # into a single dataframe
    current_df = (
        context.spark.sql(f"""
            SELECT *
            FROM {dst.database}.{dst.table}
        """).alias("dst_c")
        .join(df, "id", "inner")
        .withColumn("data_name", fn.lit("dst_c"))
        .select(
            "dst_c.*", "data_name"
        )
    )
    current_df.show()

    historical_df = (
        context.spark.sql(f"""
            SELECT * 
            FROM {dst.database}.{dst.table}_history
        """).alias("dst_h")
        .join(df, "id", "inner")
        .withColumn("data_name", fn.lit("dst_c"))
        .select("dst_h.*", "data_name")
    )
    # # union current with history
    historical_df.show()

    df_existing_destination = current_df.unionAll(historical_df)

    df_existing_destination.show()
    # df_change_set = df.unionAll(df_existing_destination)
    # # set the change tracking columns of the change set.
    # df_change_set.show()
    context.log.info("stop")
    

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



