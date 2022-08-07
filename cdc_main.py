
from yetl_flow import yetl_flow, IDataflow, Context, Timeslice, TimesliceUtcNow, OverwriteSave, Save
from yetl_flow.dataset import Destination
from pyspark.sql import functions as fn
from pyspark.sql.window import Window
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
          .drop("_corrupt_record")
          .withColumn("from_date", fn.expr("cast(extract_date as date)"))
          .withColumn("to_date", fn.expr("to_date('9999-12-31', 'yyyy-MM-dd')"))
          .withColumn("version", fn.lit(1))
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
    
    ################  TYPE 4 TRANSFORMATION  -  CREATE CHANGE SET   #####################################################

    # get all the incoming, matching current and historical recrods
    # into a single dataframe
    current_df = (
        context.spark.sql(f"""
            SELECT *
            FROM {dst.database}.{dst.table}
        """).alias("dst")
        .join(df, "id", "inner")
        .withColumn("data_name", fn.lit("dst_current"))
        .where("dst.extract_date != src.extract_date")
        .select(
            "dst.*", "data_name"
        )
    )

    historical_df = (
        context.spark.sql(f"""
            SELECT * 
            FROM {dst.database}.{dst.table}_history
        """).alias("dst")
        .join(df, "id", "inner")
        .withColumn("data_name", fn.lit("dst_history"))
        .where("dst.extract_date != src.extract_date")
        .select("dst.*", "data_name")
    )
    
    df_existing_destination = current_df.unionAll(historical_df)
    df = df.select(*df_existing_destination.columns)
    df_change_set = df.unionAll(df_existing_destination)

    ################  TYPE 4 TRANSFORMATION  -  ORDER THE CHANGESET   #####################################################
    # now we have a change set and can figure out chronological order, set the to_date, from_date, active.

    window_version  = Window.partitionBy("id").orderBy(fn.col("extract_date").desc())
    df_change_set = (df_change_set
        .withColumn("version", fn.row_number().over(window_version))
        .withColumn("active", fn.expr("version == 1 AND load_flag <> 'D'"))
    )

    df_next = df_change_set.selectExpr(
        "id as next_id",
        "(version - 1) as next_version", 
        "date_add(from_date, -1) as `next_from_date`"
    )
    df_change_set = (
        df_change_set
        .join(df_next,  (df_change_set["version"] == df_next["next_version"]) & (df_change_set["id"] == df_next["next_id"]) , "left")
        .withColumn("to_date", fn.expr("if(next_from_date is null, to_date('9999-12-31', 'yyyy-MM-dd'), next_from_date)"))
        .drop("next_id","next_version", "next_from_date", "data_name")    
    )

    context.log.info("Change set")
    df_change_set.show(truncate=False)
    df_change_set.persist()


    #####################  INSERT DATA INTO CURRENT TABLE  #################################

    # set the change tracking columns of the change set.
    result = (
        dst_table.alias("dst")
        .merge(
            df_change_set.alias("src").where("version = 1"),
            "dst.id = src.id"
        )
        .whenNotMatchedInsertAll("src.load_flag in ('I','U')")
        .whenMatchedUpdateAll("src.load_flag in ('I','U') and src.extract_date != dst.extract_date")
        .whenMatchedDelete("src.load_flag = 'D'")
        .execute()
    )
    df_current_result = context.spark.sql("select * from raw.cdc_customer")
    context.log.info("Current")
    df_current_result.show(truncate=False)
    

    #####################  INSERT DATA INTO HISTORY TABLE  #################################

    result = (
        dst_table_history.alias("dst")
        .merge(
            df_change_set.alias("src").where("not src.active"),
            "dst.id = src.id and dst.extract_date = src.extract_date"
        )
        .whenNotMatchedInsertAll()
        .whenMatchedUpdate(
            condition= """
                   dst.from_date != src.from_date
                OR dst.to_date != src.to_date
                OR dst.version != src.version
                OR dst.active != src.active
            """,
            set = {
                "dst.from_date": "src.from_date",
                "dst.to_date": "src.to_date",
                "dst.version": "src.version",
                "dst.active": "src.active"
            }
        )
        .execute()
    )
    df_history_result = context.spark.sql("select * from raw.cdc_customer_history")
    context.log.info("History")
    df_history_result.show(truncate=False)



# incremental load
results = cdc_customer_landing_to_rawdb_csv(
    timeslice = Timeslice(2022, 8, 1)
)



