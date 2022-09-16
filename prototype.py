from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame


spark_config = {
    "spark.master": "local",
    # yetl uses table properties so this must be set as a table
    # property or globally like here in the spark context
    # spark.databricks.delta.allowArbitraryProperties.enabled: true
    "spark.jars.packages": "io.delta:delta-core_2.12:2.0.0",
    "park.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


builder = SparkSession.builder

for k, v in spark_config.items():
    builder = builder.config(k, v)

builder.appName("test")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "/Users/shaunryan/AzureDevOps/yetl/data/delta_lake/raw/customer"

tbl = DeltaTable.forPath(spark, path)

tbl.details()

df: DataFrame = tbl.toDF()

(
    tbl.alias('dst').merge(
        df.alias('src'),
        'people.id = updates.id'
    ) 
    .whenMatchedUpdateAll()
    .whenMatchedDelete()
    .whenNotMatchedInsertAll()
    .execute()
)


