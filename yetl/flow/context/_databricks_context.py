# from ..dataflow import Dataflow
from pyspark.sql import SparkSession
from ._spark_context import SparkContext
from typing import Any
from pydantic import Field

class DatabricksContext(SparkContext):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

        self.databricks_version = self._get_databricks_version(
            self.spark
        )

        self.log.info(
            f"Databricks Runtime version detected as : {self.databricks_version}"
        )

    databricks_version: dict = Field(default=None)

    def _get_databricks_version(self, spark: SparkSession):

        databricks_version: dict = (
            spark.sql("select current_version() as version")
            .collect()[0]["version"]
            .asDict()
        )

        return databricks_version


    class Config:
        arbitrary_types_allowed = True
