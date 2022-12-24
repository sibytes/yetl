# from ..dataflow import Dataflow
from pyspark.sql import SparkSession
from ._spark_context import SparkContext
from typing import Any
from pydantic import Field, PrivateAttr
from ..file_system import file_system_factory, IFileSystem, FileSystemType
import logging


class DatabricksContext(SparkContext):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        # this is the datalake file system which is where the data is held
        # this is so we can perform commands directly on the datalake store.
        self.datalake_protocol = FileSystemType.DBFS
        self.datalake_fs: IFileSystem = file_system_factory.get_file_system_type(
            self.datalake_protocol
        )

        self.databricks_version = self._get_databricks_version(self.spark)

        self._logger.debug(
            f"Databricks Runtime version detected as : {self.databricks_version}"
        )

    databricks_version: dict = Field(default=None)
    default_catalog: str = Field(default=None)

    def _get_databricks_version(self, spark: SparkSession):

        databricks_version: dict = (
            spark.sql("select current_version() as version")
            .collect()[0]["version"]
            .asDict()
        )

        return databricks_version

    class Config:
        arbitrary_types_allowed = True
