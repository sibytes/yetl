from pyspark.sql import SparkSession
import json
from delta import configure_spark_with_delta_pip
from ._i_context import IContext
from typing import Any
from pydantic import Field
from ..schema_repo import schema_repo_factory, ISchemaRepo
from ..file_system import file_system_factory, IFileSystem, FileSystemType
import logging


class SparkContext(IContext):

    spark_version: str = Field(default=None)

    spark_schema_repo_config: dict = Field(alias="spark_schema_repo")
    spark_schema_repository: ISchemaRepo = Field(default=None)

    deltalake_schema_repo_config: dict = Field(alias="deltalake_schema_repo")
    deltalake_schema_repository: ISchemaRepo = Field(default=None)
    engine: dict = Field(...)
    spark: SparkSession = None
    spark_logger: Any = None

    def __init__(self, **data: Any) -> None:

        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        # this is the datalake file system which is where the data is held
        # this is so we can perform commands directly on the datalake store.
        self.datalake_protocol = FileSystemType.FILE
        self.datalake_fs: IFileSystem = file_system_factory.get_file_system_type(
            self.datalake_protocol
        )

        self.engine = self.engine.get(self.context_type.value, {})
        self.spark = self._get_spark_context(self.project, self.engine)

        # set up the spark logger, the application has a python logger built in
        # but we also make the spark logger available should it be needed
        # because the spark logger is extremely verbose it's useful to the able
        # to set the level and use python native logging.
        self.spark_logger = self._get_spark_logger(
            self.spark, self.project, self.engine
        )

        self.spark_version = self._get_spark_version(self.spark)
        self._logger.debug(f"Spark version detected as : {self.spark_version}")

        # abstraction of the spark schema repo
        self.spark_schema_repository: ISchemaRepo = (
            schema_repo_factory.get_schema_repo_type(self.spark_schema_repo_config)
        )

        # abstraction of the deltalake schema repo
        self.deltalake_schema_repository: ISchemaRepo = (
            schema_repo_factory.get_schema_repo_type(self.deltalake_schema_repo_config)
        )

    def _get_spark_version(self, spark: SparkSession):

        version: str = spark.sql("select version() as version").collect()[0]["version"]
        return version

    def _get_spark_context(self, project: str, config: dict):
        self._logger.debug("Setting spark context")
        spark_config = config.get("config", {})

        msg = json.dumps(spark_config, indent=4, default=str)
        self._logger.debug(msg)

        builder = SparkSession.builder

        for k, v in spark_config.items():
            builder = builder.config(k, v)

        builder.appName(project)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        return spark

    def _get_spark_logger(self, spark: SparkSession, project: str, config: dict):

        log_level = config.get("logging_level", "ERROR")
        self._logger.debug(
            f"Setting application context spark logger at level {log_level}"
        )
        sc = spark.sparkContext
        sc.setLogLevel(log_level)
        log4j_logger = sc._jvm.org.apache.log4j
        logger = log4j_logger.LogManager.getLogger(project)

        return logger

    class Config:
        arbitrary_types_allowed = True
