# from ..dataflow import Dataflow
from pyspark.sql import SparkSession
import json
from delta import configure_spark_with_delta_pip
from ._i_context import IContext
from typing import Any
from pydantic import Field, BaseModel
from ..schema_repo import schema_repo_factory, ISchemaRepo


class SparkContext(IContext):
    def __init__(self, **data: Any) -> None:

        super().__init__(**data)
        self.spark = self._get_spark_context(self.project, self.spark_config)

        # set up the spark logger, the application has a python logger built in
        # but we also make the spark logger available should it be needed
        # because the spark logger is extremely verbose it's useful to the able
        # to set the level and use python native logging.
        self.spark_logger = self._get_spark_logger(
            self.spark, self.project, self.spark_config
        )

        self.spark_version, self.databricks_version = self._get_spark_version(
            self.spark
        )
        self.log.info(f"Spark version detected as : {self.spark_version}")

        if self.databricks_version:
            self.is_databricks = True
            self.log.info(
                f"Databricks Runtime version detected as : {self.databricks_version}"
            )

        # abstraction of the spark schema repo
        self.spark_schema_repository: ISchemaRepo = (
            schema_repo_factory.get_schema_repo_type(
                self, config=self.spark_schema_repo_config
            )
        )

        # abstraction of the deltalake schema repo
        self.deltalake_schema_repository: ISchemaRepo = (
            schema_repo_factory.get_schema_repo_type(
                self, config=self.deltalake_schema_repo_config
            )
        )

        # Load and deserialise the spark dataflow configuration in to metaclasses (see dataset module)
        # The configuration file is loaded using the app name. This keeps intuitive tight
        # naming convention between datadlows and the config files that store them
        self.log.info(f"Setting application context dataflow {self.name}")
        self.dataflow = self._get_deltalake_flow()

    spark_version: str = Field(default=None)
    databricks_version: dict = Field(default=None)
    is_databricks: bool = Field(default=False)

    spark_schema_repo_config: dict = Field(alias="spark_schema_repo")
    spark_schema_repository: ISchemaRepo = Field(default=None)

    deltalake_schema_repo_config: dict = Field(alias="deltalake_schema_repo")
    deltalake_schema_repository: ISchemaRepo = Field(default=None)
    spark_config: dict = Field(...)
    spark: SparkSession = None
    spark_logger: Any = None

    def _get_spark_version(self, spark: SparkSession):

        version: str = spark.sql("select version() as version").collect()[0]["version"]

        try:
            databricks_version: dict = (
                spark.sql("select current_version() as version")
                .collect()[0]["version"]
                .asDict()
            )
        except:
            databricks_version: dict = {}

        return version, databricks_version

    def _get_spark_context(self, project: str, config: dict):
        self.log.info("Setting spark context")
        spark_config = config.get("config", {})

        msg = json.dumps(spark_config, indent=4, default=str)
        self.log.debug(msg)

        builder = SparkSession.builder

        for k, v in spark_config.items():
            builder = builder.config(k, v)

        builder.appName(project)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        return spark

    def _get_spark_logger(self, spark: SparkSession, project: str, config: dict):

        log_level = config.get("logging_level", "ERROR")
        self.log.info(f"Setting application context spark logger at level {log_level}")
        sc = spark.sparkContext
        sc.setLogLevel(log_level)
        log4j_logger = sc._jvm.org.apache.log4j
        logger = log4j_logger.LogManager.getLogger(project)

        return logger

    def _get_deltalake_flow(self):
        # load the data pipeline provider
        dataflow_config: dict = self.environment.load_pipeline(
            self.project, self._pipeline_root, self.name
        )
        dataflow_config = dataflow_config.get("dataflow")

        self.log.debug("Deserializing configuration into Dataflow")

        # dataflow = Dataflow(self, dataflow_config)

        # return dataflow

    class Config:
        arbitrary_types_allowed = True
