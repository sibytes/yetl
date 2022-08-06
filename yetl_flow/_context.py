from .dataflow import Dataflow
from . import _config_provider as cp
from pyspark.sql import SparkSession
from datetime import datetime
from .file_system import file_system_factory, IFileSystem
import logging
import uuid
from .schema_repo import schema_repo_factory
import json
from ._timeslice import Timeslice, TimesliceUtcNow
from .dataset import Save, DefaultSave
from typing import Type
from delta import configure_spark_with_delta_pip

class Context:
    def __init__(
        self,
        app_name: str,
        log_level: str,
        name: str,
        spark: SparkSession = None,
        timeslice: datetime = None,
        save_type: Type[Save] = DefaultSave,
    ) -> None:
        self.correlation_id = uuid.uuid4()
        self.name = name
        self.app_name = app_name

        if not app_name:
            self.app_name = self.name

        self.timeslice: Timeslice = timeslice
        if not self.timeslice:
            self.timeslice = TimesliceUtcNow()

        self.log = logging.getLogger(self.app_name)
        self.log_level = log_level
        self.spark = spark

        # load application configuration
        config: dict = cp.load_config(self.app_name)

        if not spark:
            self.spark = self._get_spark_context(app_name, config)

        # set up the spark logger, the application has a python logger built in
        # but we also make the spark logger available should it be needed
        # because the spark logger is extremely verbose it's useful to the able
        # to set the level and use python native logging.
        self.log.info(f"Setting application context spark logger at level {log_level}")
        self.spark_logger = self._get_spark_logger(
            self.spark, self.app_name, self.log_level
        )

        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        self.fs: IFileSystem = file_system_factory.get_file_system_type(
            self, config=config
        )

        # abstraction of the schema repo
        self.schema_repo_factory = schema_repo_factory

        # Load and deserialise the spark dataflow configuration in to metaclasses (see dataset module)
        # The configuration file is loaded using the app name. This keeps intuitive tight
        # naming convention between datadlows and the config files that store them
        self.log.info(f"Setting application context dataflow {self.name}")
        self.dataflow = self._get_deltalake_flow(
            self.app_name, self.name, config, save_type
        )

    def _get_deltalake_flow(
        self, app_name: str, name: str, config: dict, save_type: Type[Save]
    ):

        dataflow_config: dict = cp.load_pipeline_config(app_name, name)
        dataflow_config = dataflow_config.get("dataflow")

        self.log.debug("Deserializing configuration into Dataflow")

        dataflow = Dataflow(self, config, dataflow_config, save_type)

        return dataflow

    def _get_spark_context(self, app_name: str, config: dict):
        self.log.info("Setting spark context")
        spark_config = config["spark"]

        msg = json.dumps(spark_config, indent=4, default=str)
        self.log.debug(msg)

        builder = SparkSession.builder

        for k, v in spark_config.items():
            builder = builder.config(k, v)

        builder.appName(app_name)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # spark = builder.appName(app_name).getOrCreate()

        return spark

    def _get_spark_logger(self, spark: SparkSession, app_name: str, log_level: str):
        sc = spark.sparkContext
        sc.setLogLevel(log_level)
        log4j_logger = sc._jvm.org.apache.log4j
        logger = log4j_logger.LogManager.getLogger(app_name)

        return logger
