from ..dataflow import Dataflow
from .. import _config_provider as cp
from pyspark.sql import SparkSession
from datetime import datetime
import json
from ..audit import Audit
from delta import configure_spark_with_delta_pip
from ._icontext import IContext


class SparkContext(IContext):
    def __init__(
        self,
        app_name: str,
        log_level: str,
        name: str,
        auditor: Audit,
        timeslice: datetime = None,
    ) -> None:

        super().__init__(app_name, log_level, name, auditor, timeslice)

        self.spark = self._get_spark_context(app_name, self.config)

        # set up the spark logger, the application has a python logger built in
        # but we also make the spark logger available should it be needed
        # because the spark logger is extremely verbose it's useful to the able
        # to set the level and use python native logging.
        self.log.info(f"Setting application context spark logger at level {log_level}")
        self.spark_logger = self._get_spark_logger(
            self.spark, self.app_name, self.log_level
        )

        self.log.info(f"Checking spark and databricks versions")
        self.spark_version, self.databricks_version = self._get_spark_version(
            self.spark
        )

        if self.databricks_version:
            self.is_databricks = True
            self.log.info(
                f"Databricks Runtime version detected as : {self.databricks_version}"
            )
        else:
            self.is_databricks = False
            self.log.info(f"Databricks Runtime version not detected.")

        self.log.info(f"Spark version detected as : {self.spark_version}")

        # Load and deserialise the spark dataflow configuration in to metaclasses (see dataset module)
        # The configuration file is loaded using the app name. This keeps intuitive tight
        # naming convention between datadlows and the config files that store them
        self.log.info(f"Setting application context dataflow {self.name}")
        self.dataflow = self._get_deltalake_flow()

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

    def _get_deltalake_flow(self):
        # load the data pipeline provider
        dataflow_config: dict = cp.load_pipeline_config(self.app_name, self.name)
        dataflow_config = dataflow_config.get("dataflow")

        self.log.debug("Deserializing configuration into Dataflow")

        dataflow = Dataflow(self, dataflow_config)

        return dataflow