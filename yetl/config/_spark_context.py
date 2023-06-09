import json
import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from ._utils import is_databricks

_logger = logging.getLogger(__name__)


def get_spark_context(project: str, config: dict = None):
    if is_databricks():
        _logger.debug("Getting databricks spark context")
        try:
            from databricks.sdk.runtime import spark

            return spark
        except Exception:
            _logger.info("cannot create spark context, spark not found.")
            return None

    else:
        _logger.debug("Getting local spark context")

        if config is None:
            config = {
                "spark.master": "local",
                "spark.databricks.delta.allowArbitraryProperties.enabled": True,
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            }

        msg = json.dumps(config, indent=4, default=str)
        _logger.debug(msg)

        builder = SparkSession.builder

        for k, v in config.items():
            builder = builder.config(k, v)

        builder.appName(project)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark
