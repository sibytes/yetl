from pydantic import BaseModel, Field
from enum import Enum
from typing import Union, Dict, Any
import os


class SparkLoggingLevel(Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARNING = "WARNING"
    ERROR = "ERROR"


class SparkConfig(BaseModel):
    logging_level: SparkLoggingLevel = Field(default=SparkLoggingLevel.INFO)
    config: Dict[str, Union[str, bool]] = Field(
        default={
            "spark.master": "local",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.merge.repartitionBeforeWrite.enabled": True,
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
        }
    )


class Project(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.sql = os.path.join(self.config_path, self.sql.replace("./", ""))
        self.pipelines = os.path.join(
            self.config_path, self.pipelines.replace("./", "")
        )
        self.databricks_notebooks = os.path.join(
            self.config_path, self.databricks_notebooks.replace("./", "")
        )
        self.databricks_workflows = os.path.join(
            self.config_path, self.databricks_workflows.replace("./", "")
        )
        self.databricks_queries = os.path.join(
            self.config_path, self.databricks_queries.replace("./", "")
        )

    config_path: str = Field(...)
    name: str = Field(...)
    sql: str = Field(default="./sql")
    pipelines: str = Field(default="./pipelines")
    databricks_notebooks: str = Field(default="./databricks/notebooks")
    databricks_workflows: str = Field(default="./databricks/workflows")
    databricks_queries: str = Field(default="./databricks/queries")

    spark: SparkConfig = Field(default=SparkConfig())
