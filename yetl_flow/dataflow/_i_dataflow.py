from ..dataset import Dataset, Save, DefaultSave
from typing import Type
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
import json


class IDataflow(ABC):
    def __init__(
        self,
        context,
        config: dict,
        dataflow_config: dict,
        save_type: Type[Save] = DefaultSave,
    ) -> None:

        self.log = context.log
        self.log.debug("initialise dataflow with config")
        self.log.debug(json.dumps(config, indent=4, default=str))

        self.log.debug("initialise dataflow with dataflow_config")
        self.log.debug(json.dumps(dataflow_config, indent=4, default=str))

        self.datalake = config["datalake"]

        self._spark_schema_repo = config["spark_schema_repo"]
        self._deltalake_schema_repo = config["deltalake_schema_repo"]

        self.datalake_protocol = context.fs.datalake_protocol
        self.context = context
        self.sources = {}
        self.destinations = {}

        self.retries = dataflow_config.get("retries", 0)
        if dataflow_config.get("retries"):
            del dataflow_config["retries"]

        self.retry_wait = dataflow_config.get("retry_wait", 0)
        if dataflow_config.get("retry_wait"):
            del dataflow_config["retry_wait"]

        self.enabled_dataflow_types = dataflow_config.get("enable_dataflow_types", [])
        if dataflow_config.get("enable_dataflow_types"):
            del dataflow_config["enable_dataflow_types"]

    @abstractmethod
    def append(self, dataset: Dataset) -> None:
        pass

    @abstractmethod
    def source_df(self, database_table: str) -> DataFrame:
        pass

    @abstractmethod
    def destination_df(self, database_table: str, dataframe: DataFrame):
        pass
