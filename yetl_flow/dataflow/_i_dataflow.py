from ..dataset import Dataset
from typing import Type
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
import json
from ..audit import Audit
import uuid


class IDataflow(ABC):
    def __init__(
        self, context, config: dict, dataflow_config: dict, auditor: Audit
    ) -> None:

        self.auditor = auditor
        self.id = uuid.uuid4()
        auditor.dataflow({"dataflow_id": str(self.id)})
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

    @abstractmethod
    def append(self, dataset: Dataset) -> None:
        pass

    @abstractmethod
    def source_df(self, database_table: str) -> DataFrame:
        pass

    @abstractmethod
    def destination_df(self, database_table: str, dataframe: DataFrame):
        pass
