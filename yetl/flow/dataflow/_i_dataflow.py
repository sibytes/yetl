from ..dataset import Dataset
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
import json
import uuid


class IDataflow(ABC):
    def __init__(self, context, dataflow_config: dict) -> None:

        self.auditor = context.auditor
        self.id = uuid.uuid4()
        self.log = context.log

        self.log.debug("initialise dataflow with config")
        self.log.debug(json.dumps(context.config, indent=4, default=str))

        self.log.debug("initialise dataflow with dataflow_config")
        self.log.debug(json.dumps(dataflow_config, indent=4, default=str))

        self.datalake = context.config["datalake"]

        self._spark_schema_repo = context.config["spark_schema_repo"]
        self._deltalake_schema_repo = context.config["deltalake_schema_repo"]

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

    def _audit(self):
        self.auditor.dataflow({"dataflow_id": str(self.id)})
