from ..dataset import Dataset
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
import json
import uuid
from pydantic import BaseModel, Field, PrivateAttr
from typing import Any
from ..audit import Audit
from ..context import IContext


class IDataflow(BaseModel, ABC):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.auditor = self.context.auditor
        self.id = uuid.uuid4()
        self.log = self.context.log
        # needs validation handling.
        self._spark_schema_repo = self.context.config["spark_schema_repo"]
        self._deltalake_schema_repo = self.context.config["deltalake_schema_repo"]
        self._pipeline_repo = self.context.config["pipeline_repo"]
        self.datalake_protocol = self.context.fs.datalake_protocol
        self.datalake = self.context.config["datalake"]

    audit: Audit = Field(default=None)
    dataflow_id: uuid.UUID = Field(default=uuid.uuid4())
    datalake: str = Field(default=None)
    sources: dict = Field(default={})
    destinations: dict = Field(default={})
    datalake_protocol: str = Field(default=None)
    _spark_schema_repo: dict = PrivateAttr(default=None)
    _deltalake_schema_repo: dict = PrivateAttr(default=None)
    _pipeline_repo: dict = PrivateAttr(default=None)
    context: IContext = Field(...)
    log: Any

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
