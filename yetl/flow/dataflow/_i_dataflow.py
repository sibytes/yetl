from ..dataset import Dataset
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
import uuid
from pydantic import BaseModel, Field, PrivateAttr
from typing import Any
from ..audit import Audit
from ..context import IContext


class IDataflow(BaseModel, ABC):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.auditor = self.context.auditor
        self.dataflow_id = uuid.uuid4()

    auditor: Audit = Field(default=None)
    context: IContext = Field(...)
    dataflow_id: uuid.UUID = Field(default=uuid.uuid4())
    sources: dict = Field(default={})
    destinations: dict = Field(default={})
    _logger: Any = PrivateAttr(default=None)

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
