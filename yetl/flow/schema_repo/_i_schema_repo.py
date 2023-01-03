from pyspark.sql.types import StructType
from typing import Union, Any
from abc import ABC, abstractmethod
from pydantic import BaseModel, PrivateAttr


class ISchemaRepo(BaseModel, ABC):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    _logger: Any = PrivateAttr(default=None)

    @abstractmethod
    def save_schema(
        self,
        schema: Union[StructType, str],
        database: str,
        table: str,
        sub_location: str = None,
    ):
        """Save a schema into the repo."""
        pass

    @abstractmethod
    def load_schema(self, database: str, table: str, sub_location: str = None):
        """Loads a spark from a file."""
        pass
