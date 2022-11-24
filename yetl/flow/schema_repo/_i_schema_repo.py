from pyspark.sql.types import StructType
from typing import Union, Any
from abc import ABC, abstractmethod
from pydantic import BaseModel


class ISchemaRepo(BaseModel, ABC):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    @abstractmethod
    def save_schema(
        self,
        schema: Union[StructType, str],
        database_name: str,
        table_name: str,
        sub_location: str = None,
    ):
        """Save a schema into the repo."""
        pass

    @abstractmethod
    def load_schema(
        self, database_name: str, table_name: str, sub_location: str = None
    ):
        """Loads a spark from a file."""
        pass
