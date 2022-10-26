from pyspark.sql.types import StructType
from typing import Union


class ISchemaRepo:
    def __init__(self, context, config: dict) -> None:
        self.context = context

    def save_schema(
        self,
        schema: Union[StructType, str],
        database_name: str,
        table_name: str,
        sub_location: str = None,
    ):
        """Save a schema into the repo."""
        pass

    def load_schema(
        self, database_name: str, table_name: str, sub_location: str = None
    ):
        """Loads a spark from a file."""
        pass
