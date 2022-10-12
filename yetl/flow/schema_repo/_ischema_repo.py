from pyspark.sql.types import StructType


class ISchemaRepo:
    def __init__(self, context, config: dict) -> None:
        self.context = context

    def save_schema(self, schema: StructType, database_name: str, table_name: str):
        """Save a schema into the repo."""
        pass

    def load_schema(
        self, database_name: str, table_name: str, sub_location: str = None
    ):
        """Loads a spark from a file."""
        pass
