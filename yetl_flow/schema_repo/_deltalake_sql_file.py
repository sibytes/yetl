import json
import yaml
from ._ischema_repo import ISchemaRepo
from pyspark.sql.types import StructType
from ..file_system import FileFormat, IFileSystem


class DeltalakeSchemaFile(ISchemaRepo):

    _SCHEMA_ROOT = "./config/schema/deltalake"
    _EXT = "sql"

    def __init__(self, context, config: dict) -> None:
        super().__init__(context, config)
        self.root_path = config["deltalake_sql_file"].get("deltalake_schema_root")

    def _mkpath(self, database_name: str, table_name: str):
        """Function that builds the schema path"""
        if not self.root_path:
            return f"{self._SCHEMA_ROOT}/{database_name}/{table_name}.{self._EXT}"
        else:
            return f"{self.root_path}/{database_name}/{table_name}.{self._EXT}"

    def save_schema(self, schema: StructType, schema_name: str):
        """Serialise delta table to a create table sql file."""
        raise NotImplementedError

    def load_schema(self, database_name: str, table_name: str):
        """Loads a spark from a yaml file and deserialises to a spark schema."""

        path = self._mkpath(database_name, table_name)

        fs: IFileSystem = self.context.fs
        schema = fs.read_file(path, FileFormat.TEXT)

        return schema
