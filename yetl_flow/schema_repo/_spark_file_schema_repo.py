import json
import yaml
from ._ischema_repo import ISchemaRepo
from pyspark.sql.types import StructType
from ..file_system import FileFormat, IFileSystem


class SparkFileSchemaRepo(ISchemaRepo):

    _SCHEMA_ROOT = "./config/schema/spark"
    _EXT = "yaml"

    def __init__(self, context, config: dict) -> None:
        super().__init__(context, config)
        self.root_path = config["spark_schema_file"].get("spark_schema_root")

    def _mkpath(self, database_name: str, table_name: str):
        """Function that builds the schema path"""
        if not self.root_path:
            return f"{self._SCHEMA_ROOT}/{database_name}/{table_name}.{self._EXT}"
        else:
            return f"{self.root_path}/{database_name}/{table_name}.{self._EXT}"

    def save_schema(self, schema: StructType, schema_name: str):
        """Serialise a spark schema to a yaml file and saves to a schema file in the schema folder."""

        schema_dict = json.loads(schema.json())
        path = self._mkpath(schema_name)
        with open(path, "w") as f:
            f.write(yaml.safe_dump(schema_dict))

    def load_schema(self, database_name: str, table_name: str):
        """Loads a spark from a yaml file and deserialises to a spark schema."""

        path = self._mkpath(database_name, table_name)

        fs: IFileSystem = self.context.fs
        schema = fs.read_file(path, FileFormat.YAML)

        spark_schema = StructType.fromJson(schema)

        return spark_schema
