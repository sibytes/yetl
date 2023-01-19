import json
import yaml
from ._i_schema_repo import ISchemaRepo
from pyspark.sql.types import StructType
from ..file_system import FileFormat, IFileSystem, file_system_factory, FileSystemType
import os
from ._exceptions import SchemaNotFound
from pydantic import Field
import logging
from typing import Any

_SCHEMA_ROOT = "./config/schema/spark"
_EXT = "yaml"


class SparkFileSchemaRepo(ISchemaRepo):

    root: str = Field(default=_SCHEMA_ROOT, alias="spark_schema_root")
    # TODO: mkae private after FS is pydantic
    fs: IFileSystem = None

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        self.fs: IFileSystem = file_system_factory.get_file_system_type(
            FileSystemType.FILE
        )

    def _mkpath(self, database: str, table: str, sub_location: str):
        """Function that builds the schema path"""
        if sub_location:
            return f"{self.root}/{sub_location}/{database}/{table}.{_EXT}"
        else:
            return f"{self.root}/{database}/{table}.{_EXT}"

    def save_schema(
        self,
        schema: StructType,
        database: str,
        table: str,
        sub_location: str = None,
    ):
        """Serialise a spark schema to a yaml file and saves to a schema file in the schema folder."""

        path = self._mkpath(database, table, sub_location)
        path = os.path.abspath(path)
        dir_path = os.path.dirname(path)
        os.makedirs(dir_path, exist_ok=True)

        schema_dict = json.loads(schema.json())
        with open(path, "w", encoding="utf-8") as f:
            f.write(yaml.safe_dump(schema_dict))

    def load_schema(self, database: str, table: str, sub_location: str = None):
        """Loads a spark from a yaml file and deserialises to a spark schema."""

        path = self._mkpath(database, table, sub_location)
        path = os.path.abspath(path)

        self._logger.debug(
            f"Loading schema for dataset {database}.{table} from {path} using {type(self.fs)}"
        )
        try:
            schema = self.fs.read_file(path, FileFormat.YAML)
        except Exception as e:
            raise SchemaNotFound(path) from e

        msg = json.dumps(schema, indent=4, default=str)
        self._logger.debug(msg)

        try:
            spark_schema = StructType.fromJson(schema)
        except Exception as e:
            msg = (
                msg
            ) = f"Failed to deserialise spark schema to StructType for dataset {database}.{table} from {path}"
            raise Exception(msg) from e

        return spark_schema

    class Config:
        arbitrary_types_allowed = True
