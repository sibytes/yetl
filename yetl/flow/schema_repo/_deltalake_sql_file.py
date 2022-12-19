import json
from ._i_schema_repo import ISchemaRepo
from ..file_system import FileFormat, IFileSystem, file_system_factory, FileSystemType
import os
from ..parser.parser import render_jinja, JinjaVariables
from ._exceptions import SchemaNotFound
from typing import Any
import logging
from pydantic import Field

_SCHEMA_ROOT = "./config/schema"
_EXT = "sql"


class DeltalakeSchemaFile(ISchemaRepo):

    root: str = Field(default=_SCHEMA_ROOT, alias="deltalake_schema_root")
    # log: logging.Logger = None
    # TODO: make private after FS is pydantic
    fs: IFileSystem = None

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        self.fs: IFileSystem = file_system_factory.get_file_system_type(
            FileSystemType.FILE
        )

    def _mkpath(self, database_name: str, table_name: str, sub_location: str = None):
        """Function that builds the schema path"""

        replacements = {JinjaVariables.ROOT: self.root}
        if sub_location:
            path = render_jinja(sub_location, replacements)
        else:
            path = self.root

        path = f"{path}/{database_name}/{table_name}.{_EXT}"
        return path

    def save_schema(
        self, schema: str, database: str, table: str, sub_location: str = None
    ):
        """Serialise delta table to a create table sql file."""
        path = self._mkpath(database, table, sub_location)
        path = os.path.abspath(path)
        dir_path = os.path.dirname(path)
        os.makedirs(dir_path, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            f.write(schema)

    def load_schema(self, database: str, table: str, sub_location: str = None):
        """Loads a spark from a yaml file and deserialises to a spark schema."""

        path = self._mkpath(database, table, sub_location)

        self._logger.debug(
            f"Loading schema for dataset {database}.{table} from {path} using {type(self.fs)}"
        )

        try:
            schema = self.fs.read_file(path, FileFormat.TEXT)
        except Exception as e:
            raise SchemaNotFound(path) from e

        msg = json.dumps(schema)
        self._logger.debug(msg)

        return schema

    class Config:
        arbitrary_types_allowed = True
