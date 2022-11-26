import json
from ._i_schema_repo import ISchemaRepo
from ..file_system import FileFormat, IFileSystem, file_system_factory, FileSystemType
import os
from ..parser.parser import render_jinja, JinjaVariables
from ._exceptions import SchemaNotFound
from pydantic import Field
from typing import Any
import logging

_EXT = "sql"


class SqlReaderFile(ISchemaRepo):

    root: str = Field(alias="sql_root")
    log: logging.Logger = None
    # TODO: mkae private after FS is pydantic
    fs: IFileSystem = None

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        # not sure this is needed in context?
        self.fs: IFileSystem = file_system_factory.get_file_system_type(
            FileSystemType.FILE
        )

    def __init__(self, context, config: dict) -> None:
        super().__init__(context, config)
        self.root_path = config["pipeline_file"]["sql_root"]

    def _mkpath(self, database_name: str, table_name: str, sub_location: str):
        """Function that builds the schema path"""

        replacements = {JinjaVariables.ROOT: self.root}
        path = render_jinja(sub_location, replacements)
        path = f"{path}/{database_name}/{table_name}.{self._EXT}"
        return path

    def save_schema(
        self, schema: str, database_name: str, table_name: str, sub_location: str
    ):
        """Serialise delta table to a create table sql file."""
        path = self._mkpath(database_name, table_name, sub_location)
        path = os.path.abspath(path)
        dir_path = os.path.dirname(path)
        os.makedirs(dir_path, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            f.write(schema)

    def load_schema(self, database_name: str, table_name: str, sub_location: str):
        """Loads a spark from a yaml file and deserialises to a spark schema."""

        path = self._mkpath(database_name, table_name, sub_location)

        self.context.log.info(
            f"Loading schema for dataset {database_name}.{table_name} from {path} using {type(self.fs)}"
        )

        try:
            schema = self.fs.read_file(path, FileFormat.TEXT)
        except Exception as e:
            raise SchemaNotFound(path) from e

        msg = json.dumps(schema)
        self.context.log.debug(msg)

        return schema

    class Config:
        arbitrary_types_allowed = True
