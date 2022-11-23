# from multiprocessing import context
from ..parser._constants import *

# from ..schema_repo import ISchemaRepo, SchemaNotFound

from .. import _delta_lake as dl
from pyspark.sql import DataFrame
import uuid

# from typing import ChainMap
# from ..parser import parser
# from ..save import save_factory, Save
# from ..audit import Audit, AuditTask
# from datetime import datetime
from .._timeslice import Timeslice, TimesliceUtcNow
from pyspark.sql import functions as fn
import json
from ._base import Destination
from pydantic import Field, PrivateAttr, BaseModel
from typing import Any, Dict, List
from ..parser.parser import JinjaVariables, render_jinja
from ._properties import DeltaWriterProperties
from ..save._save_mode_type import SaveModeType
from ..file_system import FileSystemType


class Write(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._merge_schema = self.options.get("merge_schema", False)

    _DEFAULT_OPTIONS = {"mergeSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)
    mode: SaveModeType = Field(default=SaveModeType.APPEND)
    _merge_schema: bool = PrivateAttr(default=False)

    @property
    def merge_schema(self) -> bool:
        return self.merge_schema

    @merge_schema.setter
    def merge_schema(self, value: bool):
        self.options[MERGE_SCHEMA] = value
        self._merge_schema = value


class DeltaWriter(Destination):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
        }
        path = f"{self.datalake_protocol.value}{self.datalake}/{self.path}"
        self.path = render_jinja(path, self._replacements)

    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    catalog: str = Field(None)
    context_id: uuid.UUID
    dataflow_id: uuid.UUID
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    datalake_protocol: FileSystemType = Field(default=FileSystemType.FILE)
    datalake: str = Field(...)
    database: str = Field(...)
    table: str = Field(...)
    yetl_properties: DeltaWriterProperties = Field(
        default=DeltaWriterProperties(), alias="properties"
    )
    format: FormatOptions = Field(default=FormatOptions.DELTA)
    path: str = Field(...)
    check_constraints: Dict[str, str] = Field(default=None)
    partitioned_by: List[str] = Field(default=None)
    zorder_by: List[str] = Field(default=None)
    write: Write = Field(default=Write())
    _initial_load: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)

    def validate(self):
        pass

    def execute(self):
        pass

    @property
    def sql_database_table(self, sep: str = ".", qualifier: str = "`") -> str:
        "Concatenated fully qualified database table for SQL"
        return f"{qualifier}{self.database}{qualifier}{sep}{qualifier}{self.table}{qualifier}"

    @property
    def database_table(self, sep: str = ".") -> str:
        "Concatenated database table for readability"
        return f"{self.database}{sep}{self.table}"

    @property
    def has_partitions(self) -> bool:
        if self.partitioned_by:
            return True
        else:
            return False

    @property
    def has_check_constaints(self) -> bool:
        if self.check_constraints:
            return True
        else:
            return False

    @property
    def has_zorder_by(self) -> bool:
        if self.zorder_by:
            return True
        else:
            return False

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        # when it's the initial load and schema's aren't declared
        # the delta table is created 1st with no schema and the
        # data schema loads into it. To do this we override the
        # merge schema options so the data schema is merged in
        # on the 1st load without requiring changes to pipeline.
        if not self.write.merge_schema and value:
            self.write.merge_schema = value
        self._initial_load = value

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        # json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
