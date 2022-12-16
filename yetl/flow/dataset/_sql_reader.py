from ._base import Source, SQLTable
from ._decoder import parse_properties_key, parse_properties_values
from pyspark.sql import DataFrame
from ._properties import LineageProperties
from pydantic import Field, PrivateAttr, BaseModel
from pyspark.sql import DataFrame
import uuid
from ..file_system import FileSystemType
from ..context import SparkContext
from .._timeslice import Timeslice, TimesliceUtcNow
from ..parser.parser import JinjaVariables, render_jinja
from ..parser._constants import FormatOptions
from typing import Dict, Any
from ..audit import Audit
import json


def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {
        parse_properties_key(k): parse_properties_values(k, v) for k, v in obj.items()
    }
    return json.dumps(obj, default=default)


class Read(BaseModel):
    auto: bool = Field(default=True)


class SQLReader(Source, SQLTable):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self.auditor = self.context.auditor
        self.timeslice = self.context.timeslice
        self.datalake_protocol = self.context.datalake_protocol
        self.datalake = self.context.datalake
        self.render()
        self.context_id = self.context.context_id
        self._init_task_read_schema()

    def render(self):
        if self.datalake is None:
            raise Exception("datalake root path cannot be None")

        if self.datalake_protocol is None:
            raise Exception("datalake protocol cannot be None")

        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
            JinjaVariables.ROOT: f"{self.datalake_protocol.value}{self.datalake}",
        }

    def _init_task_read_schema(self):


        if (not self.sql) or (not "\n" in self.sql) or (self.sql_uri):
            if not self.sql_uri:
                    self.sql_uri = self.sql
            self.sql = self.context.pipeline_repository.load_pipeline_sql(self.database, self.table, self.sql_uri)


   

    context: SparkContext = Field(...)
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=None)
    datalake_protocol: FileSystemType = Field(default=None)
    datalake: str = Field(default=None)
    auditor: Audit = Field(default=None)
    sql_uri: str = Field(default=None)
    catalog: str = Field(None)
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    sql: str = Field(...)
    yetl_properties: LineageProperties = Field(
        default=LineageProperties(), alias="properties"
    )
    timeslice_format: str = Field(default="%Y%m%d")
    format: FormatOptions = Field(default=FormatOptions.DELTA)
    read: Read = Field(default=Read())
    _initial_load: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)

    def verify(self):
        pass

    def execute(self):
        pass

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
