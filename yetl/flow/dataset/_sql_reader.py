from ._base import Source

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



class SQLReader(Source):

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self.timeslice = self.context.timeslice
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
        }
        self.datalake_protocol = self.context.datalake_protocol
        self.datalake = self.context.datalake
        self.auditor = self.context.auditor
        self.context_id = self.context.context_id

    context:SparkContext = Field(...)
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=None)
    datalake_protocol: FileSystemType = Field(default=None)
    datalake: str = Field(default=None)
    auditor:Audit = Field(default=None)

    catalog: str = Field(None)
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    database: str = Field(...)
    table: str = Field(...)
    sql:str = Field(...)
    yetl_properties: LineageProperties = Field(
        default=LineageProperties(), alias="properties"
    )
    timeslice_format: str = Field(default="%Y%m%d")
    sql_schema_database:str = Field(default=None)
    sql_schema_name:str = Field(default=None)
    format: FormatOptions = Field(default=FormatOptions.DELTA)
    read: Read = Field(default=Read())
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


    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True


