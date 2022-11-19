from pydantic import Field, PrivateAttr
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Any, Dict
import json
from ._source_components import Thresholds, Exceptions, Read
from ..parser.parser import JinjaVariables, render_jinja
from ..parser._constants import DatalakeProtocolOptions, FormatOptions
import uuid
from ._source import Source
from pyspark.sql import DataFrame
from .._timeslice import Timeslice


def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {
        parse_properties_key(k): parse_properties_values(k, v) for k, v in obj.items()
    }
    return json.dumps(obj, default=default)


class Reader(Source):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
            JinjaVariables.TIMESLICE_FILE_DATE_FORMAT: self.timeslice.strftime(
                self.file_date_format
            ),
            JinjaVariables.TIMESLICE_PATH_DATE_FORMAT: self.timeslice.strftime(
                self.path_date_format
            ),
        }
        path = f"{self.datalake_protocol.value}{self.datalake}/{self.path}"
        self.path = render_jinja(path, self._replacements)

    timeslice: Timeslice = Field(default=Timeslice(year="*"))
    context_id: uuid.UUID
    dataflow_id: uuid.UUID
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    datalake_protocol: DatalakeProtocolOptions = Field(
        default=DatalakeProtocolOptions.FILE
    )
    datalake: str = Field(...)
    database: str = Field(...)
    table: str = Field(...)
    yetl_properties: ReaderProperties = Field(
        default=ReaderProperties(), alias="properties"
    )
    path_date_format: str = Field(default="%Y%m%d")
    file_date_format: str = Field(default="%Y%m%d")
    format: FormatOptions = Field(default=FormatOptions.JSON)
    path: str = Field(...)
    read: Read = Field(default=Read())
    exceptions: Exceptions = Field(default=None)
    thresholds: Thresholds = Field(default=None)
    _initial_load:bool = PrivateAttr(default=False)
    _replacements:Dict[JinjaVariables, str] = PrivateAttr(default=None)

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
    def has_exceptions(self) -> bool:
        "Readable property to determine if schema exception handling has been configured"
        if self.exceptions:
            return True
        else:
            False

    @property
    def has_thresholds(self) -> bool:
        "Readable property to determine if schema thresholds have been configured"
        if self.thresholds:
            return True
        else:
            False

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
