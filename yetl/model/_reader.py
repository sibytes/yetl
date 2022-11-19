from pydantic import BaseModel, Field
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Any, Optional
import json
from ._source_components import Thresholds, Exceptions, Read
from ..flow.parser.parser import JinjaVariables, render_jinja
from ..flow.parser._constants import DatalakeProtocolOptions, FormatOptions
import uuid
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class Source(BaseModel, ABC):

    @abstractmethod
    def initialise(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def validate(self):
        pass

    @property
    def is_source(self):
        return True

    @property
    def is_destination(self):
        return False

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        self._initial_load = value



def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {
        parse_properties_key(k): parse_properties_values(k, v) for k, v in obj.items()
    }
    return json.dumps(obj, default=default)




class Reader(Source):
    def __init__(__pydantic_self__, **data: Any) -> None:
        super().__init__(**data)
        __pydantic_self__.initialise()

    def initialise(self):
        path = f"{self.datalake_protocol.value}{self.datalake}/{self.path}"
        self.path = render_jinja(path, self._replacements)
        print(self.path)


    context_id:uuid.UUID
    dataflow_id:uuid.UUID
    dataframe:DataFrame = Field(default=None)
    dataset_id:uuid.UUID = Field(default=uuid.uuid4())
    datalake_protocol:DatalakeProtocolOptions = Field(default=DatalakeProtocolOptions.FILE)
    datalake:str = Field(...)
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
    _replacements:list = {
            JinjaVariables.DATABASE_NAME: database,
            JinjaVariables.TABLE_NAME: table,
            # JinjaVariables.TIMESLICE_FILE_DATE_FORMAT: context.timeslice.strftime(file_date_format),
            # JinjaVariables.TIMESLICE_PATH_DATE_FORMAT: context.timeslice.strftime(path_date_format)
    }
    _initial_load = False

    def validate(self):
        pass

    def execute(self):
        pass

    @property
    def sql_database_table(self, sep:str=".", qualifier:str="`") -> str:
        "Concatenated fully qualified database table for SQL"
        return f"{qualifier}{self.database}{qualifier}{sep}{qualifier}{self.table}{qualifier}"

    @property
    def database_table(self, sep:str=".") -> str:
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
