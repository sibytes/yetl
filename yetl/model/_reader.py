from pydantic import BaseModel, Field
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Any, Optional
import json
from ._source_components import Thresholds, Exceptions, Read
from ..flow.parser.parser import JinjaVariables
from ..flow.parser._constants import DatalakeProtocolOptions


def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {
        parse_properties_key(k): parse_properties_values(k, v) for k, v in obj.items()
    }
    return json.dumps(obj, default=default)

        # self._path = self._get_path(dataset)
        # self._path = render_jinja(self._path, self._replacements)

        # self.auditor.dataset(self.get_metadata())

class Reader(BaseModel):
    def __init__(__pydantic_self__, **data: Any) -> None:
        super().__init__(**data)

    datalake_protocol:DatalakeProtocolOptions = Field(default=DatalakeProtocolOptions.FILE)
    datalake:str = Field(...)

    database: str = Field(...)
    table: str = Field(...)
    yetl_properties: ReaderProperties = Field(
        default=ReaderProperties(), alias="properties"
    )
    path_date_format: str = Field(default="%Y%m%d")
    file_date_format: str = Field(default="%Y%m%d")
    format: str = Field(default="json")
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

    @property
    def lakepath(self, dataset: dict):
        return f"{self.datalake_protocol}{self.datalake}/{self.path}"

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
