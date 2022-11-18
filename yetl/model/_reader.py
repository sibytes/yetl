from pydantic import BaseModel, Field
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Any, Optional
import json
from ._source_components import Thresholds, Exceptions, Read



def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {parse_properties_key(k):parse_properties_values(k,v) for k, v in obj.items()}
    return json.dumps(obj, default=default)


class Reader(BaseModel):

    def __init__(__pydantic_self__, **data: Any) -> None:
        super().__init__(**data)

    yetl_properties:ReaderProperties = Field(default=ReaderProperties(), alias="properties")
    path_date_format:str = Field(default="%Y%m%d")
    file_date_format:str = Field(default="%Y%m%d")
    format:str = Field(default="json")
    path:str = Field(...)
    read:Read = Field(default=Read())
    exceptions:Exceptions = Field(default=None)
    thresholds:Thresholds = Field(default=None)

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps

