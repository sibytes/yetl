from pydantic import BaseModel, Field
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Dict, Any
import json



def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {parse_properties_key(k):parse_properties_values(k,v) for k, v in obj.items()}
    return json.dumps(obj, default=default)


class Read(BaseModel):
    _DEFAULT_OPTIONS = {
        "mode": "PERMISSIVE",
        "inferSchema": False
    }
    auto:bool = Field(default=True)
    options:Dict[str,Any] = Field(default=_DEFAULT_OPTIONS)

class Reader(BaseModel):

    def __init__(__pydantic_self__, **data: Any) -> None:
        super().__init__(**data)

    yetl_properties:ReaderProperties = Field(default=ReaderProperties(), alias="properties")
    path_date_format:str = Field(default="%Y%m%d")
    file_date_format:str = Field(default="%Y%m%d")
    format:str = Field(default="json")
    path:str = Field(...)
    read:Read = Field(default=Read())

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps


    # customer_details:
    #   type: Reader     

    #   properties:
    #     yetl.schema.createIfNotExists: true
    #     yetl.schema.corruptRecord: false
    #     yetl.metadata.timeslice: timeslice_file_date_format
    #     yetl.metadata.filepathFilename: true


    #   path_date_format: "%Y%m%d"
    #   file_date_format: "%Y%m%d"
    #   format: csv
    #   path: "landing/demo/{{ timeslice_path_date_format }}/customer_details_{{ timeslice_file_date_format }}.csv"
    #   read:
    #     auto: true
    #     options:
    #       mode: PERMISSIVE
    #       inferSchema: false
    #       header: true
  