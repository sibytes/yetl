from pydantic import BaseModel, Field
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Dict, Any
import json



def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {parse_properties_key(k):parse_properties_values(k,v) for k, v in obj.items()}
    return json.dumps(obj, default=default)

class ThresholdLimit(BaseModel):
    min_rows:int = Field(default=0)
    max_rows:int = Field(default=None)
    exception_count:int = Field(default=0)
    exception_percent:int = Field(default=0)


class Read(BaseModel):
    _DEFAULT_OPTIONS = {
        "mode": "PERMISSIVE",
        "inferSchema": False
    }
    auto:bool = Field(default=True)
    options:Dict[str,Any] = Field(default=_DEFAULT_OPTIONS)


class Exceptions(BaseModel):
    path:str = Field(...)
    database:str = Field(...)
    table:str = Field(...)


class Thresholds(BaseModel):
    warnging:ThresholdLimit = Field(default=ThresholdLimit())
    error:ThresholdLimit = Field(default=ThresholdLimit())


class Reader(BaseModel):

    def __init__(__pydantic_self__, **data: Any) -> None:
        super().__init__(**data)

    yetl_properties:ReaderProperties = Field(default=ReaderProperties(), alias="properties")
    path_date_format:str = Field(default="%Y%m%d")
    file_date_format:str = Field(default="%Y%m%d")
    format:str = Field(default="json")
    path:str = Field(...)
    read:Read = Field(default=Read())
    exceptions = Field(default=Exceptions())
    thresholds = Field(default=Thresholds())

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
    #   exceptions:
    #     path: "delta_lake/demo_landing/{{table_name}}_exceptions"
    #     database: demo_landing
    #     table: "{{table_name}}_exceptions"

    # thresholds:
    #     warning:
        #     min_rows: 1
        #     max_rows: 1000
        #     exception_count: 0
        #     exception_percent: 0
    #     error:
        #     min_rows: 0
        #     max_rows: 100000000
        #     exception_count: 50
        #     exception_percent: 80