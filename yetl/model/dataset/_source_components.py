from pydantic import BaseModel, Field
from typing import Dict, Any
from enum import Enum

class ReadModeOptions(Enum):
    PERMISSIVE = "PERMISSIVE"
    FAILFAST = "FAILFAST"
    DROPMALFORMED = "DROPMALFORMED"
    BADRECORDSPATH = "badRecordsPath"


class ThresholdLimit(BaseModel):
    min_rows: int = Field(default=0)
    max_rows: int = Field(default=None)
    exception_count: int = Field(default=0)
    exception_percent: int = Field(default=0)



class Read(BaseModel):

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.mode = self.options.get("mode", ReadModeOptions.PERMISSIVE)
        self.infer_schema = self.options.get("inferSchema", False)


    _DEFAULT_OPTIONS = {"mode": ReadModeOptions.PERMISSIVE.value, "inferSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)
    mode: ReadModeOptions = Field(default=ReadModeOptions.PERMISSIVE)
    infer_schema:bool = Field(default=False)


class Exceptions(BaseModel):
    path: str = Field(...)
    database: str = Field(...)
    table: str = Field(...)


class Thresholds(BaseModel):
    warning: ThresholdLimit = Field(default=ThresholdLimit())
    error: ThresholdLimit = Field(default=ThresholdLimit())
