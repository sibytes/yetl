from pydantic import BaseModel, Field
from typing import Dict, Any


class ThresholdLimit(BaseModel):
    min_rows: int = Field(default=0)
    max_rows: int = Field(default=None)
    exception_count: int = Field(default=0)
    exception_percent: int = Field(default=0)


class Read(BaseModel):
    _DEFAULT_OPTIONS = {"mode": "PERMISSIVE", "inferSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)


class Exceptions(BaseModel):
    path: str = Field(...)
    database: str = Field(...)
    table: str = Field(...)


class Thresholds(BaseModel):
    warning: ThresholdLimit = Field(default=ThresholdLimit())
    error: ThresholdLimit = Field(default=ThresholdLimit())
