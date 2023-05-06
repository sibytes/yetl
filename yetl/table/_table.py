import logging
from pydantic import BaseModel, Field, PrivateAttr
from .._utils import JinjaVariables
from typing import Any, Dict, Union, List
from .._timeslice import Timeslice
from .._stage_type import StageType
from ._table_type import TableType
from .._project import Project


class ValidationThreshold(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    invalid_ratio: float = Field(default=None)
    invalid_rows: int = Field(default=None)
    max_rows: int = Field(default=None)
    min_rows: int = Field(default=None)


class Table(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    _logger: Any = PrivateAttr(default=None)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    stage: StageType = Field(...)
    database: str = Field(...)
    table: str = Field(...)
    id: Union[str, List[str]] = Field(default=[])
    custom_properties: Dict[str, Any] = Field(default=None)
    table_type: TableType = Field(...)
    warning_thresholds: ValidationThreshold = Field(default=None)
    exception_thresholds: ValidationThreshold = Field(default=None)
    project: Project = Field(...)
    container: str = Field(...)
    location: str = Field(...)
    path: str = Field(default=None)
    options: dict = Field(...)
    timeslice: Timeslice = Field(...)
    checkpoint: str = Field(default=None)
    config_path: str = Field(...)

    def _render(self):
        pass
