import logging
from pydantic import BaseModel, Field, PrivateAttr
from .._utils import JinjaVariables
from typing import Any, Dict, Union, List
from .._timeslice import Timeslice
from .._stage_type import StageType
from .dataset_type import TableType


class Table(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    stage: StageType = Field(...)
    database: str = Field(...)
    table: str = Field(...)
    id: Union[str, List[str]] = Field(default=[])
    custom_properties: Dict[str, Any] = Field(default=None)
    table_type: TableType = Field(...)

    def _render(self):
        pass


class DataSet(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    _logger: Any = PrivateAttr(default=None)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    container: str = Field(...)
    root: str = Field(...)
    path: str = Field(default=None)
    options: dict = Field(...)
    timeslice: Timeslice = Field(...)
    checkpoint: str = Field(default=None)
    config_path: str = Field(...)

    def _render(self):
        pass
