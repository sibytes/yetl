from pydantic import BaseModel, Field
from typing import Any, List
from ...flow.dataset import Thresholds


class Table(BaseModel):
    table: str = Field(...)
    enabled_exceptions: bool = Field(default=False)
    thresholds: Thresholds = Field(default=None)
    keys:List[str] = Field(default=[])


class Project(BaseModel):

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    database:str = Field(...)
    tables:List[Table] = Field(default=[])

