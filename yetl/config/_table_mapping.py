from pydantic import BaseModel, Field
from typing import Union, Any, Dict
from .table import Table


class TableMapping(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    destination: Table = Field(...)
    source: Union[Dict[str, Table], Table] = Field(...)
