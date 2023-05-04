# used to carry notebook data
from pydantic import BaseModel, Field
from typing import Any


class Notebook(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        # add the notebook path to parameters for error reporting.
        self.parameters["notebook"] = self.path

    path: str = Field(...)
    timeout: int = Field(default=3600)
    parameters: dict = Field(default={})
    retry: int = Field(default=0)
    enabled: bool = Field(default=True)
