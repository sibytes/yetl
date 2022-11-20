from .. import _config_provider as cp
from datetime import datetime
from ..file_system import file_system_factory, IFileSystem
import logging
import uuid
from ..schema_repo import schema_repo_factory, ISchemaRepo
from .._timeslice import Timeslice, TimesliceUtcNow
from ..audit import Audit
from pydantic import BaseModel, Field, PrivateAttr
from typing import Any
from abc import ABC, abstractmethod
from ..parser._constants import DatalakeProtocolOptions

class IContext(BaseModel, ABC):

    auditor: Audit = Field(...)
    project: str = Field(...)
    name: str = Field(...)
    datalake:str = Field(...)
    datalake_protocol:DatalakeProtocolOptions = Field(default=DatalakeProtocolOptions.FILE)
    pipeline_repo: dict = Field(...)
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=uuid.uuid4())
    log: logging.Logger = None
    fs: IFileSystem = None


    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.log = logging.getLogger(self.project)
        self.auditor.dataflow({"context_id": str(self.context_id)})


        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        self.fs: IFileSystem = file_system_factory.get_file_system_type(
            self, config={}
        )

    @abstractmethod
    def _get_deltalake_flow(self):
        pass

    class Config:
        arbitrary_types_allowed = True
