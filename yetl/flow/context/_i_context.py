from .. import _config_provider as cp
from datetime import datetime
from ..file_system import file_system_factory, IFileSystem, FileSystemType
import logging
import uuid
from ..schema_repo import schema_repo_factory, ISchemaRepo
from .._timeslice import Timeslice, TimesliceUtcNow
from ..audit import Audit
from pydantic import BaseModel, Field, PrivateAttr
from typing import Any
from abc import ABC, abstractmethod

class IPipelineRepo(BaseModel, ABC):

    @abstractmethod
    def load_pipeline(self, name:str):
        pass

    @abstractmethod
    def load_pipeline_sql(self, name:str):
        pass

class PipelineFileRepo(IPipelineRepo):

    pipeline_root:str = Field(default="./config/{{project}}/pipelines")
    sql_root:str = Field(default="./config/{{project}}/sql")

    def load_pipeline(self, name:str):
        pass

    def load_pipeline_sql(self, name:str):
        pass


class IContext(BaseModel, ABC):

    auditor: Audit = Field(...)
    project: str = Field(...)
    name: str = Field(...)
    datalake:str = Field(...)
    datalake_protocol:FileSystemType = Field(default=FileSystemType.FILE)
    pipeline_repo_config: dict = Field(alias="pipeline_repo")
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=uuid.uuid4())
    log: logging.Logger = None
    fs: IFileSystem = None
    pipeline_repository:IPipelineRepo = Field(default=PipelineFileRepo())


    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.log = logging.getLogger(self.project)
        self.auditor.dataflow({"context_id": str(self.context_id)})


        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        self.fs: IFileSystem = file_system_factory.get_file_system_type(
            self, self.datalake_protocol
        )

        # this we would pass into a factory, TODO: pipeline repo factory
        self.pipeline_repository = PipelineFileRepo(**self.pipeline_repo_config)

    @abstractmethod
    def _get_deltalake_flow(self):
        pass

    class Config:
        arbitrary_types_allowed = True
