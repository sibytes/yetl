from ..file_system import IFileSystem, FileSystemType
import logging
import uuid
from ..pipeline_repo import pipeline_repo_factory, IPipelineRepo
from .._timeslice import Timeslice, TimesliceUtcNow
from ..audit import Audit
from pydantic import BaseModel, Field
from typing import Any
from .._environment import Environment
from ._context_options import ContextType


class IContext(BaseModel):

    auditor: Audit = Field(...)
    project: str = Field(...)
    name: str = Field(...)
    datalake: str = Field(...)
    pipeline_repository: IPipelineRepo = Field(default=None)
    datalake_protocol: FileSystemType = Field(default=FileSystemType.FILE)
    pipeline_repo_config: dict = Field(alias="pipeline_repo")
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=uuid.uuid4())
    log: logging.Logger = None
    datalake_fs: IFileSystem = None
    environment: Environment = Field(...)
    context_type: ContextType = Field(...)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.log = logging.getLogger(self.project)
        self.auditor.dataflow({"context_id": str(self.context_id)})

        # abstraction of the pipeline repo, used for loading pipeline configuration
        self.pipeline_repository = pipeline_repo_factory.get_pipeline_repo_type(
            self.pipeline_repo_config
        )

    class Config:
        arbitrary_types_allowed = True