from .. import _config_provider as cp
from datetime import datetime
from ..file_system import file_system_factory, IFileSystem
import logging
import uuid
from ..schema_repo import schema_repo_factory
from .._timeslice import Timeslice, TimesliceUtcNow
from ..audit import Audit
from ..metadata_repo import metadata_repo_factory, IMetadataRepo
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field
from typing import Any

class BaseContext(BaseModel, ABC):

    auditor:Audit = Field(...)
    project:str = Field(...)
    name:str = Field(...)
    timeslice:Timeslice = Field(default=TimesliceUtcNow())
    context_id:uuid.UUID = Field(default=uuid.uuid4())
    config:dict = Field(default=None)
    metadata_repo: IMetadataRepo = None
    log:logging.Logger = None
    fs: IFileSystem = None
    schema_repo_factory = None


    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.log = logging.getLogger(self.project)
        self.auditor.dataflow({"context_id": str(self.context_id)})


        # load the context configuration
        self.config = cp.load_config(self.project)

        # abstraction of the filesystem for driver file commands e.g. rm, ls, mv, cp
        self.fs: IFileSystem = file_system_factory.get_file_system_type(
            self, config=self.config
        )

        # abstraction of the metadata repo for saving yetl dataflow lineage.
        self.metadata_repo: IMetadataRepo = (
            metadata_repo_factory.get_metadata_repo_type(self, config=self.config)
        )

        # abstraction of the schema repo
        self.schema_repo_factory = schema_repo_factory

    @abstractmethod
    def _get_deltalake_flow(self):
        pass

    class Config:
        arbitrary_types_allowed = True