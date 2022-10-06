from .. import _config_provider as cp
from datetime import datetime
from ..file_system import file_system_factory, IFileSystem
import logging
import uuid
from ..schema_repo import schema_repo_factory
from .._timeslice import Timeslice, TimesliceUtcNow
from ..audit import Audit
from ..metadata_repo import metadata_repo_factory, IMetadataRepo
from abc import ABC


class IContext(ABC):
    def __init__(
        self,
        app_name: str,
        log_level: str,
        name: str,
        auditor: Audit,
        timeslice: datetime = None,
    ) -> None:
        pass

        self.auditor = auditor
        self.context_id = uuid.uuid4()
        auditor.dataflow({"context_id": str(self.context_id)})
        self.name = name
        self.app_name = app_name
        if not app_name:
            self.app_name = self.name
        self.timeslice: Timeslice = timeslice
        if not self.timeslice:
            self.timeslice = TimesliceUtcNow()
        self.log = logging.getLogger(self.app_name)
        self.log_level = log_level

        # load the context configuration
        self.config: dict = cp.load_config(self.app_name)

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

    def _get_deltalake_flow(
        self
    ):
        None
