from pydantic import Field
from ._i_pipeline_repo import IPipelineRepo
from ..file_system import file_system_factory, IFileSystem, FileSystemType
from pydantic import PrivateAttr
from typing import Any
from ..file_system._file_system_options import FileFormat
import os

_EXT = "yaml"


class PipelineFileRepo(IPipelineRepo):

    pipeline_root: str = Field(...)
    sql_root: str = Field(...)
    _fs: IFileSystem = PrivateAttr(...)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._fs = file_system_factory.get_file_system_type(FileSystemType.FILE)

    def load_pipeline(self, name: str):
        """Loads a pipeline configuration file."""

        path = os.path.join(self.pipeline_root, f"{name}.{FileFormat.YAML.value}")
        path = os.path.abspath(path)
        pipeline = self._fs.read_file(path, FileFormat.YAML)

        return pipeline

    def load_pipeline_sql(self, name: str):
        """Loads a pipeline SQL file."""

        path = os.path.join(self.pipeline_root, f"{name}.{FileFormat.SQL.value}")
        path = os.path.abspath(path)
        pipeline = self._fs.read_file(path, FileFormat.SQL)

        return pipeline