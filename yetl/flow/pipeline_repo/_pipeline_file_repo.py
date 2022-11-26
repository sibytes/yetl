from pydantic import Field
from ._ipipeline_repo import IPipelineRepo
from ..file_system import file_system_factory, IFileSystem, FileSystemType
from pydantic import PrivateAttr
from typing import Any


class PipelineFileRepo(IPipelineRepo):

    pipeline_root: str = Field(...)
    sql_root: str = Field(...)
    _fs: IFileSystem = PrivateAttr(...)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        _fs = file_system_factory.get_file_system_type(FileSystemType.FILE)

    def load_pipeline(self, name: str):
        pass

    def load_pipeline_sql(self, name: str):
        pass
