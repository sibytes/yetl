from enum import Enum

from ._dbfs_file_system import DbfsFileSystem
from ._file_system import FileSystem
from ._ifile_system import IFileSystem
from typing import Union
import logging


class FileSystemType(Enum):
    FILE = "file:"
    DBFS = "dbfs:"


class _FileSystemFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._file_system = {}

    def register_file_system_type(
        self, fs_type: FileSystemType, file_system_type: type
    ):
        self._logger.debug(f"Register file system type {file_system_type} as {type}")
        self._file_system[fs_type] = file_system_type

    def _get_fs_type(self, name: str):
        name = name.replace(":", "").strip().upper()
        try:
            if FileSystemType[name] in FileSystemType:
                return FileSystemType[name]
        except:
            return None

    def get_file_system_type(
        self, context, fileSystemType: FileSystemType
    ) -> IFileSystem:

        if isinstance(fileSystemType, FileSystemType):
            # return based on the type asked for.
            context.log.debug(f"Setting FileSystemType using type {fileSystemType}")
            file_system: IFileSystem = self._file_system.get(fileSystemType)
            return file_system(context)

        else:
            raise Exception(
                f"FileSystemType cannot be produced using {type(fileSystemType)}"
            )


factory = _FileSystemFactory()
factory.register_file_system_type(FileSystemType.FILE, FileSystem)
# factory.register_file_system_type(FileSystemType.DBFS, DbfsFileSystem)
factory.register_file_system_type(FileSystemType.DBFS, FileSystem)
