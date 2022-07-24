from enum import Enum

from ._dbfs_file_system import DbfsFileSystem
from ._file_system import FileSystem
from ._ifile_system import IFileSystem
import logging


class FileSystemType(Enum):
    FILE = 1
    DBFS = 2


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

    def get_file_system_type(self, context, config: dict) -> IFileSystem:

        datalake_protocol: str = config["datalake_protocol"]
        fs_type: FileSystemType = self._get_fs_type(datalake_protocol)

        context.log.info(f"Setting filestystem using protocol {datalake_protocol}")

        context.log.debug(f"Setting FileSystemType using type {fs_type}")
        file_system: IFileSystem = self._file_system.get(fs_type)

        if not file_system:
            self._logger.error(
                f"FileSystemType {fs_type.name} not registered in the file_system factory"
            )
            raise ValueError(fs_type)

        return file_system(context, datalake_protocol)


factory = _FileSystemFactory()
factory.register_file_system_type(FileSystemType.FILE, FileSystem)
factory.register_file_system_type(FileSystemType.DBFS, DbfsFileSystem)
