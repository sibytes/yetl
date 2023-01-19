from ._dbfs_file_system import DbfsFileSystem
from ._file_system import FileSystem
from ._i_file_system import IFileSystem
from ._file_system_options import FileSystemType
import logging


class _FileSystemFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
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

    def get_file_system_type(self, fileSystemType: FileSystemType) -> IFileSystem:

        if isinstance(fileSystemType, FileSystemType):
            # return based on the type asked for.
            self._logger.debug(f"Setting FileSystemType using type {fileSystemType}")
            file_system: IFileSystem = self._file_system.get(fileSystemType)
            # different file systems have different arguments
            return file_system(protocol=fileSystemType)

        else:
            raise Exception(
                f"FileSystemType cannot be produced using {type(fileSystemType)}"
            )


factory = _FileSystemFactory()
factory.register_file_system_type(FileSystemType.FILE, FileSystem)
factory.register_file_system_type(FileSystemType.DBFS, DbfsFileSystem)
