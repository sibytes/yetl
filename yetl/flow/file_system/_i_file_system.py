from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from ._file_system_options import FileSystemType, FileFormat
from typing import Any


class IFileSystem(BaseModel, ABC):

    protocol: FileSystemType = Field(...)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    @abstractmethod
    def rm(self, path: str, recurse=False) -> bool:
        """Removes a file or directory."""
        pass

    @abstractmethod
    def cp(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Copies a file or directory, possibly across FileSystems."""
        pass

    @abstractmethod
    def mv(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Moves a file or directory, possibly across FileSystems."""
        pass

    @abstractmethod
    def ls(self, path: str) -> list:
        """Copies a file or directory, possibly across FileSystems."""
        pass

    @abstractmethod
    def put(self, file: str, contents: str, overwrite=False) -> bool:
        """Writes the given String out to a file, encoded in UTF-8."""
        pass

    @abstractmethod
    def head(self, file: str, maxBytes: int = 65536) -> str:
        """Returns up to the first 'maxBytes' bytes of the given file as a String encoded in UTF-8"""
        pass

    @abstractmethod
    def mkdirs(self, path: str) -> bool:
        """Creates the given directory if it does not exist, also creating any necessary parent directories"""
        pass

    @abstractmethod
    def read_file(self, path: str, file_format: FileFormat) -> str:
        pass

    @abstractmethod
    def append_file(self, path: str, data: dict, file_format: FileFormat):
        pass

    @abstractmethod
    def write_file(self, path: str, data: dict, file_format: FileFormat):
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass
