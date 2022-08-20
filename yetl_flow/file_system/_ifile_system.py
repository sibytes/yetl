from enum import Enum


class FileFormat(Enum):
    TEXT = 1
    JSON = 2
    YAML = 3
    JSONL = 4


class IFileSystem:
    def __init__(self, context, datalake_protocol: str) -> None:
        self.context = context
        self.datalake_protocol = datalake_protocol

    def rm(self, path: str, recurse=False) -> bool:
        """Removes a file or directory."""
        pass

    def cp(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Copies a file or directory, possibly across FileSystems."""
        pass

    def mv(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Moves a file or directory, possibly across FileSystems."""
        pass

    def ls(self, path: str) -> list:
        """Copies a file or directory, possibly across FileSystems."""
        pass

    def put(self, file: str, contents: str, overwrite=False) -> bool:
        """Writes the given String out to a file, encoded in UTF-8."""
        pass

    def head(self, file: str, maxBytes: int = 65536) -> str:
        """Returns up to the first 'maxBytes' bytes of the given file as a String encoded in UTF-8"""
        pass

    def mkdirs(self, path: str) -> bool:
        """Creates the given directory if it does not exist, also creating any necessary parent directories"""
        pass

    def read_file(self, path: str, file_format: FileFormat) -> str:
        pass

    def append_file(self, path: str, data: dict, file_format: FileFormat):
        pass

    def write_file(self, path: str, data: dict, file_format: FileFormat):
        pass

    def exists(self, path: str) -> bool:
        pass
