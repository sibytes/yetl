import os
import shutil
from ._ifile_system import IFileSystem, FileFormat
from typing import Type, Union
import yaml
import json


class FileSystem(IFileSystem):
    def __init__(self, context: str, datalake_protocol: str = "file:") -> None:
        super().__init__(context, datalake_protocol)

    def rm(self, path: str, recurse=False) -> bool:
        """Removes a file or directory."""

        if os.path.exists(path):
            if os.path.isfile(path):
                os.remove(path)
                return True
            elif os.path.isdir(path):
                if recurse:
                    shutil.rmtree(path)
                    return True
                else:
                    os.rmdir(path)
                    return True
            else:
                return False
        else:
            return False

    def cp(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Copies a file or directory, possibly across FileSystems."""

        if os.path.exists(from_path) and os.path.exists(to_path):
            if os.path.isfile(from_path):
                # TODO: shutil.copy need to add logic wherther destination is a folder or filename
                shutil.copyfile(from_path, to_path)
                return True
            elif os.path.isdir(from_path):
                if recurse:
                    shutil.copytree(from_path, to_path)
                    return True
                else:
                    # TODO: handle if it's directory that must have recurse true
                    False
            else:
                return False
        else:
            return False

    def mv(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Moves a file or directory, possibly across FileSystems."""
        if not os.path.exists(to_path):
            self.mkdirs(to_path)

        if os.path.exists(from_path) and os.path.exists(to_path):
            shutil.move(from_path, to_path)
            return True
        else:
            return False

    def ls(self, path: str) -> list:
        """Copies a file or directory, possibly across FileSystems."""
        return os.listdir(path)

    def put(self, file: str, contents: str, overwrite=False) -> bool:
        """Writes the given String out to a file, encoded in UTF-8."""
        if overwrite:
            mode = "w"
        else:
            mode = "a"
        with open(file, mode) as f:
            f.write(contents)

    def head(self, file: str, maxBytes: int = 65536) -> str:
        """Returns up to the first 'maxBytes' bytes of the given file as a String encoded in UTF-8"""
        with open(file, "rb") as fp:
            try:
                data = fp.read(maxBytes)
            except EOFError:
                pass

        return data.decode("utf-8")

    def mkdirs(self, path: str) -> bool:
        """Creates the given directory if it does not exist, also creating any necessary parent directories"""
        if os.path.exists(path):
            return False
        else:
            os.makedirs(path)
            return True

    def read_file(self, path: str, file_format: FileFormat) -> str:

        with open(path, "r") as f:
            if file_format == FileFormat.JSON:
                data = json.loads(f.read())
            elif file_format == FileFormat.YAML:
                data = f.read()
                data = yaml.safe_load(data)
            elif file_format == FileFormat.TEXT:
                data = f.read()
            else:
                raise Exception(
                    f"File format not supported {file_format} when reading file {path}"
                )

        return data

    def append_file(self, path: str, data: Union[str, dict], file_format: FileFormat):

        if isinstance(data, dict) and file_format == FileFormat.TEXT:
            raise TypeError()
        if isinstance(data, str) and file_format in [
            FileFormat.JSON,
            FileFormat.YAML,
            FileFormat.JSONL,
        ]:
            raise TypeError()
        if not isinstance(data, (str, dict)):
            raise TypeError()

        with open(path, "a") as f:
            if file_format == FileFormat.JSON:
                data_formatted = json.dumps(data, indent=4, default=str)
                f.write(data_formatted)
            elif file_format == FileFormat.YAML:
                data_formatted = yaml.safe_dump(data, indent=4)
                f.write(data_formatted)
            elif file_format == FileFormat.TEXT:
                f.write(data)
            else:
                raise Exception(
                    f"File format not supported {file_format} when appending file {path}"
                )

    def write_file(self, path: str, data: Union[str, dict], file_format: FileFormat):

        if isinstance(data, dict) and file_format == FileFormat.TEXT:
            raise TypeError()
        if isinstance(data, str) and file_format in [
            FileFormat.JSON,
            FileFormat.YAML,
            FileFormat.JSONL,
        ]:
            raise TypeError()
        if not isinstance(data, (str, dict)):
            raise TypeError()

        with open(path, "w") as f:
            if file_format == FileFormat.JSON:
                data_formatted = json.dumps(data, indent=4, default=str)
                f.write(data_formatted)
            elif file_format == FileFormat.YAML:
                data_formatted = yaml.safe_dump(data, indent=4)
                f.write(data_formatted)
            elif file_format == FileFormat.TEXT:
                f.write(data)
            else:
                raise Exception(
                    f"File format not supported {file_format} when writing file {path}"
                )

    def exists(self, path: str) -> bool:
        return os.path.exists(path)
