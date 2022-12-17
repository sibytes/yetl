from pyspark.sql import SparkSession
from ._i_file_system import IFileSystem
from typing import Union, Callable, Any
import yaml
import json
from pydantic import PrivateAttr
from ._file_system_options import FileFormat


class DbfsFileSystem(IFileSystem):

    _fs: Callable = PrivateAttr(...)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        spark = SparkSession.builder.getOrCreate()
        try:
            self._fs = self._get_dbutils(spark).fs
        except ModuleNotFoundError as e:
            raise Exception(
                "Cannot import DBUtils, most likely cause is having DBFS configured for environment that isn't databricks and doesn't support."
            ) from e

    def _get_dbutils(self, spark: SparkSession):
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)

    def rm(self, path: str, recurse=False) -> bool:
        """Removes a file or directory."""
        return self._fs.rm(path, recurse)

    def cp(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Copies a file or directory, possibly across FileSystems."""
        return self._fs.cp(from_path, to_path, recurse)

    def mv(self, from_path: str, to_path: str, recurse=False) -> bool:
        """Moves a file or directory, possibly across FileSystems."""
        return self._fs.mv(from_path, to_path, recurse)

    def ls(self, path: str) -> list:
        """Copies a file or directory, possibly across FileSystems."""
        return self._fs.ls(path)

    def put(self, file: str, contents: str, overwrite=False) -> bool:
        """Writes the given String out to a file, encoded in UTF-8."""
        return self._fs.put(file, contents, overwrite)

    def head(self, file: str, maxBytes: int = 65536) -> str:
        """Returns up to the first 'maxBytes' bytes of the given file as a String encoded in UTF-8"""
        return self._fs.head(file, maxBytes)

    def mkdirs(self, path: str) -> bool:
        """Creates the given directory if it does not exist, also creating any necessary parent directories"""
        return self._fs.mkdirs(path)

    def read_file(self, path: str, file_format: FileFormat) -> str:

        if path[0] != ".":
            path = f"/dbfs{path}"
        with open(path, "r") as f:
            if file_format == FileFormat.JSON:
                data = json.loads(f.read())
            elif file_format == FileFormat.YAML:
                data = f.read()
                data = yaml.safe_load(data)
            elif file_format in (FileFormat.TEXT, FileFormat.SQL):
                data = f.read()
            else:
                raise Exception(
                    f"File format not supported {file_format} when reading file {path}"
                )

        return data

    def append_file(self, path: str, data: Union[str, dict], file_format: FileFormat):

        if path[0] != ".":
            path = f"/dbfs{path}"
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

        if path[0] != ".":
            path = f"/dbfs{path}"
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
        raise NotImplementedError()

    class Config:
        arbitrary_types_allowed = True
