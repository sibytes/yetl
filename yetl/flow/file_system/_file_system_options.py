from enum import Enum


class FileSystemType(Enum):
    FILE = "file:"
    DBFS = "dbfs:"


class FileFormat(Enum):
    TEXT = 1
    JSON = 2
    YAML = 3
    JSONL = 4
