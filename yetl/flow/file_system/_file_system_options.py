from enum import Enum


class FileSystemType(Enum):
    FILE = "file:"
    DBFS = "dbfs:"


class FileFormat(Enum):
    TEXT = "txt"
    JSON = "json"
    YAML = "yaml"
    JSONL = "jsonl"
    SQL = "sql"
