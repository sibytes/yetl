from enum import Enum


class TableType(str, Enum):
    read = "read"
    write = "write"
    delta_lake = "delta_lake"
