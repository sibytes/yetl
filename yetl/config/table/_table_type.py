from enum import Enum


class TableType(Enum):
    Read = "read"
    Write = "write"
    DeltaLake = "delta_lake"
