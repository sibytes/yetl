from enum import Enum


class DataSetType(Enum):
    Read = "read"
    Write = "write"
    DeltaLake = "delta_lake"


class TableType(Enum):
    ReadTable = "read"
    DeltaLakeTable = "delta_lake"
