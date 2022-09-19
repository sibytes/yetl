from enum import Enum


class SaveModeType(Enum):
    """https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#save-modes"""

    DEFAULT = "default"
    ERROR_IF_EXISTS = "errorifexists"
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    MERGE = "merge"
    OVERWRITE_SCHEMA = "overwriteSchema"
