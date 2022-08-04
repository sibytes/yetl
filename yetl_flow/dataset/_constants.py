from enum import Enum

BAD_RECORDS_PATH = "badRecordsPath"
FORMAT = "format"
MODE = "mode"
OPTIONS = "options"
PATH = "path"
EXCEPTIONS = "exceptions"
TABLE = "table"
DATABASE = "database"
PERMISSIVE = "permissive"
CORRUPT_RECORD = "_corrupt_record"
MERGE_SCHEMA = "mergeSchema"
APPEND = "append"
NAME = "name"
REPLACE = "replace"
ARGS = "args"
AUTO_IO = "auto_io"
TIMESTAMP = "timestamp"
INFER_SCHEMA = "inferSchema"
RECURSIVE_FILE_LOOKUP = "recursiveFileLookup"

# lineage columns
CORRELATION_ID = "_correlation_id"
LOAD_TIMESTAMP = "_load_timestamp"
TIMESLICE = "_timeslice"
FILENAME = "_filename"


class Format(Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    DELTA = "delta"
