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
FAIL_FAST = "failfast"
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
CHECK_CONSTRAINTS = "check_constraints"
PROPERTIES = "properties"
PARTITIONS = "partitions"

# lineage columns
CONTEXT_ID = "_context_id"
LOAD_TIMESTAMP = "_load_timestamp"
TIMESLICE = "_timeslice"
FILENAME = "_filename"
FILEPATH_FILENAME = "_filepath_filename"
FILEPATH = "_filepath"

# yetl table properties
YETL_TBLP_SCHEMA_CREATE_IF_NOT_EXISTS = "yetl.schema.createIfNotExists"
YETL_TBLP_SCHEMA_CORRUPT_RECORD = "yetl.schema.corrupt_record"
YETL_TBLP_METADATA_TIMESLICE = "yetl.metadata.timeslice"
YETL_TBLP_METADATA_FILEPATH_FILENAME = "yetl.metadata.filepath_filename"
YETL_TBLP_METADATA_FILEPATH = "yetl.metadata.filepath"
YETL_TBLP_METADATA_FILENAME = "yetl.metadata.filename"
YETL_TBLP_OPTIMIZE_ZORDER_BY = "yetl.delta.optimizeZOrderBy"


class Format(Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    DELTA = "delta"
