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
DATAFLOW_ID = "_dataflow_id"
DATASET_ID = "_dataset_id"
LOAD_TIMESTAMP = "_load_timestamp"
TIMESLICE = "_timeslice"
FILENAME = "_filename"
FILEPATH_FILENAME = "_filepath_filename"
FILEPATH = "_filepath"

# yetl table properties

# If the SQL or Spark Schema depending on the type doesn't exist
# then it will be inferred and created automatically
YETL_TBLP_SCHEMA_CREATE_IF_NOT_EXISTS = "yetl.schema.createIfNotExists"
# Add the _corrupt_recort into the schema when it is autogenerated
# with yetl.schema.createIfNotExists for handling permissive loads
YETL_TBLP_SCHEMA_CORRUPT_RECORD = "yetl.schema.corruptRecord"
# following writes to delta tables optimise and zorderby
# if the table has partitions it will only execute on partitions that were
# affected by the write operation.
YETL_TBLP_OPTIMIZE_ZORDER_BY = "yetl.delta.optimizeZOrderBy"

# lineages columns that will be added to datasets
YETL_TBLP_METADATA_TIMESLICE = "yetl.metadata.timeslice"
YETL_TBLP_METADATA_FILEPATH_FILENAME = "yetl.metadata.filepathFilename"
YETL_TBLP_METADATA_FILEPATH = "yetl.metadata.filepath"
YETL_TBLP_METADATA_FILENAME = "yetl.metadata.filename"
YETL_TBLP_METADATA_CONTEXT_ID = "yetl.metadata.contextId"
YETL_TBLP_METADATA_DATAFLOW_ID = "yetl.metadata.dataflowId"
YETL_TBLP_METADATA_DATASET_ID = "yetl.metadata.datasetId"


class YetlTableProperties(Enum):
    # If the SQL or Spark Schema depending on the type doesn't exist
    # then it will be inferred and created automatically
    SCHEMA_CREATE_IF_NOT_EXISTS = "yetl.schema.createIfNotExists"
    # Add the _corrupt_recort into the schema when it is autogenerated
    # with yetl.schema.createIfNotExists for handling permissive loads
    SCHEMA_CORRUPT_RECORD = "yetl.schema.corruptRecord"
    SCHEMA_CORRUPT_RECORD_NAME = "yetl.schema.corruptRecordName"
    # following writes to delta tables optimise and zorderby
    # if the table has partitions it will only execute on partitions that were
    # affected by the write operation.
    OPTIMIZE_ZORDER_BY = "yetl.delta.optimizeZOrderBy"
    METADATA_TIMESLICE = "yetl.metadata.timeslice"
    METADATA_FILEPATH_FILENAME = "yetl.metadata.filepathFilename"
    METADATA_FILEPATH = "yetl.metadata.filepath"
    METADATA_FILENAME = "yetl.metadata.filename"
    METADATA_CONTEXT_ID = "yetl.metadata.contextId"
    METADATA_DATAFLOW_ID = "yetl.metadata.dataflowId"
    METADATA_DATASET_ID = "yetl.metadata.datasetId"

class TimesliceOptions(Enum):
    TIMESLICE_FILE_DATE_FORMAT = "timeslice_file_date_format"
    TIMESLICE_PATH_DATE_FORMAT = "timeslice_path_date_format"

class DatalakeProtocolOptions(Enum):
    FILE = "file:"
    DBFS = "dbfs:"

class Format(Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    DELTA = "delta"
