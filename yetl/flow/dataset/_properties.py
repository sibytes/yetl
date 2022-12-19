"""model classes for dataset table yetl properties.
    These are custom tables properties that yetl supports 
    for various features supported in yetl data flows.
"""

from pydantic import BaseModel, Field
from ..parser._constants import YetlTableProperties, CORRUPT_RECORD
from ..parser._constants import TimesliceOptions
from ._decoder import parse_properties
import json


def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = parse_properties(obj)
    return json.dumps(obj, default=default)


class LineageProperties(BaseModel):
    """Yetl table properies that drives various features of a data flow for all types of datasets"""

    # whether or not to add the context_id
    metadata_context_id: bool = Field(
        default=False, alias=YetlTableProperties.METADATA_CONTEXT_ID.value
    )

    # whether or not to add the dataflow_id
    metadata_dataflow_id: bool = Field(
        default=False, alias=YetlTableProperties.METADATA_DATAFLOW_ID.value
    )

    # whether or not to add the dataset_id
    metadata_dataset_id: bool = Field(
        default=True, alias=YetlTableProperties.METADATA_DATASET_ID.value
    )

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps


class SchemaProperties(BaseModel):
    """Yetl table properies that drives various features of a data flow for all types of datasets"""

    # Create the schema in the schema repo if it does not exist
    schema_create_if_not_exists: bool = Field(
        default=True, alias=YetlTableProperties.SCHEMA_CREATE_IF_NOT_EXISTS.value
    )

    # the name of the dataframe column that holds the currupt record details
    schema_corrupt_record_name: str = Field(
        default=CORRUPT_RECORD,
        alias=YetlTableProperties.SCHEMA_CORRUPT_RECORD_NAME.value,
    )

    # whether or not to add the corrupt record field into the dataframe
    schema_corrupt_record: bool = Field(
        default=True, alias=YetlTableProperties.SCHEMA_CORRUPT_RECORD.value
    )

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps


class SqlReaderProperties(LineageProperties):
    """Yetl table properies that drives SqlReader features of a data flow"""

    # whether or not to add the context_id
    create_as_view: bool = Field(
        default=False, alias=YetlTableProperties.CREATE_AS_VIEW.value
    )

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps


class ReaderProperties(SchemaProperties, LineageProperties):
    """Yetl table properies that drives various features of a data flow for Reader source"""

    # which timeslice property format to use to shred the timeslice from the datapath of files
    metadata_timeslice: TimesliceOptions = Field(
        default=TimesliceOptions.TIMESLICE_FILE_DATE_FORMAT,
        alias=YetlTableProperties.METADATA_TIMESLICE.value,
    )

    # whether or not to add the full filename path to the dataframe
    metadata_filepath_filename: bool = Field(
        default=True, alias=YetlTableProperties.METADATA_FILEPATH_FILENAME.value
    )

    # whether or not to add the filepath directory to the dataframe
    metadata_filepath: bool = Field(
        default=False, alias=YetlTableProperties.METADATA_FILEPATH.value
    )

    # whether or not to add the filename to the dataframe
    metadata_filename: bool = Field(
        default=False, alias=YetlTableProperties.METADATA_FILENAME.value
    )

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps


class DeltaWriterProperties(SchemaProperties, LineageProperties):
    """Yetl table properies that drives various features of a data flow for DeltaWriter destintation"""

    # whether or not to optimize z orderby deltatable partitions after the are written
    delta_optimize_z_order_by: bool = Field(
        default=False, alias=YetlTableProperties.OPTIMIZE_ZORDER_BY.value
    )

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
