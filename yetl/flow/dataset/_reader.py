from pydantic import Field, PrivateAttr
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Any, Dict, Union
import json
from ..parser.parser import (
    JinjaVariables,
    render_jinja,
    to_regex_search_pattern,
    to_spark_format_code,
    prefix_root_var,
)
from ..parser._constants import FormatOptions
from ..file_system import FileSystemType
import uuid
from ._base import Source, SQLTable
from pyspark.sql import DataFrame
from .._timeslice import Timeslice, TimesliceUtcNow
from pydantic import BaseModel, Field
from enum import Enum
from ..context import SparkContext, DatabricksContext
from ..audit import Audit, AuditTask
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as fn
from ..schema_repo import SchemaNotFound
from .. import _delta_lake as dl
from datetime import datetime
from ..parser._constants import *
from ._validation import PermissiveSchemaOnRead, BadRecordsPathSchemaOnRead, Thresholds
import logging


class ReaderConfigurationException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {
        parse_properties_key(k): parse_properties_values(k, v) for k, v in obj.items()
    }
    return json.dumps(obj, default=default)


class ReadModeOptions(Enum):
    PERMISSIVE = "PERMISSIVE"
    FAILFAST = "FAILFAST"
    DROPMALFORMED = "DROPMALFORMED"
    BADRECORDSPATH = "badRecordsPath"


class Read(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    _DEFAULT_OPTIONS = {"mode": ReadModeOptions.PERMISSIVE.value, "inferSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)
    _logger: Any = PrivateAttr(default=None)

    def render(self, replacements: Dict[JinjaVariables, str]):

        if self.get_mode() == ReadModeOptions.BADRECORDSPATH:
            path = self.options.get(ReadModeOptions.BADRECORDSPATH.value, "")
            # if the path has no root {{root}} prefixed then add one
            path = prefix_root_var(self.path)
            self.options[ReadModeOptions.BADRECORDSPATH.value] = render_jinja(
                path, replacements
            )
            if "mode" in self.options:
                del self.options["mode"]

    @property
    def infer_schema(self):
        return self.options.get("inferSchema", False)

    @property
    def bad_records_path(self):
        return self.options.get(ReadModeOptions.BADRECORDSPATH.value, None)

    @infer_schema.setter
    def infer_schema(self, value: bool):
        self.options["inferSchema"] = value

    def set_infer_schema(self, infer_schema: bool):
        self.options["inferSchema"] = infer_schema

    def get_mode(self):
        mode = None
        if isinstance(
            self.options.get(ReadModeOptions.BADRECORDSPATH.value, None), str
        ):
            mode = ReadModeOptions.BADRECORDSPATH
        if not mode:
            mode = self.options.get("mode", ReadModeOptions.PERMISSIVE.value)
            mode = ReadModeOptions(mode)
        return mode

    def set_mode(self, mode: ReadModeOptions, bad_records_path: str = None):
        if mode == ReadModeOptions.BADRECORDSPATH and not bad_records_path:
            raise Exception(
                f"Setting mode to BADRECORDSPATH must have a bad_records_path string argument"
            )

        if mode != ReadModeOptions.BADRECORDSPATH:
            self.options["mode"] = mode.value
        else:
            self.options[ReadModeOptions.BADRECORDSPATH.value] = bad_records_path
            if "mode" in self.options:
                del self.options["mode"]


class Exceptions(SQLTable):
    path: str = Field(...)
    context: Union[SparkContext, DatabricksContext] = Field(...)
    _logger: Any = PrivateAttr(default=None)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    def render(self, replacements: Dict[JinjaVariables, str]):

        self.table = render_jinja(self.table, replacements)
        self.database = render_jinja(self.database, replacements)
        # if the path has no root {{root}} prefixed then add one
        path = prefix_root_var(self.path)
        self.path = render_jinja(path, replacements)

    def table_exists(self):
        dl.create_database(self.context, self.database)
        exists = dl.table_exists(self.context, self.database, self.table)
        return exists

    def create_table(self):
        sql = dl.create_table(
            self.context,
            self.database,
            self.table,
            self.path,
        )
        return sql


class Reader(Source, SQLTable):
    def __init__(self, **data: Any) -> None:
        self._cascade_context(data)
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self.initialise()

    def initialise(self):
        self.auditor = self.context.auditor
        self.timeslice = self.context.timeslice
        self.datalake_protocol = self.context.datalake_protocol
        self.datalake = self.context.datalake
        self.auditor = self.context.auditor
        self.context_id = self.context.context_id
        self.render()
        self.auditor.dataset(self.get_metadata())
        self._init_task_read_schema()
        if self.read.auto:
            self._init_task_create_exception_table()
        self._init_validate()

    def _cascade_context(self, data: dict):
        if data.get("exceptions") and data.get("context"):
            data["exceptions"]["context"] = data.get("context")

    context: Union[SparkContext, DatabricksContext] = Field(...)
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=None)
    dataflow_id: uuid.UUID = Field(default=None)
    datalake_protocol: FileSystemType = Field(default=None)
    datalake: str = Field(default=None)
    auditor: Audit = Field(default=None)

    catalog: str = Field(None)
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    yetl_properties: ReaderProperties = Field(
        default=ReaderProperties(), alias="properties"
    )
    path_date_format: str = Field(default="%Y%m%d")
    file_date_format: str = Field(default="%Y%m%d")
    format: FormatOptions = Field(default=FormatOptions.JSON)
    path: str = Field(...)
    read: Read = Field(default=Read())
    exceptions: Exceptions = Field(default=None)
    thresholds: Thresholds = Field(default=None)
    spark_schema: StructType = None
    _initial_load: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    _create_spark_schema: bool = PrivateAttr(default=False)

    def render(self):
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
            JinjaVariables.TIMESLICE_FILE_DATE_FORMAT: self.timeslice.strftime(
                self.file_date_format
            ),
            JinjaVariables.TIMESLICE_PATH_DATE_FORMAT: self.timeslice.strftime(
                self.path_date_format
            ),
            JinjaVariables.ROOT: f"{self.datalake_protocol.value}{self.datalake}",
        }
        # if the path has no root {{root}} prefixed then add one
        path = prefix_root_var(self.path)
        self.path = render_jinja(path, self._replacements)

        if self.has_exceptions:
            self.exceptions.render(self._replacements)
        self.read.render(self._replacements)

    def _init_task_read_schema(self):
        try:
            self.spark_schema = self.context.spark_schema_repository.load_schema(
                database=self.database, table=self.table
            )
        except SchemaNotFound as e:
            if self.yetl_properties.schema_create_if_not_exists:
                # default all settings to allow a inferred schema load
                # we're effectively the user is forcing this to they've configured it
                # to create the schema automatically.
                self._create_spark_schema = True
                self.read.set_infer_schema(True)
                self.read.set_mode(ReadModeOptions.PERMISSIVE)
                self._initial_load = True
                self.exceptions = None
            elif not self._infer_schema:
                raise e

    def _init_task_create_exception_table(self):

        if self.has_exceptions:
            table_exists = self.exceptions.table_exists()
            if table_exists:
                self._logger.debug(
                    f"Exception table already exists {self.exceptions.database_table} at {self.exceptions.path} {CONTEXT_ID}={str(self.context_id)}"
                )
                self._initial_load = False
            else:
                start_datetime = datetime.now()
                sql = self.exceptions.create_table()
                self.auditor.dataset_task(
                    self.dataset_id, AuditTask.SQL, sql, start_datetime
                )
                self._initial_load = True

    def _init_validate(self):
        """Validate the conguration ensuring that compatible options are configured.
        Because we're validating across class composition and there is complexity encapsulated in those classes
        bydantic validation isn't appropriate since it will duplicate the logic required to shred the dictionaries
        that we have already used in pydantic to create the objects.
        """
        if self.read.get_mode() == ReadModeOptions.BADRECORDSPATH and not isinstance(
            self.context, DatabricksContext
        ):
            raise ReaderConfigurationException(
                f"{ReadModeOptions.BADRECORDSPATH.value} is only supported on databricks runtime."
            )

        if self._create_spark_schema:
            if self.read.get_mode() not in [
                ReadModeOptions.BADRECORDSPATH,
                ReadModeOptions.PERMISSIVE,
            ]:
                if self.has_exceptions:
                    msg = f"{MODE}={self.read.get_mode()}, exceptions can only be handled on a mode={ReadModeOptions.PERMISSIVE.value} or {ReadModeOptions.BADRECORDSPATH.value}, {EXCEPTIONS} configuration will be disabled."
                    self._logger.warning(msg)
                    self.exceptions = None
                else:
                    msg = f"{MODE}={self.read.get_mode()} requires Exceptions details configured."
                    self._logger.error(msg)
                    raise ReaderConfigurationException(msg)

            if (
                self.read.get_mode() == ReadModeOptions.PERMISSIVE
                and self.has_exceptions
                and not self.has_corrupt_column
            ):
                msg = f"""{MODE}={ReadModeOptions.PERMISSIVE} exceptions requires _corrupt_record column in the schema, 
                if not schema exists yet use the properties to add one {YetlTableProperties.SCHEMA_CORRUPT_RECORD.name},
                {YetlTableProperties.SCHEMA_CORRUPT_RECORD_NAME.name}."""
                self._logger.error(msg)
                raise ReaderConfigurationException(msg)

    def _get_validation_exceptions_handler(self):
        def handle_exceptions(exceptions: DataFrame):
            exceptions_count = 0
            if self.has_exceptions:
                exceptions_count = exceptions.count()
                if exceptions and exceptions_count > 0:
                    options = {MERGE_SCHEMA: True}
                    self._logger.warning(
                        f"Writing {exceptions_count} exception(s) from {self.database_table} to {self.exceptions.database_table} delta table {CONTEXT_ID}={str(self.context_id)}"
                    )
                    exceptions.write.format(FormatOptions.DELTA.value).options(
                        **options
                    ).mode(APPEND).save(self.exceptions.path)
            return exceptions_count

        return handle_exceptions

    def verify(self):

        validation_handler = self._get_validation_exceptions_handler()
        validator = None

        if self.read.get_mode() == ReadModeOptions.BADRECORDSPATH:
            self._logger.debug(
                f"Validating dataframe read using badRecordsPath at {self.read.bad_records_path} {CONTEXT_ID}={str(self.context_id)}"
            )
            validator = BadRecordsPathSchemaOnRead(
                context=self.context,
                dataframe=self.dataframe,
                validation_handler=validation_handler,
                database=self.database,
                table=self.table,
                warning_thresholds=self.thresholds.warning,
                error_thresholds=self.thresholds.error,
                path=self.read.bad_records_path,
                spark=self.context.spark,
            )

        if (
            self.read.get_mode() == ReadModeOptions.PERMISSIVE
            and self.has_corrupt_column
        ):
            self._logger.debug(
                f"Validating dataframe read using PERMISSIVE corrupt column at {CORRUPT_RECORD} {CONTEXT_ID}={str(self.context_id)}"
            )
            validator = PermissiveSchemaOnRead(
                context=self.context,
                dataframe=self.dataframe,
                validation_handler=validation_handler,
                database=self.database,
                table=self.table,
                warning_thresholds=self.thresholds.warning,
                error_thresholds=self.thresholds.error,
            )

        if validator:
            start_datetime = datetime.now()
            level_validation, validation = validator.validate()
            self.auditor.dataset_task(
                self.dataset_id,
                AuditTask.SCHEMA_ON_READ_VALIDATION,
                validation,
                start_datetime,
            )
            self.dataframe = validator.dataframe

    def _execute_add_source_metadata(self, df: DataFrame):

        if self.yetl_properties.metadata_filepath_filename:
            df: DataFrame = df.withColumn(FILEPATH_FILENAME, fn.input_file_name())

        if self.yetl_properties.metadata_filepath:
            df: DataFrame = df.withColumn(FILEPATH, fn.input_file_name()).withColumn(
                FILEPATH,
                fn.expr(
                    f"replace({FILEPATH}, concat('/', substring_index({FILEPATH}, '/', -1)))"
                ),
            )

        if self.yetl_properties.metadata_filename:
            df: DataFrame = df.withColumn(FILENAME, fn.input_file_name()).withColumn(
                FILENAME, fn.substring_index(fn.col(FILENAME), "/", -1)
            )

        if self.yetl_properties.metadata_context_id:
            df: DataFrame = df.withColumn(CONTEXT_ID, fn.lit(str(self.context_id)))

        if self.yetl_properties.metadata_dataflow_id:
            df: DataFrame = df.withColumn(DATAFLOW_ID, fn.lit(str(self.dataflow_id)))

        if self.yetl_properties.metadata_dataset_id:
            df: DataFrame = df.withColumn(DATASET_ID, fn.lit(str(self.dataset_id)))

        return df

    def _execute_add_timeslice(self, df: DataFrame):

        if (
            self.yetl_properties.metadata_timeslice.name
            == JinjaVariables.TIMESLICE_PATH_DATE_FORMAT.name
        ):

            pattern = to_regex_search_pattern(self.path_date_format)
            spark_format_string = to_spark_format_code(self.path_date_format)

            df: DataFrame = (
                df.withColumn(TIMESLICE, fn.input_file_name())
                .withColumn(TIMESLICE, fn.regexp_extract(fn.col(TIMESLICE), pattern, 0))
                .withColumn(
                    TIMESLICE,
                    fn.to_timestamp(TIMESLICE, spark_format_string),
                )
            )

        elif (
            self.yetl_properties.metadata_timeslice.name
            == JinjaVariables.TIMESLICE_FILE_DATE_FORMAT.name
        ):

            pattern = to_regex_search_pattern(self.file_date_format)
            spark_format_string = to_spark_format_code(self.file_date_format)

            df: DataFrame = (
                df.withColumn(TIMESLICE, fn.input_file_name())
                .withColumn(TIMESLICE, fn.substring_index(fn.col(TIMESLICE), "/", -1))
                .withColumn(TIMESLICE, fn.regexp_extract(fn.col(TIMESLICE), pattern, 0))
                .withColumn(
                    TIMESLICE,
                    fn.to_timestamp(TIMESLICE, spark_format_string),
                )
            )

        return df

    def _execute_create_spark_schema(self, df: DataFrame):
        if self._create_spark_schema:
            self._logger.debug(
                f"Saving inferred schema for {self.database}.{self.table} into schema repository. {CONTEXT_ID}={str(self.context_id)}"
            )
            self.spark_schema = df.schema
            if self.yetl_properties.schema_corrupt_record:
                self.spark_schema.add(
                    self.yetl_properties.schema_corrupt_record_name,
                    StringType(),
                    nullable=True,
                )
            self.context.spark_schema_repository.save_schema(
                self.spark_schema, self.database, self.table
            )

    def execute(self):
        self._logger.debug(
            f"Reading data for {self.database_table} from {self.path} with options {self.read.options} {CONTEXT_ID}={str(self.context_id)}"
        )

        start_datetime = datetime.now()

        df: DataFrame = self.context.spark.read.format(self.format.value)

        if self.spark_schema:
            df = df.schema(self.spark_schema)

        df = df.options(**self.read.options).load(self.path)

        # if there isn't a schema and it's configured to create one the save it to repo.
        self._execute_create_spark_schema(df=df)
        # add metadata after schema is created since we don't want these
        # derived columns in the read metadata, only the _corrupt_column if present
        df = self._execute_add_timeslice(df)
        df = self._execute_add_source_metadata(df)

        self._logger.debug(
            f"Reordering sys_columns to end for {self.database_table} from {self.path}. {CONTEXT_ID}={str(self.context_id)}"
        )
        self.dataframe = df
        detail = {"path": self.path, "options": self.read.options}
        self.auditor.dataset_task(
            self.dataset_id, AuditTask.LAZY_READ, detail, start_datetime
        )

        # only run the validator if exception handling has ben configured.
        if self.has_exceptions:
            self.verify()

        return self.dataframe

    @property
    def has_exceptions(self) -> bool:
        "Readable property to determine if schema exception handling has been configured"
        if self.exceptions:
            return True
        else:
            False

    @property
    def has_thresholds(self) -> bool:
        "Readable property to determine if schema thresholds have been configured"
        if self.thresholds:
            return True
        else:
            False

    @property
    def has_corrupt_column(self) -> bool:
        "Readable property to determine if schema has a corrupt column"
        if not self.spark_schema and self.yetl_properties.schema_create_if_not_exists:
            return self.yetl_properties.schema_corrupt_record
        if self.spark_schema:
            return (
                self.yetl_properties.schema_corrupt_record_name
                in self.spark_schema.fieldNames()
            )

    def get_metadata(self):
        metadata = super().get_metadata()
        metadata[str(self.dataset_id)]["path"] = self.path

        return metadata

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
