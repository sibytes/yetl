from ..parser.parser import (
    JinjaVariables,
    render_jinja,
    to_regex_search_pattern,
    to_spark_format_code,
)
from ._dataset import Dataset
from pyspark.sql import functions as fn
from pyspark.sql.types import StructType, StringType
from ..parser._constants import *

from ..schema_repo import ISchemaRepo, SchemaNotFound
from ._validation import (
    PermissiveSchemaOnRead,
    BadRecordsPathSchemaOnRead,
    ThresholdLevels,
)
from pyspark.sql import DataFrame
import json
from .. import _delta_lake as dl
from ..audit import Audit, AuditTask
from datetime import datetime
from ._base import Source
from ..parser.parser import JinjaVariables


class Reader(Dataset, Source):
    def __init__(
        self,
        context,
        database: str,
        table: str,
        config: dict,
        io_type: str,
        auditor: Audit,
    ) -> None:
        super().__init__(context, database, table, config, io_type, auditor)

        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
            **self._replacements,
        }

        # gets the read, write, etc options based on the type
        self.config = config
        io_properties = config.get("read")

        # get the table properties
        properties: dict = self._get_table_properties(config["table"])
        self._set_table_properties(properties)

        self.thresholds_warnings = self._get_thresholds(config, ThresholdLevels.WARNING)
        self.thresholds_error = self._get_thresholds(config, ThresholdLevels.ERROR)

        self.has_exception_configured = EXCEPTIONS in config.keys()
        self.auto_io = io_properties.get(AUTO_IO, True)
        self.options: dict = io_properties.get(OPTIONS)
        self.infer_schema = self.options.get(INFER_SCHEMA, False)
        self.has_badrecordspath_configured = BAD_RECORDS_PATH in self.options.keys()
        self.mode = self.options.get(MODE, None)

        # try and load a schema if schema on read
        try:
            self.schema = self._get_schema(config["spark_schema_repo"])
            self.has_schema = True

        except SchemaNotFound as e:
            self.schema = None
            self.has_schema = False
            if not self.infer_schema and not self._create_schema_if_not_exists:
                self._creating_inferred_schema = False
                msg = f"schema not found for {self.database}.{self.table} as path {e}"
                raise SchemaNotFound
            else:
                msg = f"schema not found for {self.database}.{self.table} as path {e}"
                self.context.log.warning(msg)

        self._creating_inferred_schema = (
            self._create_schema_if_not_exists and not self.has_schema
        )

        if self.auto_io and self._creating_inferred_schema:
            self._on_schema_creation()

        if self.has_exception_configured:
            self._set_exceptions_attributes(config)

        if not self._creating_inferred_schema:
            self._validate_configuration()

        if self.has_badrecordspath_configured:
            self.options = self.configure_badrecords_path(
                self.options, self.datalake_protocol, self.datalake
            )

        # set the exceptions table settings if configured.

        self.database_table = f"{self.database}.{self.table}"
        # self._initial_load = super().initial_load

        if self.auto_io and self.has_exception_configured:
            self.context.log.info(
                f"auto_io = {self.auto_io} automatically creating or altering exception delta table {self.database}.{self.table} {CONTEXT_ID}={str(self.context_id)}"
            )
            self.create_or_alter_table()

    def _set_table_properties(self, properties: dict):

        self._metadata_timeslice_enabled = properties.get(
            YETL_TBLP_METADATA_TIMESLICE, None
        )
        if isinstance(self._metadata_timeslice_enabled, str):
            self._metadata_timeslice_enabled = JinjaVariables[
                self._metadata_timeslice_enabled.upper()
            ]

        self._create_schema_if_not_exists = properties.get(
            YETL_TBLP_SCHEMA_CREATE_IF_NOT_EXISTS, False
        )
        self._add_corrupt_record = properties.get(YETL_TBLP_SCHEMA_CORRUPT_RECORD, True)

        self.metadata_filepath_filename = properties.get(
            YETL_TBLP_METADATA_FILEPATH_FILENAME, False
        )
        self.metadata_filepath = properties.get(YETL_TBLP_METADATA_FILEPATH, False)
        self.metadata_filename = properties.get(YETL_TBLP_METADATA_FILENAME, False)

        self._metadata_context_id_enabled = properties.get(
            YETL_TBLP_METADATA_CONTEXT_ID, False
        )
        self._metadata_dataflow_id_enabled = properties.get(
            YETL_TBLP_METADATA_DATAFLOW_ID, False
        )
        self._metadata_dataset_id_enabled = properties.get(
            YETL_TBLP_METADATA_DATASET_ID, False
        )

    def _get_thresholds(self, config: dict, level: ThresholdLevels):
        thresholds: dict = config.get("thresholds")
        if thresholds:
            return thresholds.get(level.value)

    def _validate_configuration(self):

        if not self.has_badrecordspath_configured and not self.mode:
            self.context.log.warning(
                f"{BAD_RECORDS_PATH} and {MODE} option are not set, defaulting to {MODE}={FAIL_FAST} {CONTEXT_ID}={str(self.context_id)}"
            )
            self.options[MODE] = FAIL_FAST

        self.has_corrupt_column = self._is_corrupt_column_set(self.options, self.schema)

        if self.has_badrecordspath_configured and self.mode:
            msg = f"{BAD_RECORDS_PATH} and {MODE} option are both set, this is not supported. Configure either {BAD_RECORDS_PATH} or {MODE} {CONTEXT_ID}={str(self.context_id)}"
            self.context.log.error(msg)
            raise Exception(msg)

        if self.has_badrecordspath_configured and not self.has_schema:
            msg = f"No schema has been defined. {BAD_RECORDS_PATH} requires that a schema is defined. {CONTEXT_ID}={str(self.context_id)}"
            self.context.log.error(msg)
            raise Exception(msg)

        if self.has_badrecordspath_configured and not self.has_exception_configured:
            msg = f"{BAD_RECORDS_PATH} requires that {EXCEPTIONS} configuration is defined. {CONTEXT_ID}={str(self.context_id)}"
            self.context.log.error(msg)
            raise Exception(msg)

        if self.has_badrecordspath_configured and not self.has_corrupt_column:
            msg = f"{BAD_RECORDS_PATH} doesn't support the use of the _corrupt_record columnd. {CONTEXT_ID}={str(self.context_id)}"
            self.context.log.error(msg)
            raise Exception(msg)

        if self.mode and not self.has_schema:
            msg = f"No schema has been defined. {MODE}={self.mode} configuration requires that a schema is defined. {CONTEXT_ID}={str(self.context_id)}"
            self.context.log.error(msg)
            raise Exception(msg)

        if self.mode.lower() != PERMISSIVE and self.has_exception_configured:
            msg = f"{MODE}={self.mode}, exceptions can only be handled on a mode={PERMISSIVE}, {EXCEPTIONS} configuration will be disabled. {CONTEXT_ID}={str(self.context_id)}"
            self.context.log.warning(msg)
            self.has_exception_configured = False

        if (
            self.mode.lower() == PERMISSIVE
            and self.has_exception_configured
            and not self.has_corrupt_column
        ):
            msg = f"If expceptions are configured for {MODE}={self.mode} then _corrupt_record columns must be supplied in the schema. {CONTEXT_ID}={str(self.context_id)}"
            self.context.log.error(msg)
            raise Exception(msg)

    def _on_schema_creation(self):

        self.initial_load = True
        self.has_corrupt_column = False

        if self.has_exception_configured:
            msg = f"Creating inferred schema for {self.database}.{self.table} {EXCEPTIONS} configuration will be ignored"
            self.context.log.warning(msg)
            self.has_exception_configured = False

        if self.has_badrecordspath_configured:
            msg = f"Creating inferred schema for {self.database}.{self.table} {BAD_RECORDS_PATH} configuration will be ingnored"
            self.context.log.warning(msg)
            self.has_badrecordspath_configured = False

        if self.mode:
            msg = f"Creating inferred schema for {self.database}.{self.table} {self.mode} configuration will be ignored"
            self.context.log.warning(msg)
            del self.options[MODE]

        if not self.infer_schema:
            msg = f"Creating inferred schema for {self.database}.{self.table} defaulting {INFER_SCHEMA} to True"
            self.context.log.warning(msg)
            self.options[INFER_SCHEMA] = True

    def _get_table_properties(self, table_config: dict):
        properties = table_config.get(PROPERTIES, {})
        if properties == None:
            properties = {}

        return properties

    def create_or_alter_table(self):

        dl.create_database(self.context, self.exceptions_database)
        table_exists = dl.table_exists(
            self.context, self.exceptions_database, self.exceptions_table
        )
        if table_exists:
            self.context.log.info(
                f"Exception table already exists {self.exceptions_database}.{self.exceptions_table} at {self.exceptions_path} {CONTEXT_ID}={str(self.context_id)}"
            )
            self.initial_load = False
        else:
            start_datetime = datetime.now()
            sql = dl.create_table(
                self.context,
                self.exceptions_database,
                self.exceptions_table,
                self.exceptions_path,
            )
            self.initial_load = True
            self.auditor.dataset_task(self.id, AuditTask.SQL, sql, start_datetime)

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):

        self._initial_load = value

    def _get_schema(self, config: dict):

        self.schema_repo: ISchemaRepo = (
            self.context.schema_repo_factory.get_schema_repo_type(self.context, config)
        )
        schema = self.schema_repo.load_schema(self.database, self.table)
        return schema

    def configure_badrecords_path(
        self, options: dict, datalake_protocol: str, datalake: str
    ):

        bad_records_path = options[BAD_RECORDS_PATH]
        bad_records_path = render_jinja(bad_records_path, self._replacements)
        options[BAD_RECORDS_PATH] = f"{datalake_protocol}{datalake}/{bad_records_path}"

        return options

    def _set_exceptions_attributes(self, dataset: dict):

        exceptions = dataset[EXCEPTIONS]
        self.exceptions_table = render_jinja(exceptions.get(TABLE), self._replacements)
        self.exceptions_database = render_jinja(
            exceptions.get(DATABASE), self._replacements
        )
        exceptions_path = render_jinja(exceptions.get(PATH), self._replacements)

        self.exceptions_database_table = (
            f"{self.exceptions_database}.{self.exceptions_table}"
        )
        self.exceptions_path = f"{self.datalake_protocol}{self.datalake}/{exceptions_path}/{self.exceptions_database}/{self.exceptions_table}"
        self.context.log.debug(
            f"""Jinja rendered exception table configuration:
            database_name: {self.exceptions_database}
            table_name: {self.exceptions_table}
            exception_path: {self.exceptions_path}
        """
        )

    def _is_corrupt_column_set(self, options: dict, schema: StructType):

        if schema:
            has_corrupt_column = (
                options.get(MODE, "").lower() == PERMISSIVE
                and CORRUPT_RECORD in schema.fieldNames()
            )

            if (
                options.get(MODE, "").lower() == PERMISSIVE
                and not CORRUPT_RECORD in schema.fieldNames()
            ):
                self.context.log.warning(
                    f"mode={PERMISSIVE} and corrupt record is not set in the schema, schema on read corrupt records will be silently dropped."
                )
        else:
            has_corrupt_column = False

        return has_corrupt_column

    def _get_validation_exceptions_handler(self):
        def handle_exceptions(exceptions: DataFrame):
            exceptions_count = 0
            if self.has_exception_configured:
                exceptions_count = exceptions.count()
                if exceptions and exceptions_count > 0:
                    options = {MERGE_SCHEMA: True}
                    self.context.log.warning(
                        f"Writing {exceptions_count} exception(s) from {self.database_table} to {self.exceptions_database_table} delta table {CONTEXT_ID}={str(self.context_id)}"
                    )
                    exceptions.write.format(Format.DELTA.value).options(**options).mode(
                        APPEND
                    ).save(self.exceptions_path)
            return exceptions_count

        return handle_exceptions

    def validate(self):

        validation_handler = self._get_validation_exceptions_handler()
        validator = None

        if self.has_badrecordspath_configured:

            bad_records_path = self.options[BAD_RECORDS_PATH]
            self.context.log.info(
                f"Validating dataframe read using badRecordsPath at {bad_records_path} {CONTEXT_ID}={str(self.context_id)}"
            )
            validator = BadRecordsPathSchemaOnRead(
                self.context,
                self.dataframe,
                validation_handler,
                self.database,
                self.table,
                bad_records_path,
                self.context.spark,
                self.thresholds_warnings,
                self.thresholds_error,
            )

        if self.has_corrupt_column:
            self.context.log.info(
                f"Validating dataframe read using PERMISSIVE corrupt column at {CORRUPT_RECORD} {CONTEXT_ID}={str(self.context_id)}"
            )
            validator = PermissiveSchemaOnRead(
                self.context,
                self.dataframe,
                validation_handler,
                self.database,
                self.table,
                self.thresholds_warnings,
                self.thresholds_error,
            )

        if validator:
            start_datetime = datetime.now()
            level_validation, validation = validator.validate()
            self.auditor.dataset_task(
                self.id, AuditTask.SCHEMA_ON_READ_VALIDATION, validation, start_datetime
            )
            self.dataframe = validator.dataframe

    def _add_timeslice(self, df: DataFrame):

        if (
            self._metadata_timeslice_enabled
            == JinjaVariables.TIMESLICE_PATH_DATE_FORMAT
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
            self._metadata_timeslice_enabled
            == JinjaVariables.TIMESLICE_FILE_DATE_FORMAT
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

    def _add_source_metadata(self, df: DataFrame):

        if self.metadata_filepath_filename:
            df: DataFrame = df.withColumn(FILEPATH_FILENAME, fn.input_file_name())

        if self.metadata_filepath:
            df: DataFrame = df.withColumn(FILEPATH, fn.input_file_name()).withColumn(
                FILEPATH,
                fn.expr(
                    f"replace({FILEPATH}, concat('/', substring_index({FILEPATH}, '/', -1)))"
                ),
            )

        if self.metadata_filename:
            df: DataFrame = df.withColumn(FILENAME, fn.input_file_name()).withColumn(
                FILENAME, fn.substring_index(fn.col(FILENAME), "/", -1)
            )

        if self._metadata_context_id_enabled:
            df: DataFrame = df.withColumn(CONTEXT_ID, fn.lit(self.context_id))

        if self._metadata_dataflow_id_enabled:
            df: DataFrame = df.withColumn(DATAFLOW_ID, fn.lit(self.dataflow_id))

        if self._metadata_dataset_id_enabled:
            df: DataFrame = df.withColumn(DATASET_ID, fn.lit(self.id))

        return df

    def read(self):
        self.context.log.info(
            f"Reading data for {self.database_table} from {self.path} with options {self.options} {CONTEXT_ID}={str(self.context_id)}"
        )

        self.context.log.debug(json.dumps(self.options, indent=4, default=str))

        start_datetime = datetime.now()

        df: DataFrame = self.context.spark.read.format(self.format)

        if self.has_schema:
            df = df.schema(self.schema)

        df = (
            df.options(**self.options)
            .load(self.path)
        )

        # if there isn't a schema and it's configured to create one the save it to repo.
        if self._creating_inferred_schema:
            self.context.log.info(
                f"Saving inferred schema for {self.database}.{self.table} into schema repository. {CONTEXT_ID}={str(self.context_id)}"
            )
            self.schema = df.schema
            if self._add_corrupt_record:
                self.schema.add(CORRUPT_RECORD, StringType(), nullable=True)
            self.schema_repo.save_schema(self.schema, self.database, self.table)

        # add metadata after schema is created since we don't want these 
        # derived columns in the read metadata, only the _corrupt_column if present
        df = self._add_timeslice(df)
        df = self._add_source_metadata(df)

        self.context.log.debug(
            f"Reordering sys_columns to end for {self.database_table} from {self.path}. {CONTEXT_ID}={str(self.context_id)}"
        )
        self.dataframe = df
        detail = {"path": self.path, "options": self.options}
        self.auditor.dataset_task(self.id, AuditTask.LAZY_READ, detail, start_datetime)
        self.validation_result = self.validate()

        return self.dataframe
