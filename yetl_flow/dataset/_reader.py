from ._source import Source
from pyspark.sql import functions as fn
from pyspark.sql.types import StructType
from ._constants import *
from . import _builtin_functions as builtin_funcs
from ..schema_repo import ISchemaRepo
from ._validation import PermissiveSchemaOnRead, BadRecordsPathSchemaOnRead
from pyspark.sql import DataFrame
import json
from .. import _delta_lake as dl


class Reader(Source):
    def __init__(
        self, context, database: str, table: str, config: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, config, io_type)

        # try and load a schema if schema on read
        self.schema = self._get_schema(config["spark_schema_repo"])

        # gets the read, write, etc options based on the type
        io_properties = config.get(io_type)

        self.auto_io = io_properties.get(AUTO_IO, True)
        self.options: dict = io_properties.get(OPTIONS)
        # set record exception handling state.
        # The following state routes schema row excpetions out into an exception delta table:
        # - mode=PERMISSIVE and _currupt_columns in the schema
        # - if badrecordpath set
        self.has_corrupt_column = self._is_corrupt_column_set(self.options, self.schema)
        self.has_bad_records_path, self.options = self._is_bad_records_path_set(
            self.options, self.datalake_protocol, self.datalake
        )

        if not self.has_bad_records_path and not self.options.get(MODE):
            self.context.log.warning(
                f"badRecordsPath and mode option are not set, default to mode={PERMISSIVE} {CORRELATION_ID}={str(self.correlation_id)}"
            )
            self.options[MODE] = PERMISSIVE

        self._set_has_exceptions_table(config)

        self.database_table = f"{self.database}.{self.table}"

        self._initial_load = super().initial_load

        if self.auto_io:
            self.context.log.info(
                f"auto_io = {self.auto_io} automatically creating or altering exception delta table {self.database}.{self.table} {CORRELATION_ID}={str(self.correlation_id)}"
            )
            self.create_or_alter_table()

    def create_or_alter_table(self):

        dl.create_database(self.context, self.exceptions_database)
        table_exists = dl.table_exists(
            self.context, self.exceptions_database, self.exceptions_table
        )
        if table_exists:
            self.context.log.info(
                f"Exception table already exists {self.exceptions_database}.{self.exceptions_table} at {self.exceptions_path} {CORRELATION_ID}={str(self.correlation_id)}"
            )
            self.initial_load = False
        else:
            dl.create_table(
                self.context,
                self.exceptions_database,
                self.exceptions_table,
                self.exceptions_path,
            )
            self.initial_load = True

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
        return self.schema_repo.load_schema(self.database, self.table)

    def _is_bad_records_path_set(
        self, options: dict, datalake_protocol: str, datalake: str
    ):

        has_bad_records_path = BAD_RECORDS_PATH in options

        if self.has_corrupt_column and has_bad_records_path:
            has_bad_records_path = False
            self.context.log.warning(
                f"badRecordsPath and mode option is not supported together, default to mode={self.options.get(MODE)} {CORRELATION_ID}={str(self.correlation_id)}"
            )

        elif has_bad_records_path:
            bad_records_path = options[BAD_RECORDS_PATH]
            bad_records_path = builtin_funcs.execute_replacements(
                bad_records_path, self
            )
            options[
                BAD_RECORDS_PATH
            ] = f"{datalake_protocol}{datalake}/{bad_records_path}"

        return has_bad_records_path, options

    def _set_has_exceptions_table(self, dataset: dict):
        self.has_exceptions_table = EXCEPTIONS in dataset
        if self.has_exceptions_table:
            exceptions = dataset[EXCEPTIONS]
            self.exceptions_table = exceptions.get(TABLE)
            self.exceptions_database = exceptions.get(DATABASE)
            self.exceptions_database_table = (
                f"{self.exceptions_database}.{self.exceptions_table}"
            )
            exceptions_path = exceptions.get(PATH)
            self.exceptions_path = f"{self.datalake_protocol}{self.datalake}/{exceptions_path}/{self.exceptions_database}/{self.exceptions_table}"

    def _is_corrupt_column_set(self, options: dict, schema: StructType):

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

        return has_corrupt_column

    def _get_validation_exceptions_handler(self):
        def handle_exceptions(exceptions: DataFrame):
            exceptions_count = 0
            if self.has_exceptions_table:
                exceptions_count = exceptions.count()
                if exceptions and exceptions_count > 0:
                    options = {MERGE_SCHEMA: True}
                    self.context.log.warning(
                        f"Writing {exceptions_count} exception(s) from {self.database_table} to {self.exceptions_database_table} delta table {CORRELATION_ID}={str(self.correlation_id)}"
                    )
                    exceptions.write.format(Format.DELTA.value).options(**options).mode(
                        APPEND
                    ).save(self.exceptions_path)
            return exceptions_count

        return handle_exceptions

    def validate(self):

        validation_handler = self._get_validation_exceptions_handler()
        validator = None

        if self.has_bad_records_path:

            bad_records_path = self.options[BAD_RECORDS_PATH]
            self.context.log.info(
                f"Validating dataframe read using badRecordsPath at {bad_records_path} {CORRELATION_ID}={str(self.correlation_id)}"
            )
            validator = BadRecordsPathSchemaOnRead(
                self.dataframe,
                validation_handler,
                self.database,
                self.table,
                bad_records_path,
                self.context.spark,
            )

        if self.has_corrupt_column:
            self.context.log.info(
                f"Validating dataframe read using PERMISSIVE corrupt column at {CORRUPT_RECORD} {CORRELATION_ID}={str(self.correlation_id)}"
            )
            validator = PermissiveSchemaOnRead(
                self.dataframe, validation_handler, self.database, self.table
            )
            self.dataframe = self.dataframe.drop(CORRUPT_RECORD)

        if validator:
            validation = validator.validate()
            self.dataframe = validator.dataframe
            self.context.log.info(json.dumps(validation, indent=4, default=str))

    def read(self):
        self.context.log.info(
            f"Reading data for {self.database_table} from {self.path} with options {self.options} {CORRELATION_ID}={str(self.correlation_id)}"
        )

        self.context.log.debug(json.dumps(self.options, indent=4, default=str))

        df = (
            self.context.spark.read.format(self.format)
            .schema(self.schema)
            .options(**self.options)
            .load(self.path)
            .withColumn(CORRELATION_ID, fn.lit(str(self.correlation_id)))
            .withColumn(LOAD_TIMESTAMP, fn.current_timestamp())
            # .withColumn(TIMESLICE, fn.lit(self.timeslice)) # TODO: fix injection of time slice lineage.
            .withColumn(FILENAME, fn.input_file_name())
        )

        self.dataframe = df
        self.validation_result = self.validate()
        return self.dataframe
