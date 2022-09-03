from ._source import Source
from pyspark.sql import functions as fn
from pyspark.sql.types import StructType
from ..parser._constants import *
from . import _builtin_functions as builtin_funcs
from ..schema_repo import ISchemaRepo, SchemaNotFound
from ._validation import PermissiveSchemaOnRead, BadRecordsPathSchemaOnRead
from pyspark.sql import DataFrame
import json
from .. import _delta_lake as dl
from ..parser import parser


class Reader(Source):
    def __init__(
        self, context, database: str, table: str, config: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, config, io_type)

        # get the table properties
        properties: dict = self._get_table_properties(config["table"])

        self._metadata_lineage_enabled = properties.get(
            YETL_TBLP_METADATA_LINEAGE_ENABLED, False
        )
        self._create_schema_if_not_exists = properties.get(
            YETL_TBLP_SCHEMA_CREATE_IF_NOT_EXISTS, False
        )
        self._metadata_timeslice_tabled = properties.get(
            YETL_TBLP_METADATA_TIMESLICE, True
        )
        # try and load a schema if schema on read
        self.schema = self._get_schema(config["spark_schema_repo"])

        # gets the read, write, etc options based on the type
        io_properties = config.get(io_type)

        self.auto_io = io_properties.get(AUTO_IO, True)
        self.options: dict = io_properties.get(OPTIONS)

        # if we don't have a schema and set to create if not exists then set infer schema option to true
        # so that we can infer the schema and use it create the schema file.
        if not self.schema and self._create_schema_if_not_exists:
            self.options["inferSchema"] = True

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
                f"badRecordsPath and mode option are not set, default to mode={PERMISSIVE} {CONTEXT_ID}={str(self.context_id)}"
            )
            self.options[MODE] = PERMISSIVE

        self._set_has_exceptions_table(config)

        self.database_table = f"{self.database}.{self.table}"

        self._initial_load = super().initial_load

        if self.auto_io:
            self.context.log.info(
                f"auto_io = {self.auto_io} automatically creating or altering exception delta table {self.database}.{self.table} {CONTEXT_ID}={str(self.context_id)}"
            )
            self.create_or_alter_table()

    def _get_table_properties(self, table_config: dict):
        return table_config.get(PROPERTIES, {})

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
        try:
            schema = self.schema_repo.load_schema(self.database, self.table)
        except SchemaNotFound as e:
            msg = f"schema not for {self.database}.{self.table} as path {e}"
            self.context.log.warning(msg)
            schema = None

        return schema

    def _save_schema(self, schema: StructType):
        schema = self.schema_repo.save_schema(schema, self.database, self.table)

    def _is_bad_records_path_set(
        self, options: dict, datalake_protocol: str, datalake: str
    ):

        has_bad_records_path = BAD_RECORDS_PATH in options

        if self.has_corrupt_column and has_bad_records_path:
            has_bad_records_path = False
            self.context.log.warning(
                f"badRecordsPath and mode option is not supported together, default to mode={self.options.get(MODE)} {CONTEXT_ID}={str(self.context_id)}"
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

        if self.has_bad_records_path:

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
            )

        if validator:
            validation = validator.validate()
            self.dataframe = validator.dataframe
            self.context.log.info(json.dumps(validation, indent=4, default=str))

    def read(self):
        self.context.log.info(
            f"Reading data for {self.database_table} from {self.path} with options {self.options} {CONTEXT_ID}={str(self.context_id)}"
        )

        self.context.log.debug(json.dumps(self.options, indent=4, default=str))

        input_file_name = f"_path_{self.database}_{self.table}"
        df: DataFrame = (
            self.context.spark.read.format(self.format)
            .schema(self.schema)
            .options(**self.options)
            .load(self.path)
            .withColumn(CONTEXT_ID, fn.lit(str(self.context_id)))
        )

        if self._metadata_timeslice_tabled:
            df: DataFrame = (
                df
                # TODO .withColumn(input_file_name, fn.input_file_name())
                .withColumn(TIMESLICE, fn.input_file_name())
                .withColumn(
                    TIMESLICE,
                    fn.substring(
                        TIMESLICE,
                        self._timeslice_position.start_pos,
                        self._timeslice_position.length,
                    ),
                )
                .withColumn(
                    TIMESLICE,
                    fn.to_timestamp(TIMESLICE, self._timeslice_position.format_code),
                )
            )

        # if there isn't a schema and it's configured to create one the save it to repo.
        if self._create_schema_if_not_exists and not self.schema:
            self.schema = df.schema
            self.schema_repo.save_schema(self.schema, self.database, self.table)

        self.context.log.debug(
            f"Reordering sys_columns to end for {self.database_table} from {self.path} {CONTEXT_ID}={str(self.context_id)}"
        )
        self.dataframe = df
        self.validation_result = self.validate()
        self.save_metadata()
        return self.dataframe

    def save_metadata(self):
        super().save_metadata()
