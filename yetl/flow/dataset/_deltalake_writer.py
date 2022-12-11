# from multiprocessing import context
from ..parser._constants import *
from ..schema_repo import SchemaNotFound
from .. import _delta_lake as dl
from pyspark.sql import DataFrame
import uuid
from ..save import Save, save_factory
from typing import ChainMap
from ..audit import Audit, AuditTask
from datetime import datetime
from .._timeslice import Timeslice, TimesliceUtcNow
from pyspark.sql import functions as fn
import json
from ._base import Destination, SQLTable
from pydantic import Field, PrivateAttr, BaseModel
from typing import Any, Dict, List, Union
from ..parser.parser import (
    JinjaVariables,
    render_jinja,
    sql_partitioned_by,
    prefix_root_var,
)
from ._properties import DeltaWriterProperties
from ..save._save_mode_type import SaveModeOptions
from ..file_system import FileSystemType
from ..context import SparkContext
from ..schema_repo import SchemaNotFound


class Write(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._merge_schema = self.options.get("merge_schema", False)
        self._init_mode(self.mode)

    _DEFAULT_OPTIONS = {"mergeSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)
    mode: Union[SaveModeOptions, dict] = Field(default=None)
    dataset: Destination = Field(default=None)

    _save: Save = PrivateAttr(default=None)
    _merge_schema: bool = PrivateAttr(default=False)
    _mode_options: dict = PrivateAttr(default=None)

    def get_merge_schema(self) -> bool:
        return self._merge_schema

    def set_merge_schema(self, value: bool):
        self.options[MERGE_SCHEMA] = value
        self._merge_schema = value

    def _init_mode(self, mode: Union[SaveModeOptions, dict]):

        if isinstance(mode, dict):
            mode_value = next(iter(mode))
            self.mode = SaveModeOptions(mode_value)
            self._mode_options = mode.get(mode_value)

        else:
            self.mode = mode
            self._mode_options = None

    def set_dataset_save(self, destination: Destination, mode_options: dict = None):

        if mode_options:
            self._mode_options = mode_options

        self.dataset = destination
        self._save = save_factory.get_save_type(
            dataset=self.dataset, options=self._mode_options
        )

    def get_save(self) -> Save:
        return self._save

    def set_save(self, value: Save):
        self._save = value

    class Config:
        arbitrary_types_allowed = True


class DeltaWriter(Destination, SQLTable):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self.write.set_dataset_save(self)
        self.auditor = self.context.auditor
        self.timeslice = self.context.timeslice
        self.datalake = self.context.datalake
        self.datalake_protocol = self.context.datalake_protocol
        self.render()
        self.datalake = self.context.datalake
        self.context_id = self.context.context_id
        self.auditor.dataset(self.get_metadata())
        self._init_task_read_schema()
        self._init_partitions()
        if self.auto_write and self.ddl:
            self.create_or_alter_table()

    context: SparkContext = Field(...)
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=None)
    dataflow_id: uuid.UUID = Field(default=None)
    datalake_protocol: FileSystemType = Field(default=None)
    datalake: str = Field(default=None)
    auditor: Audit = Field(default=None)

    catalog: str = Field(None)
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    ddl: str = Field(default=None)
    yetl_properties: DeltaWriterProperties = Field(
        default=DeltaWriterProperties(), alias="properties"
    )
    deltalake_properties: dict = Field(default={})
    format: FormatOptions = Field(default=FormatOptions.DELTA)
    path: str = Field(...)
    check_constraints: Dict[str, str] = Field(default=None)
    partitioned_by: List[str] = Field(default=None)
    zorder_by: List[str] = Field(default=None)
    write: Write = Field(...)

    _initial_load: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    _create_spark_schema = PrivateAttr(default=False)

    def render(self):
        if self.datalake is None:
            raise Exception("datalake root path cannot be None")

        if self.datalake_protocol is None:
            raise Exception("datalake protocol cannot be None")

        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
            JinjaVariables.ROOT: f"{self.datalake_protocol.value}{self.datalake}",
        }
        # if the path has no root {{root}} prefixed then add one
        path = prefix_root_var(self.path)
        self.path = render_jinja(path, self._replacements)
        self._replacements[JinjaVariables.PATH] = self.path

    def _init_task_read_schema(self):
        # if table ddl not defined in the config
        # or it is but it's not a SQL statement it self.
        # We just look to see if it's multi-line at the moment
        # and let spark handle the parsing of whether it's SQL or
        # not. The assumption is that the paths will be single line
        # and SQL will be multiline.
        if (not self.ddl) or (not "\n" in self.ddl):
            try:
                self.ddl = self.context.deltalake_schema_repository.load_schema(
                    database=self.database, table=self.table, sub_location=self.ddl
                )
            except SchemaNotFound as e:
                # currently we're forcing the creation or management of delta lake schema
                # this is somewhat opinionated since we could just load a table off the
                # data and not create a schema to manage. Currently we don't allow this
                # in the spirit of best practice.
                if self.yetl_properties.schema_create_if_not_exists:
                    self._create_spark_schema = True

                elif not self._infer_schema:
                    raise e

    def _init_partitions(self):
        """Parse the partitioned columns from the SQL schema ddl
        if they are defined in the SQL it will overide what is in the yaml configuration.
        Otherwise they are taken from the configuration. If they are not defined at all
        the field is already defaulted to None"""

        partitions = None
        if self.ddl:
            try:
                partitions: List[str] = sql_partitioned_by(self.ddl)
                msg = f"Parsed partitioning columns from sql ddl for {self.database_table} as {partitions}"
                self.context.log.info(msg)
            except Exception as e:
                msg = f"An error has occured parsing sql ddl partitioned clause for {self.database_table} for the ddl: {self.ddl}"
                self.context.log.error(msg)
                raise Exception(msg) from e

        if partitions:
            self.partitioned_by = partitions
            msg = f"Parsed partitioning columns from dataflow yaml config for {self.database}.{self.table} as {partitions}"
            self.context.log.info(msg)

    def _get_table_properties_sql(self, existing_properties: dict = None):

        tbl_properties = self.deltalake_properties
        if tbl_properties:
            if existing_properties:
                tbl_properties = dict(ChainMap(tbl_properties, existing_properties))
            tbl_properties = [f"'{k}' = '{v}'" for k, v in tbl_properties.items()]
            tbl_properties = ", ".join(tbl_properties)
            tbl_properties = dl.alter_table_set_tblproperties(
                self.database, self.table, tbl_properties
            )
            return tbl_properties
        else:
            return None

    def _set_delta_table_properties(self, existing_properties: dict):
        _existing_properties = {}
        if existing_properties:
            _existing_properties = existing_properties.get(self.database_table)
            _existing_properties = _existing_properties.get(PROPERTIES)
        tbl_properties_ddl = self._get_table_properties_sql(_existing_properties)
        self.context.log.debug(
            f"DeltaWriter table properties ddl = {tbl_properties_ddl}"
        )
        if tbl_properties_ddl:
            start_datetime = datetime.now()
            self.context.spark.sql(tbl_properties_ddl)
            self.auditor.dataset_task(
                self.dataset_id,
                AuditTask.SET_TABLE_PROPERTIES,
                tbl_properties_ddl,
                start_datetime,
            )

    def _get_check_constraints_sql(self, existing_constraints: dict = None):

        sql_constraints = []

        # if the existing constraint is not defined in the config constraints
        # and it is different then drop it and recreate
        if existing_constraints:
            for name, existing_constraint in existing_constraints.items():
                defined_constraint = self.check_constraints.get(name)

                # if the existing constraint is not defined in the config constraints then drop it
                if not defined_constraint:
                    sql_constraints.append(
                        dl.alter_table_drop_constraint(self.database, self.table, name)
                    )
                # if the existing constraint is defined and it is different then drop and add it
                elif (
                    defined_constraint.replace(" ", "").lower()
                    != existing_constraint.replace(" ", "").lower()
                ):
                    sql_constraints.append(
                        dl.alter_table_drop_constraint(self.database, self.table, name)
                    )
                    sql_constraints.append(
                        dl.alter_table_add_constraint(
                            self.database, self.table, name, defined_constraint
                        )
                    )

        # the constraint is defined but doesn't exist on the table yet so
        # add the constraint
        if self.check_constraints:
            for name, defined_constraint in self.check_constraints.items():
                existing_constraint = existing_constraints.get(name)
                if not existing_constraint:
                    sql_constraints.append(
                        dl.alter_table_add_constraint(
                            self.database, self.table, name, defined_constraint
                        )
                    )

        return sql_constraints

    def _set_table_constraints(self, table_properties: dict):

        existing_constraints = {}
        if table_properties:
            existing_constraints = table_properties.get(self.database_table)
            existing_constraints = existing_constraints.get("constraints")

        column_constraints_ddl = self._get_check_constraints_sql(
            existing_constraints
        )
        self.context.log.debug(
            f"Writer table check constraints ddl = {column_constraints_ddl}"
        )
        if self.ddl or not self.initial_load:
            # can only add constraints to columns if there are any
            # if there is no table_ddl an empty table is created and the data schema defines the table
            # on the initial load so this is skipped on the 1st load.
            if column_constraints_ddl:
                start_datetime = datetime.now()
                for cc in column_constraints_ddl:
                    self.context.spark.sql(cc)
                self.auditor.dataset_task(
                    self.dataset_id,
                    AuditTask.SET_TABLE_PROPERTIES,
                    column_constraints_ddl,
                    start_datetime,
                )

    def create_or_alter_table(self):

        current_properties = None
        start_datetime = datetime.now()
        detail = dl.create_database(self.context, self.database)
        self.auditor.dataset_task(
            self.dataset_id, AuditTask.SQL, detail, start_datetime
        )

        table_exists = dl.table_exists(self.context, self.database, self.table)
        if table_exists:
            self.context.log.info(
                f"Table already exists {self.database_table} at {self.path}"
            )
            self._set_initial_load(False)

            start_datetime = datetime.now()
            # on non initial loads get the constraints and properties
            # to them to and sync with the declared constraints and properties.
            # TODO: consolidate details and properties fetch since the properties are in the details. The delta lake api may have some improvements.
            current_properties = dl.get_table_properties(
                self.context, self.database, self.table
            )
            details = dl.get_table_details(self.context, self.database, self.table)
            # get the partitions from the table details and add them to the properties.
            current_properties[self.database_table][PARTITIONS] = details[
                self.database_table
            ][PARTITIONS]
            self.auditor.dataset_task(
                self.dataset_id,
                AuditTask.GET_TABLE_PROPERTIES,
                current_properties,
                start_datetime,
            )

        else:
            start_datetime = datetime.now()
            rendered_table_ddl = self.ddl
            if self.ddl:
                rendered_table_ddl = render_jinja(self.ddl, self._replacements)
            detail = dl.create_table(
                self.context, self.database, self.table, self.path, rendered_table_ddl
            )
            self.auditor.dataset_task(
                self.dataset_id, AuditTask.SQL, detail, start_datetime
            )
            self._set_initial_load(True)

        # alter, drop or create any constraints defined that are not on the table
        self._set_table_constraints(current_properties)
        # alter, drop or create any properties that are not on the table
        self._set_delta_table_properties(current_properties)

    def verify(self):
        pass

    def execute(self):
        pass

    @property
    def has_partitions(self) -> bool:
        if self.partitioned_by:
            return True
        else:
            return False

    @property
    def has_check_constaints(self) -> bool:
        if self.check_constraints:
            return True
        else:
            return False

    @property
    def has_zorder_by(self) -> bool:
        if self.zorder_by:
            return True
        else:
            return False

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        self._set_initial_load(value)

    def _set_initial_load(self, value: bool):
        # when it's the initial load and schema's aren't declared
        # the delta table is created 1st with no schema and the
        # data schema loads into it. To do this we override the
        # merge schema options so the data schema is merged in
        # on the 1st load without requiring changes to pipeline.
        if not self.write.get_merge_schema() and value:
            self.write.set_merge_schema(value)
        self._initial_load = value

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        # json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
