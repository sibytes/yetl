# from multiprocessing import context
from ..parser._constants import *

from ..schema_repo import SchemaNotFound

from .. import _delta_lake as dl
from pyspark.sql import DataFrame
import uuid
from ..save import Save, save_factory

# from typing import ChainMap
# from ..parser import parser

from ..audit import Audit, AuditTask

# from datetime import datetime
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


class Write(BaseModel):

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._merge_schema = self.options.get("merge_schema", False)
        self._init_mode(self.mode)


    _DEFAULT_OPTIONS = {"mergeSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)
    mode: Union[SaveModeOptions, dict] = Field(default=None)
    dataset:Destination = Field(default=None)

    _save: Save = PrivateAttr(default=None)
    _merge_schema: bool = PrivateAttr(default=False)
    _mode_options: dict = PrivateAttr(default=None)

    @property
    def merge_schema(self) -> bool:
        return self.merge_schema

    @merge_schema.setter
    def merge_schema(self, value: bool):
        self.options[MERGE_SCHEMA] = value
        self._merge_schema = value

    def _init_mode(self, mode:Union[SaveModeOptions, dict]):

        if isinstance(mode, dict):
            mode_value = next(iter(mode))
            self.mode = SaveModeOptions(mode_value)
            self._mode_options = mode.get(mode_value)

        else:
            self.mode = mode
            self._mode_options = None

    def set_dataset_save(self, destination:Destination, mode_options:dict=None):

        if mode_options:
            self._mode_options = mode_options
            
        self.dataset = destination
        self._save = save_factory.get_save_type(dataset=self.dataset, options=self._mode_options)

    @property
    def save(self) -> Save:
        return self._save

    @save.setter
    def save(self, value:Save):
        self.save = value

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
        self.datalake_protocol = self.context.datalake_protocol
        self.render()
        self.datalake = self.context.datalake
        self.context_id = self.context.context_id
        self.auditor.dataset(self.get_metadata())
        self._init_task_read_schema()
        self._init_partitions()

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
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
            JinjaVariables.ROOT: f"{self.datalake_protocol.value}{self.datalake}",
        }
        # if the path has no root {{root}} prefixed then add one
        path = prefix_root_var(self.path)
        self.path = render_jinja(path, self._replacements)

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
        # when it's the initial load and schema's aren't declared
        # the delta table is created 1st with no schema and the
        # data schema loads into it. To do this we override the
        # merge schema options so the data schema is merged in
        # on the 1st load without requiring changes to pipeline.
        if not self.write.merge_schema and value:
            self.write.merge_schema = value
        self._initial_load = value

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        # json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
