# from multiprocessing import context
from ..parser._constants import *

from ..schema_repo import SchemaNotFound

from .. import _delta_lake as dl
from pyspark.sql import DataFrame
import uuid

# from typing import ChainMap
# from ..parser import parser
# from ..save import save_factory, Save
from ..audit import Audit, AuditTask

# from datetime import datetime
from .._timeslice import Timeslice, TimesliceUtcNow
from pyspark.sql import functions as fn
import json
from ._base import Destination, SQLTable
from pydantic import Field, PrivateAttr, BaseModel
from typing import Any, Dict, List
from ..parser.parser import JinjaVariables, render_jinja
from ._properties import DeltaWriterProperties
from ..save._save_mode_type import SaveModeType
from ..file_system import FileSystemType
from ..context import SparkContext


class Write(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._merge_schema = self.options.get("merge_schema", False)

    _DEFAULT_OPTIONS = {"mergeSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)
    mode: SaveModeType = Field(default=SaveModeType.APPEND)
    _merge_schema: bool = PrivateAttr(default=False)

    @property
    def merge_schema(self) -> bool:
        return self.merge_schema

    @merge_schema.setter
    def merge_schema(self, value: bool):
        self.options[MERGE_SCHEMA] = value
        self._merge_schema = value


class DeltaWriter(Destination, SQLTable):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self.auditor = self.context.auditor
        self.timeslice = self.context.timeslice
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
        }
        self.datalake_protocol = self.context.datalake_protocol
        self.datalake = self.context.datalake
        path = f"{self.datalake_protocol.value}{self.datalake}/{self.path}"
        self.path = render_jinja(path, self._replacements)
        self.context_id = self.context.context_id
        self.auditor.dataset(self.get_metadata())
        self._init_task_read_schema()

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
    write: Write = Field(default=Write())
    _initial_load: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    _create_spark_schema = PrivateAttr(default=False)

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

    def validate(self):
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
