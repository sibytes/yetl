from multiprocessing import context
from ..parser._constants import *
from ..schema_repo import ISchemaRepo, SchemaNotFound
from .. import _delta_lake as dl
from pyspark.sql import DataFrame
from typing import ChainMap
from ..parser import parser
from ..save import save_factory, Save
from ..audit import Audit, AuditTask
from datetime import datetime
from pyspark.sql import functions as fn
import json
from ._base import Destination
from pydantic import Field, PrivateAttr
from typing import Any, Dict
from ..parser.parser import JinjaVariables, render_jinja
from ._properties import DeltaWriterProperties

class DeltaWriter(Destination):

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table
        }
        path = f"{self.datalake_protocol.value}{self.datalake}/{self.path}"
        self.path = render_jinja(path, self._replacements)

    timeslice: Timeslice = Field(default=Timeslice(year="*"))
    context_id: uuid.UUID
    dataflow_id: uuid.UUID
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    datalake_protocol: DatalakeProtocolOptions = Field(
        default=DatalakeProtocolOptions.FILE
    )
    datalake: str = Field(...)
    database: str = Field(...)
    table: str = Field(...)
    yetl_properties: DeltaWriterProperties = Field(
        default=DeltaWriterProperties(), alias="properties"
    )
    format: FormatOptions = Field(default=FormatOptions.DELTA)
    path: str = Field(...)
    _initial_load: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)

    def validate(self):
        pass

    def execute(self):
        pass

    @property
    def sql_database_table(self, sep: str = ".", qualifier: str = "`") -> str:
        "Concatenated fully qualified database table for SQL"
        return f"{qualifier}{self.database}{qualifier}{sep}{qualifier}{self.table}{qualifier}"

    @property
    def database_table(self, sep: str = ".") -> str:
        "Concatenated database table for readability"
        return f"{self.database}{sep}{self.table}"



    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):

        if not self.options[MERGE_SCHEMA] and value:
            self.options[MERGE_SCHEMA] = value
        self._initial_load = value
