from ._base import Source, SQLTable
from ._decoder import parse_properties_key, parse_properties_values
from pyspark.sql import DataFrame
from ._properties import SqlReaderProperties
from pydantic import Field, PrivateAttr, BaseModel
from pyspark.sql import DataFrame
import uuid
from ..file_system import FileSystemType
from ..context import SparkContext
from .._timeslice import Timeslice, TimesliceUtcNow
from ..parser.parser import JinjaVariables
from ..parser._constants import CONTEXT_ID, DATAFLOW_ID, DATASET_ID
from ..parser._constants import FormatOptions
from typing import Dict, Any
from ..audit import Audit, AuditTask
import json
from ..audit import Audit
from datetime import datetime
from pyspark.sql import functions as fn
import logging


def _yetl_properties_dumps(obj: dict, *, default):
    """Decodes the data back into a dictionary with yetl configuration properties names"""
    obj = {
        parse_properties_key(k): parse_properties_values(k, v) for k, v in obj.items()
    }
    return json.dumps(obj, default=default)


class Read(BaseModel):
    auto: bool = Field(default=True)


class SQLReader(Source, SQLTable):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self.initialise()

    def initialise(self):
        self.auditor = self.context.auditor
        self.timeslice = self.context.timeslice
        self.datalake_protocol = self.context.datalake_protocol
        self.datalake = self.context.datalake
        self.render()
        self.context_id = self.context.context_id
        self.auditor.dataset(self.get_metadata())
        self._init_task_read_schema()

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

    def _init_task_read_schema(self):

        if (not self.sql) or (not "\n" in self.sql) or (self.sql_uri):
            if not self.sql_uri:
                self.sql_uri = self.sql
            self.sql = self.context.pipeline_repository.load_pipeline_sql(
                self.database, self.table, self.sql_uri
            )

        if self.yetl_properties.create_as_view:
            name = (
                f"{self.catalog}.{self.sql_database_table}"
                if self.catalog
                else self.sql_database_table
            )
            ddl = f"create or replace view {name}\n"
            sql = f"{sql}{ddl}"

    context: SparkContext = Field(...)
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=None)
    datalake_protocol: FileSystemType = Field(default=None)
    datalake: str = Field(default=None)
    auditor: Audit = Field(default=None)
    sql_uri: str = Field(default=None)
    catalog: str = Field(None)
    dataframe: DataFrame = Field(default=None)
    dataset_id: uuid.UUID = Field(default=uuid.uuid4())
    dataflow_id: uuid.UUID = Field(default=None)
    sql: str = Field(...)
    yetl_properties: SqlReaderProperties = Field(
        default=SqlReaderProperties(), alias="properties"
    )
    timeslice_format: str = Field(default="%Y%m%d")
    format: FormatOptions = Field(default=FormatOptions.DELTA)
    read: Read = Field(default=Read())
    _initial_load: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)

    def verify(self):
        pass

    def execute(self):
        self._logger.debug(
            f"Reading data for {self.database_table} with query {self.sql} {CONTEXT_ID}={str(self.context_id)}"
        )

        start_datetime = datetime.now()

        self.dataframe = self.context.spark.sql(self.sql)
        self.dataframe = self._add_source_metadata(self.dataframe)

        detail = {"table": self.database_table, "sql": self.sql}
        self.auditor.dataset_task(
            self.dataset_id, AuditTask.LAZY_READ, detail, start_datetime
        )

        return self.dataframe

    def _add_df_metadata(self, column: str, value: str, df: DataFrame):

        if column in df.columns:
            # We have to drop the column first if it exists since it may have been added
            # to incoming dataframe specific to source dataset
            df = df.drop(column)
        df = df.withColumn(column, fn.lit(value))
        return df

    def _add_source_metadata(self, df: DataFrame):

        if self.yetl_properties.metadata_context_id:
            df = self._add_df_metadata(CONTEXT_ID, str(self.context_id), df)

        if self.yetl_properties.metadata_dataflow_id:
            df = self._add_df_metadata(DATAFLOW_ID, str(self.dataflow_id), df)

        if self.yetl_properties.metadata_dataset_id:
            df = self._add_df_metadata(DATASET_ID, str(self.dataset_id), df)

        return df

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
