from pydantic import Field, PrivateAttr
from ._properties import ReaderProperties
from ._decoder import parse_properties_key, parse_properties_values
from typing import Any, Dict
import json
from ..parser.parser import JinjaVariables, render_jinja
from ..parser._constants import FormatOptions
from ..file_system import FileSystemType
import uuid
from ._base import Source, SQLTable
from pyspark.sql import DataFrame
from .._timeslice import Timeslice, TimesliceUtcNow
from pydantic import BaseModel, Field
from enum import Enum
from ..context import SparkContext, DatabricksContext
from ..audit import Audit
from pyspark.sql.types import StructType


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


class ThresholdLimit(BaseModel):
    min_rows: int = Field(default=0)
    max_rows: int = Field(default=None)
    exception_count: int = Field(default=0)
    exception_percent: int = Field(default=0)


class Read(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.mode = self.options.get("mode", ReadModeOptions.PERMISSIVE)
        self.infer_schema = self.options.get("inferSchema", False)

    _DEFAULT_OPTIONS = {"mode": ReadModeOptions.PERMISSIVE.value, "inferSchema": False}
    auto: bool = Field(default=True)
    options: Dict[str, Any] = Field(default=_DEFAULT_OPTIONS)
    mode: ReadModeOptions = Field(default=ReadModeOptions.PERMISSIVE)
    infer_schema: bool = Field(default=False)


class Exceptions(BaseModel):
    path: str = Field(...)
    database: str = Field(...)
    table: str = Field(...)


class Thresholds(BaseModel):
    warning: ThresholdLimit = Field(default=ThresholdLimit())
    error: ThresholdLimit = Field(default=ThresholdLimit())


class Reader(Source, SQLTable):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.initialise()

    def initialise(self):
        self.timeslice = self.context.timeslice
        self._replacements = {
            JinjaVariables.DATABASE_NAME: self.database,
            JinjaVariables.TABLE_NAME: self.table,
            JinjaVariables.TIMESLICE_FILE_DATE_FORMAT: self.timeslice.strftime(
                self.file_date_format
            ),
            JinjaVariables.TIMESLICE_PATH_DATE_FORMAT: self.timeslice.strftime(
                self.path_date_format
            ),
        }
        self.datalake_protocol = self.context.datalake_protocol
        self.datalake = self.context.datalake
        self.auditor = self.context.auditor
        path = f"{self.datalake_protocol.value}{self.datalake}/{self.path}"
        self.path = render_jinja(path, self._replacements)
        self.context_id = self.context.context_id
        self.spark_schema = self.context.spark_schema_repository.load_schema(
            database=self.database, table=self.table
        )

    context: SparkContext = Field(...)
    timeslice: Timeslice = Field(default=TimesliceUtcNow())
    context_id: uuid.UUID = Field(default=None)
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

    def validate(self):
        pass

    def execute(self):
        pass

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

    class Config:
        # use a custom decoder to convert the field names
        # back into yetl configuration names
        json_dumps = _yetl_properties_dumps
        arbitrary_types_allowed = True
