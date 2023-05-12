import logging
from pydantic import Field, PrivateAttr
from .._utils import JinjaVariables, render_jinja, get_ddl, load_schema, abs_config_path
from typing import Any, Dict, List, Union
from enum import Enum
import os
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame
from .._stage_type import StageType
from ._table import Table


class TriggerType(Enum):
    File = "file"


class Read(Table):
    _OPTION_CF_SCHEMA_HINTS = "cloudFiles.schemaHints"
    _OPTION_CORRUPT_RECORD_NAME = "columnNameOfCorruptRecord"

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._render()
        self.path = os.path.join(self.location, self.filename)

    _logger: Any = PrivateAttr(default=None)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    create_table: bool = Field(default=True)
    managed: bool = Field(default=False)
    trigger: str = Field(default=None)
    trigger_type: TriggerType = Field(default=None)
    filename: str = Field(...)
    filename_date_format: str = Field(...)
    path_date_format: str = Field(...)
    format: str = Field(...)
    spark_schema: Union[StructType, str] = Field(default=None)
    ddl: List[str] = Field(default=None)
    headerless_ddl: List[str] = Field(default=None)
    stage: StageType = Field(...)

    def _render(self):
        super()._render()
        self._replacements[
            JinjaVariables.FILENAME_DATE_FORMAT
        ] = self.timeslice.strftime(self.filename_date_format)
        self._replacements[JinjaVariables.PATH_DATE_FORMAT] = self.timeslice.strftime(
            self.path_date_format
        )
        if not self._rendered:
            self.location = render_jinja(self.location, self._replacements)
            self.filename = render_jinja(self.filename, self._replacements)
            self.database = render_jinja(self.database, self._replacements)
            self.table = render_jinja(self.table, self._replacements)
            self.trigger = render_jinja(self.trigger, self._replacements)

            if self.options:
                for option, value in self.options.items():
                    self.options[option] = render_jinja(value, self._replacements)

            self._config_schema_hints()

            if isinstance(self.spark_schema, str):
                path = self.spark_schema
                path = render_jinja(self.spark_schema, self._replacements)
                self._load_schema(path)

            corrupt_record_name = self.options.get(
                self._OPTION_CORRUPT_RECORD_NAME, None
            )
            if isinstance(self.spark_schema, StructType) and corrupt_record_name:
                if corrupt_record_name not in self.spark_schema.names:
                    self.spark_schema.add(field=corrupt_record_name, data_type="string")

            self._rendered = True

        if self._rendered and self.options:
            value = self.options.get("checkpointLocation")
            if value:
                self.options["checkpointLocation"] = render_jinja(
                    value, self._replacements
                )

    def _config_schema_hints(self):
        path = self.options.get(self._OPTION_CF_SCHEMA_HINTS, None)
        if path and "/" in path:
            self._load_schema(path)

            if self.options.get("header"):
                self.options[self._OPTION_CF_SCHEMA_HINTS] = ", ".join(self.ddl)
            else:
                self.options[self._OPTION_CF_SCHEMA_HINTS] = ", ".join(
                    self.headerless_ddl
                )

    def _load_schema(self, path: str):
        path = abs_config_path(self.config_path, path)
        if not self.spark_schema or isinstance(self.spark_schema, str):
            self.spark_schema = load_schema(path)
        if not self.ddl:
            self.ddl = get_ddl(self.spark_schema, header=True)
        if not self.headerless_ddl:
            self.headerless_ddl = get_ddl(self.spark_schema, header=False)

    def rename_headerless(self, df: Union[StreamingQuery, DataFrame]):
        columns = [c for c in df.columns if c not in ["_rescued_data"]]
        columns_cnt = len(columns)
        ddls = len(self.ddl)
        if columns_cnt != ddls:
            raise Exception(
                f"Headless files with schema hints must have a fully hinted schema since it must work positionally. Datasets!=dll({columns_cnt}!={ddls}"
            )

        for i, c in enumerate(columns):
            from_name = f"_c{i}"
            to_name = self.ddl[i].split(" ")[0].strip()
            logging.info(f"rename {from_name} to {to_name}")
            df: Union[StreamingQuery, DataFrame] = df.withColumnRenamed(
                from_name, to_name
            )

        return df

    class Config:
        arbitrary_types_allowed = True
