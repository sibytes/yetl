import logging
from pydantic import Field, PrivateAttr
from .._utils import (
    JinjaVariables,
    render_jinja,
    is_databricks,
    abs_config_path,
    load_text,
)
from typing import Any, Dict, Union, List
from .._timeslice import Timeslice
import os
from .._stage_type import StageType
from ._table import Table
from ..deltalake import DeltaLakeFn

# from ..datallake import create_database, create_table

# try:
#     from databricks.sdk.runtime import *  # noqa F403
# except ModuleNotFoundError:
#     pass


class DeltaLake(Table):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._spark = DeltaLakeFn(project=self.project)
        self._render()
        if self.create_table:
            self.create_delta_table()

    @classmethod
    def in_allowed_stages(cls, stage: StageType):
        return stage in (stage.raw, stage.base, stage.curated)

    _logger: Any = PrivateAttr(default=None)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    _spark: DeltaLakeFn = PrivateAttr(default=None)
    depends_on: List[str] = Field(default=[])
    delta_properties: Dict[str, str] = Field(default=None)
    delta_constraints: Dict[str, str] = Field(default=None)
    partition_by: List[str] = Field(default=None)
    z_order_by: List[str] = Field(default=None)
    create_table: bool = Field(default=True)
    managed: bool = Field(default=False)
    options: Union[dict, None] = Field(default=None)
    timeslice: Timeslice = Field(...)
    location: str = Field(default=None)
    checkpoint_location: str = Field(default=None)
    stage: StageType = Field(...)
    managed: bool = Field(default=False)
    create_table: bool = Field(default=True)
    sql: str = Field(default=None)

    def _load_sql(self, path: str):
        path = abs_config_path(self.config_path, path)
        sql = load_text(path)
        return sql

    def _render(self):
        self._replacements = {
            JinjaVariables.TABLE: self.table,
            JinjaVariables.DATABASE: self.database,
            JinjaVariables.CONTAINER: self.container,
            JinjaVariables.CHECKPOINT: self.checkpoint,
        }
        if self.delta_properties:
            delta_properties_sql = self._spark.get_delta_properties_sql(
                self.delta_properties
            )
            self._replacements[JinjaVariables.DELTA_PROPERTIES] = delta_properties_sql
        self.database = render_jinja(self.database, self._replacements)
        self.table = render_jinja(self.table, self._replacements)
        self.location = render_jinja(self.location, self._replacements)
        self.path = render_jinja(self.path, self._replacements)
        self.location = os.path.join(self.location, self.path)
        if not is_databricks():
            self.location = f"{self.config_path}/../data{self.location}"
            self.location = os.path.abspath(self.location)
        self._replacements[JinjaVariables.LOCATION] = self.location

        if self.options:
            for option, value in self.options.items():
                self.options[option] = render_jinja(value, self._replacements)

        if self.sql:
            # render the path
            self.sql = render_jinja(self.sql, self._replacements)
            # load the file
            self.sql = self._load_sql(self.sql)
            # render the SQL
            self.sql = render_jinja(self.sql, self._replacements)

    # TODO: Create or alter table
    def create_delta_table(self):
        self._spark.create_database(self.database)

        if self.managed:
            self._spark.create_table(
                database=self.database,
                table=self.table,
                delta_properties=self.delta_properties,
                sql=self.sql,
            )
        else:
            self._spark.create_table(
                database=self.database,
                table=self.table,
                delta_properties=self.delta_properties,
                path=self.location,
                sql=self.sql,
            )
