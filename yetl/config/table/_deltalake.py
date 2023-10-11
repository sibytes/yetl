import logging
from pydantic import Field, PrivateAttr
from .._utils import (
    JinjaVariables,
    render_jinja,
    is_databricks,
    abs_config_path,
    load_text,
)
from typing import Any, Dict, Union, List, Optional
from .._timeslice import Timeslice
import os
from .._stage_type import StageType
from ._table import Table
from ..deltalake import DeltaLakeFn
from pyspark.sql.types import StructType


class DeltaLake(Table):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._spark = DeltaLakeFn(project=self.project)
        self._render()

    @classmethod
    def in_allowed_stages(cls, stage: StageType):
        return stage in (stage.raw, stage.base, stage.curated)

    _logger: Any = PrivateAttr(default=None)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    _spark: DeltaLakeFn = PrivateAttr(default=None)
    depends_on: Optional[List[str]] = Field(default=[])
    delta_properties: Optional[Dict[str, Union[str, bool, int, float]]] = Field(
        default=None
    )
    delta_constraints: Optional[Dict[str, str]] = Field(default=None)
    partition_by: Optional[Union[List[str], str]] = Field(default=None)
    cluster_by: Optional[Union[List[str], str]] = Field(default=None)
    z_order_by: Optional[Union[List[str], str]] = Field(default=None)
    vacuum: Optional[int] = Field(default=31)
    options: Optional[Union[dict, None]] = Field(default=None)
    timeslice: Timeslice = Field(...)
    location: Optional[str] = Field(default=None)
    stage: StageType = Field(...)
    managed: Optional[bool] = Field(default=False)

    sql: Optional[str] = Field(default=None)

    def _load_sql(self, path: str):
        path = abs_config_path(self.config_path, path)
        sql = load_text(path)
        return sql

    def _render(self):
        super()._render()
        if not self._rendered:
            if self.delta_properties:
                delta_properties_sql = self._spark.get_delta_properties_sql(
                    self.delta_properties
                )
                self._replacements[
                    JinjaVariables.DELTA_PROPERTIES
                ] = delta_properties_sql
            self.database = render_jinja(self.database, self._replacements)
            self.table = render_jinja(self.table, self._replacements)
            self.location = render_jinja(self.location, self._replacements)
            self.path = render_jinja(self.path, self._replacements)
            if self.location and self.path:
                self.location = os.path.join(self.location, self.path)
            if not is_databricks():
                self.location = f"{self.config_path}/../data{self.location}"
                self.location = os.path.abspath(self.location)
            self._replacements[JinjaVariables.LOCATION] = self.location

            if self.sql:
                # render the path
                self.sql = render_jinja(self.sql, self._replacements)
                # load the file
                self.sql = self._load_sql(self.sql)
                # render the SQL
                self.sql = render_jinja(self.sql, self._replacements)

            if self.options:
                for option, value in self.options.items():
                    if isinstance(value, str):
                        self.options[option] = render_jinja(value, self._replacements)

        self._rendered = True

    def create_database(self, catalog: str = None):
        super().create_database(catalog=catalog)
        self._spark.create_database(self.database, catalog=self.catalog)

    # TODO: alter table
    def create_table(self, catalog: str = None, schema: StructType = None):
        super().create_table(catalog=catalog)
        if self._spark.table_exists(
            database=self.database, table=self.table, catalog=self.catalog
        ):
            pass
            # TODO: alter table
            if self.catalog:
                msg = f"table `{self.catalog}`.{self.database}`.`{self.table}` already exists."
            else:
                msg = f"table {self.database}`.`{self.table}` already exists."
            self._logger.info(msg)
        else:
            if self.managed:
                self._spark.create_table(
                    database=self.database,
                    table=self.table,
                    delta_properties=self.delta_properties,
                    sql=self.sql,
                    catalog=self.catalog,
                    schema=schema,
                    cluster_by=self.cluster_by,
                    partition_by=self.partition_by,
                )
            else:
                self._spark.create_table(
                    database=self.database,
                    table=self.table,
                    delta_properties=self.delta_properties,
                    path=self.location,
                    sql=self.sql,
                    catalog=self.catalog,
                    schema=schema,
                    cluster_by=self.cluster_by,
                    partition_by=self.partition_by,
                )

    def qualified_table_name(self):
        name = f"`{self.database}`.`{self.table}`"
        if self.catalog:
            name = f"`{self.catalog}`.{name}"
        return name
