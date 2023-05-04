import logging
from pydantic import Field, PrivateAttr
from .._utils import JinjaVariables, render_jinja, is_databricks
from typing import Any, Dict, Union, List
from .._timeslice import Timeslice
import os
from .._stage_type import StageType
from ._dataset import DataSet, Table
from ..datallake import create_database, create_table

# try:
#     from databricks.sdk.runtime import *  # noqa F403
# except ModuleNotFoundError:
#     pass


class DeltaLakeTable(Table):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    depends_on: List[str] = Field(default=[])
    delta_properties: Dict[str, str] = Field(default=None)
    delta_constraints: Dict[str, str] = Field(default=None)
    partition_by: List[str] = Field(default=None)
    z_order_by: List[str] = Field(default=None)
    create_table: bool = Field(default=True)
    managed: bool = Field(default=False)


class DeltaLake(DataSet, DeltaLakeTable):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._render()
        self.create_delta_table()

    @classmethod
    def in_allowed_stages(cls, stage: StageType):
        return stage in (stage.raw, stage.base, stage.curated)

    _logger: Any = PrivateAttr(default=None)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    options: Union[dict, None] = Field(default=None)
    timeslice: Timeslice = Field(...)
    location: str = Field(default=None)
    checkpoint_location: str = Field(default=None)
    stage: StageType = Field(...)
    managed: bool = Field(default=False)
    create_table: bool = Field(default=True)

    def _render(self):
        self._replacements = {
            JinjaVariables.TABLE: self.table,
            JinjaVariables.DATABASE: self.database,
            JinjaVariables.CONTAINER: self.container,
            JinjaVariables.CHECKPOINT: self.checkpoint,
        }

        self.root = render_jinja(self.root, self._replacements)
        self.path = render_jinja(self.path, self._replacements)
        self.database = render_jinja(self.database, self._replacements)
        self.table = render_jinja(self.table, self._replacements)
        if self.options:
            for option, value in self.options.items():
                self.options[option] = render_jinja(value, self._replacements)

        self.location = os.path.join(self.root, self.path)
        if not is_databricks():
            self.location = f"{self.config_path}/../data{self.location}"
            self.location = os.path.abspath(self.location)

    def create_delta_table(self):
        database_table = f"`{self.database}`.`{self.table}`"
        self._logger.info(f"Creating delta lake table {database_table}")
        create_database(self.database)

        if self.managed:
            create_table(
                database=self.database,
                table=self.table,
                delta_properties=self.delta_properties,
            )
        else:
            create_table(
                database=self.database,
                table=self.table,
                delta_properties=self.delta_properties,
                path=self.location,
            )
