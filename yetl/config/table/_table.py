import logging
from pydantic import BaseModel, Field, PrivateAttr
from .._utils import JinjaVariables
from typing import Any, Dict, Union, List
from .._timeslice import Timeslice
from .._stage_type import StageType
from ._table_type import TableType
from .._project import Project
from enum import Enum
from .._utils import render_jinja


class ValidationThresholdType(Enum):
    exception = ("exception",)
    warning = "warning"


class ValidationThreshold(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    invalid_ratio: float = Field(default=None)
    invalid_rows: int = Field(default=None)
    max_rows: int = Field(default=None)
    min_rows: int = Field(default=None)

    @classmethod
    def default_select_sql(cls):
        sql = "null"
        return sql

    def select_sql(self):
        thresholds_sql = []
        if self.invalid_ratio is not None:
            thresholds_sql.append(
                f"cast({self.invalid_ratio} as double) as invalid_ratio"
            )
        else:
            thresholds_sql.append("null as invalid_ratio")

        if self.invalid_rows is not None:
            thresholds_sql.append(f"cast({self.invalid_rows} as long) as invalid_rows")
        else:
            thresholds_sql.append("null as invalid_rows")

        if self.max_rows is not None:
            thresholds_sql.append(f"cast({self.max_rows} as long) as max_rows")
        else:
            thresholds_sql.append("null as max_rows")

        if self.min_rows is not None:
            thresholds_sql.append(f"cast({self.min_rows} as long) as min_rows")
        else:
            thresholds_sql.append("null as min_rows")

        sql = f"""
            struct(
                {",".join(thresholds_sql)}
            )
        """

        return sql


class Table(BaseModel):
    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._logger = logging.getLogger(self.__class__.__name__)

    _logger: Any = PrivateAttr(default=None)
    _rendered: bool = PrivateAttr(default=False)
    _replacements: Dict[JinjaVariables, str] = PrivateAttr(default=None)
    stage: StageType = Field(...)
    database: str = Field(...)
    table: str = Field(...)
    id: Union[str, List[str]] = Field(default=[])
    custom_properties: Dict[str, Any] = Field(default=None)
    table_type: TableType = Field(...)
    warning_thresholds: ValidationThreshold = Field(default=None)
    exception_thresholds: ValidationThreshold = Field(default=None)
    project: Project = Field(...)
    container: str = Field(...)
    location: str = Field(...)
    path: str = Field(default=None)
    options: dict = Field(...)
    timeslice: Timeslice = Field(...)
    checkpoint: str = Field(default=None)
    config_path: str = Field(...)
    catalog: str = Field(default=None)
    catalog_enabled: bool = Field(default=True)

    def _render(self):
        self._replacements = {
            JinjaVariables.TABLE: self.table,
            JinjaVariables.DATABASE: self.database,
            JinjaVariables.CONTAINER: self.container,
            JinjaVariables.CHECKPOINT: self.checkpoint,
            JinjaVariables.PROJECT: self.project.name,
            JinjaVariables.CATALOG: self.catalog,
        }

    def render(self):
        self._replacements[JinjaVariables.CHECKPOINT] = self.checkpoint

        if self.options:
            for option, value in self.options.items():
                if isinstance(value, str):
                    self.options[option] = render_jinja(value, self._replacements)

    def thresholds_select_sql(self, threshold_type: ValidationThresholdType):
        if threshold_type == ValidationThresholdType.exception:
            if self.exception_thresholds:
                return self.exception_thresholds.select_sql()
            else:
                return ValidationThreshold.default_select_sql()

        if threshold_type == ValidationThresholdType.warning:
            if self.warning_thresholds:
                return self.warning_thresholds.select_sql()
            else:
                return ValidationThreshold.default_select_sql()

    def _set_catalog(self, catalog: str = None, catalog_enabled: bool = True):
        if catalog:
            self.catalog = catalog
        self.catalog_enabled = catalog_enabled
        if not self.catalog_enabled:
            self.catalog = None

    def create_table(self, catalog: str = None, catalog_enabled: bool = True):
        self._set_catalog(catalog, catalog_enabled)

    def create_database(self, catalog: str = None, catalog_enabled: bool = True):
        self._set_catalog(catalog, catalog_enabled)

    def qualified_table_name(self):
        pass
