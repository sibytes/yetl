import os
from .table import Table
from ._timeslice import Timeslice
from ._tables import Tables, _INDEX_WILDCARD, KeyContants
from ._stage_type import StageType
from ._utils import abs_config_path, load_yaml, get_config_path, check_version
from ._logging_config import configure_logging
import logging
from ._project import Project
from ..validation import validate_tables


class Config:
    def __init__(
        self,
        project: str,
        pipeline: str,
        timeslice: Timeslice = None,
        config_path: str = None,
    ):
        self.config_path = get_config_path(project, config_path)
        self._logger = logging.getLogger(self.__class__.__name__)
        configure_logging(project, self.config_path)
        if not timeslice:
            timeslice = Timeslice(
                year=_INDEX_WILDCARD, month=_INDEX_WILDCARD, day=_INDEX_WILDCARD
            )
        self.project = self._load_project(project)
        self.pipeline = pipeline
        self.tables = self._load_tables(timeslice)

    def _load_project(self, project: str):
        project_file_path = os.path.join(self.config_path, f"{project}.yaml")
        project_config = load_yaml(project_file_path)
        check_version(project_config)
        project_config["config_path"] = self.config_path
        project = Project(**project_config)
        return project

    def _load_pipeline(self, pipeline: str):
        pipeline_file = f"{pipeline}.yaml"
        config_file_path = os.path.join(self.project.pipelines, pipeline_file)
        pipeline = load_yaml(config_file_path)
        check_version(pipeline)
        return pipeline

    def _load_tables(self, timeslice: Timeslice):
        tables_config = self._load_pipeline(self.pipeline)
        tables_path = tables_config[KeyContants.TABLES.value]
        tables_path = abs_config_path(self.project.pipelines, tables_path)

        data: dict = load_yaml(tables_path)
        validate_tables(data)
        check_version(data)

        tables_config[KeyContants.TABLES.value] = data
        tables_config[KeyContants.TIMESLICE.value] = timeslice
        tables_config[KeyContants.CONFIG_PATH.value] = self.project.pipelines
        tables_config[KeyContants.PROJECT.value] = self.project

        tables = Tables(table_data=tables_config)
        return tables

    def get_table_mapping(
        self,
        stage: StageType,
        table: str = _INDEX_WILDCARD,
        database: str = _INDEX_WILDCARD,
        create_database: bool = True,
        create_table: bool = True,
        catalog: str = None,
        catalog_enabled: bool = True,
    ):
        table_mapping = self.tables.get_table_mapping(
            stage=stage,
            table=table,
            database=database,
            create_database=create_database,
            create_table=create_table,
            catalog=catalog,
            catalog_enabled=catalog_enabled,
        )

        return table_mapping

    def set_checkpoint(
        self,
        source: Table,
        destination: Table,
        checkpoint_name: str = None,
    ):
        if not checkpoint_name:
            checkpoint_name = f"{source.database}.{source.table}-{destination.database}.{destination.table}"

        source.checkpoint = checkpoint_name
        source.render()
        destination.checkpoint = checkpoint_name
        destination.render()
