import os
from .dataset import DataSet
from ._timeslice import Timeslice
from ._tables import Tables, _INDEX_WILDCARD
from ._stage_type import StageType
from .dataset import dataset_factory
from ._utils import abs_config_path, load_yaml, get_config_path, check_version
from ._logging_config import configure_logging
from ._project import Project


class Config:
    _TABLES = "tables"
    _CONFIG_PATH = "config_path"

    def __init__(self, project: str, pipeline: str, config_path: str = None):
        self.config_path = get_config_path(project, config_path)
        configure_logging(project, self.config_path)
        self.project = self._load_project(project)
        self.pipeline = self._load_pipeline(pipeline)
        self.tables = self._load_tables()

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

        # add the configuration path into the confif dictionart
        # so that it gets passed to table config when created
        pipeline[self._CONFIG_PATH] = self.project.pipelines
        return pipeline

    def _load_tables(self):
        tables_path = self.pipeline[self._TABLES]
        tables_path = abs_config_path(
            self.project.pipelines, self.pipeline[self._TABLES]
        )

        data = load_yaml(tables_path)
        check_version(data)
        self.pipeline[self._TABLES] = data

        tables = Tables(
            table_data=self.pipeline[self._TABLES],
            config_path=self.pipeline[self._CONFIG_PATH],
        )
        return tables

    def get_table_mapping(
        self,
        timeslice: Timeslice,
        stage: StageType,
        table: str = _INDEX_WILDCARD,
        database: str = _INDEX_WILDCARD,
    ):
        table_mapping = self.tables.get_table_mapping(
            stage=stage, table=table, database=database
        )

        table_mapping.source = dataset_factory.get_data_set(
            self.pipeline, table_mapping.source, timeslice
        )
        table_mapping.destination = dataset_factory.get_data_set(
            self.pipeline, table_mapping.destination, timeslice
        )

        return table_mapping

    def set_checkpoint(
        self,
        source: DataSet,
        destination: DataSet,
        checkpoint_name: str = None,
    ):
        if not checkpoint_name:
            checkpoint_name = f"{source.database}.{source.table}-{destination.database}.{destination.table}"

        source.checkpoint = checkpoint_name
        source._render()
        destination.checkpoint = checkpoint_name
        destination.options["checkpointLocation"] = destination.checkpoint_location
        destination._render()
