from pyspark.sql import DataFrame
from ..dataset import Dataset, Source, Destination
from ._i_dataflow import IDataflow
from ..dataset import dataset_factory
from typing import Callable
from enum import Enum
from ._exceptions import SourceNotFound, DestinationNotFound


class Dataflow(IDataflow):
    def __init__(self, context, dataflow_config: dict) -> None:

        super().__init__(context, dataflow_config)

        for database, table in dataflow_config.items():
            for table, table_config in table.items():
                table_config["datalake"] = self.datalake
                table_config["datalake_protocol"] = self.datalake_protocol
                table_config["spark_schema_repo"] = self._spark_schema_repo
                table_config["deltalake_schema_repo"] = self._deltalake_schema_repo
                table_config["pipeline_repo"] = self._pipeline_repo
                table_config["context_id"] = self.context.context_id
                table_config["dataflow_id"] = self.id
                table_config["timeslice"] = self.context.timeslice
                md = dataset_factory.get_dataset_type(
                    self.context, database, table, table_config, self.auditor
                )
                self.log.debug(
                    f"Deserialized {database}.{table} configuration into {type(md)}"
                )
                self.append(md)
                self.audit_lineage()

    def audit_lineage(self):
        lineage = {"lineage": {str(self.id): {}}}
        for _, d in self.destinations.items():
            src_ids = [str(s.id) for _, s in self.sources.items()]
            lineage["lineage"][str(self.id)][str(d.id)] = {"depends_on": src_ids}

        self.auditor.dataflow(lineage)

    def append(self, dataset: Dataset):

        if dataset.is_source():
            self.log.debug(
                f"Appending source {dataset.database_table} as {type(dataset)} to dataflow"
            )
            self.sources[dataset.database_table] = dataset

        elif dataset.is_destination():
            self.log.debug(
                f"Appending destination {dataset.database_table} as {type(dataset)} to dataflow"
            )
            self.destinations[dataset.database_table] = dataset

    def source_df(self, database_table: str):

        try:
            source: Source = self.sources[database_table]
        except KeyError as e:
            raise SourceNotFound(str(e), self.sources)

        if source.auto_io:
            source.read()
        return source.dataframe

    def destination_df(
        self, database_table: str, dataframe: DataFrame, save: Callable = None
    ):

        try:
            dst: Destination = self.destinations[database_table]
        except KeyError as e:
            raise DestinationNotFound(str(e), self.destinations)

        dst.dataframe = dataframe
        if save:
            dst.save = save(dst)

        if dst.auto_io:
            dst.write()
