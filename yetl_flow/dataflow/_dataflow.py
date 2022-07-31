from pyspark.sql import DataFrame
from ..dataset import Dataset, Source, Destination
from ._i_dataflow import IDataflow
from ..dataset import dataset_factory, Save, DefaultSave
from typing import Type
from enum import Enum


class DataFlowType(Enum):
    """The type of lead e.g. incremental, incremental CDC, full load etc
    FULL_LOAD - Selects all the data, has the option of supplying a slice * mask to the timeslice
    PARTITION_LOAD - Loads / reloads affected partitions
    MERGE_COMPARE - Merges based on a full comparison of change tracking fields
    MERGE_CDC - Merges based on CDC provided metadata
    NUKE - Drops all the tables and data before fully reloading everything
    """

    FULL_LOAD = (0,)
    PARTITION_LOAD = 1
    MERGE_COMPARE = (2,)
    MERGER_CDC = (3,)
    NUKE = 100


class Dataflow(IDataflow):
    def __init__(
        self,
        context,
        config: dict,
        dataflow_config: dict,
        save_type: Type[Save] = DefaultSave,
    ) -> None:

        super().__init__(context, config, dataflow_config, save_type)

        for database, v in dataflow_config.items():
            for table, v in v.items():
                v["datalake"] = self.datalake
                v["datalake_protocol"] = self.datalake_protocol
                v["spark_schema_repo"] = self._spark_schema_repo
                v["deltalake_schema_repo"] = self._deltalake_schema_repo
                v["correlation_id"] = self.context.correlation_id
                v["timeslice"] = self.context.timeslice
                md = dataset_factory.get_dataset_type(
                    self.context, database, table, v, save_type
                )
                self.log.debug(
                    f"Deserialized {database}.{table} configuration into {type(md)}"
                )
                self.append(md)

    def append(self, dataset: Dataset):

        if dataset.is_source():
            self.log.debug(
                f"Appending source {dataset.database_table} as {type(dataset)} to dataflow"
            )
            if dataset.auto_io:
                source: Source = dataset
                source.read()
            self.sources[dataset.database_table] = source

        elif dataset.is_destination():
            self.log.debug(
                f"Appending destination {dataset.database_table} as {type(dataset)} to dataflow"
            )
            self.destinations[dataset.database_table] = dataset

    def source_df(self, database_table: str):

        src: Source = self.sources[database_table]
        return src.dataframe

    def destination_df(self, database_table: str, dataframe: DataFrame):

        dst: Destination = self.destinations[database_table]

        dst.dataframe = dataframe
        if dst.auto_io:
            dst.write()
