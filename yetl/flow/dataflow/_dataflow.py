from pyspark.sql import DataFrame
from ..dataset import Dataset, Source, Destination
from typing import Callable
from ._exceptions import SourceNotFound, DestinationNotFound
from typing import Any
from ._i_dataflow import IDataflow

class Dataflow(IDataflow):

    def __init__(self, **kwargs:Any) -> None:
        super().__init__(**kwargs)

    def audit_lineage(self):
        lineage = {"lineage": {str(self.dataflow_id): {}}}
        for _, d in self.destinations.items():
            src_ids = [str(s.dataset_id) for _, s in self.sources.items()]
            lineage["lineage"][str(self.dataflow_id)][str(d.dataset_id)] = {"depends_on": src_ids}

        self.auditor.dataflow(lineage)

    def append(self, dataset: Dataset):

        if dataset.is_source:
            # self.log.debug(
            #     f"Appending source {dataset.database_table} as {type(dataset)} to dataflow"
            # )
            self.sources[dataset.database_table] = dataset

        elif dataset.is_destination:
            # self.log.debug(
            #     f"Appending destination {dataset.database_table} as {type(dataset)} to dataflow"
            # )
            self.destinations[dataset.database_table] = dataset

    def source_df(self, database_table: str):

        try:
            source: Source = self.sources[database_table]
        except KeyError as e:
            raise SourceNotFound(str(e), self.sources)

        if source.auto_io:
            source.execute()
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
            dst.execute()

