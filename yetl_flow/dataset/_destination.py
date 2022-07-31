from ._dataset import Dataset
from pyspark.sql import DataFrame
from ._save import DefaultSave


class Destination(Dataset):
    def __init__(
        self, context, database: str, table: str, dataset: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, dataset, io_type)

        self.auto_io: bool
        self.dataframe: DataFrame

    def write(self):
        super().write()

    def is_source(self):
        return False

    def is_destination(self):
        return True
