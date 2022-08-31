from ._dataset import Dataset
from pyspark.sql import DataFrame
from ._save import DefaultSave
from pyspark.sql import functions as fn
from ._constants import *


class Destination(Dataset):
    def __init__(
        self, context, database: str, table: str, dataset: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, dataset, io_type)

        self.auto_io: bool
        self.dataframe: DataFrame

    def write(self):

        self.context.log.debug(
            f"Reordering sys_columns to end for {self.database_table} from {self.path} {CONTEXT_ID}={str(self.context_id)}"
        )
        # remove a re-add the _context_id since there will be dupplicate columns
        # when dataframe is built from multiple sources.
        self.dataframe = self.dataframe.drop(CONTEXT_ID).withColumn(
            CONTEXT_ID, fn.lit(str(self.context_id))
        )
        sys_columns = [c for c in self.dataframe.columns if c.startswith("_")]
        data_columns = [c for c in self.dataframe.columns if not c.startswith("_")]
        data_columns = data_columns + sys_columns
        self.dataframe = self.dataframe.select(*data_columns)
        super().write()
        # self.save_metadata()

    def is_source(self):
        return False

    def is_destination(self):
        return True

    def save_metadata(self):
        return super().save_metadata()
