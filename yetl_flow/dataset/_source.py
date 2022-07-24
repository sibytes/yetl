from ._validation import PermissiveSchemaOnRead, BadRecordsPathSchemaOnRead
from pyspark.sql import DataFrame
import json
from ._dataset import Dataset
from ._constants import Format, MERGE_SCHEMA, APPEND, BAD_RECORDS_PATH, CORRUPT_RECORD


class Source(Dataset):
    def __init__(
        self, context, database: str, table: str, dataset: dict, io_type: str
    ) -> None:
        super().__init__(context, database, table, dataset, io_type)
        self.dataframe: DataFrame = None
        self.auto_io: bool

    def validate(self):
        pass

    def read(self):
        pass

    def is_source(self):
        return True

    def is_destination(self):
        return False
