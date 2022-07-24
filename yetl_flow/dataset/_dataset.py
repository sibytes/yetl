from pyspark.sql.types import StructType
from ._constants import *
from . import _builtin_functions as builtin_funcs
from ..schema_repo import ISchemaRepo


class Dataset:
    def __init__(
        self, context, database: str, table: str, dataset: dict, io_type: str
    ) -> None:

        self.datalake = dataset["datalake"]
        self.datalake_protocol = context.fs.datalake_protocol
        self.context = context
        self.database = database
        self.table = table
        self.database_table = f"{self.database}.{self.table}"
        self.path_date_format = dataset.get("path_date_format")
        self.file_date_format = dataset.get("file_date_format")
        self._path = self._get_path(dataset)
        self._path = builtin_funcs.execute_replacements(self._path, self)
        self.correlation_id = dataset.get("correlation_id")
        self.timeslice = dataset.get("timeslice")

        # default format to delta if not
        fmt = dataset.get(FORMAT, Format.DELTA.name)
        self.format_type = Format[fmt.upper()]
        self._initial_load = False

    def _get_path(self, dataset: dict):
        path = dataset.get(PATH)
        return f"{self.datalake_protocol}{self.datalake}/{path}"

    @property
    def path(self):

        return self._path

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        self._initial_load = value

    @property
    def format(self):
        return self.format_type.value

    def is_source(self):
        pass

    def is_destination(self):
        pass
