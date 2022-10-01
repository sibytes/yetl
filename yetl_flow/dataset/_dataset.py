from ..parser._constants import *
from . import _builtin_functions as builtin_funcs
from ..parser import parser
import uuid
from ..audit import Audit
from datetime import datetime


class Dataset:
    def __init__(
        self,
        context,
        database: str,
        table: str,
        dataset: dict,
        io_type: str,
        auditor: Audit,
    ) -> None:

        self.auditor = auditor
        self.id = uuid.uuid4()
        self.datalake = dataset["datalake"]
        self.datalake_protocol = context.fs.datalake_protocol
        self.context = context
        self.database = database
        self.table = table
        self.database_table = f"{self.database}.{self.table}"
        self.path_date_format = dataset.get("path_date_format")
        self.file_date_format = dataset.get("file_date_format")
        self._path = self._get_path(dataset)
        self._timeslice_position = parser.get_slice_position(self.path, self)
        self._path = builtin_funcs.execute_replacements(self._path, self)
        self.context_id = dataset.get("context_id")
        self.dataflow_id = dataset.get("dataflow_id")
        self.timeslice = dataset.get("timeslice")

        # default format to delta if not
        fmt = dataset.get(FORMAT, Format.DELTA.name)
        self.format_type = Format[fmt.upper()]
        self._initial_load = False

        self.auditor.dataset(self.get_metadata())

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

    def save_metadata(self):
        self.context.metadata_repo.save(self)

    def get_metadata(self):
        metadata = {
            str(self.id): {
                "type": self.__class__.__name__,
                "dataflow_id": str(self.dataflow_id),
                "database": self.database,
                "table": self.table,
                "path": self.path,
            }
        }

        return metadata
