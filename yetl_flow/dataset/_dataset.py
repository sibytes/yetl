from ..parser._constants import *
from . import _builtin_functions as builtin_funcs
from ..parser import parser
from ..audit import Audit
from ._base import _Base


class Dataset(_Base):
    def __init__(
        self,
        context,
        database: str,
        table: str,
        dataset: dict,
        io_type: str,
        auditor: Audit,
    ) -> None:

        super().__init__(context, database, table, dataset, io_type, auditor)

        self.path_date_format = dataset.get("path_date_format")
        self.file_date_format = dataset.get("file_date_format")
        self._path = self._get_path(dataset)
        self._timeslice_position = parser.get_slice_position(self.path, self)
        self._path = builtin_funcs.execute_replacements(self._path, self)

        self.auditor.dataset(self.get_metadata())

    def _get_path(self, dataset: dict):
        path = dataset.get(PATH)
        return f"{self.datalake_protocol}{self.datalake}/{path}"

    @property
    def path(self):

        return self._path

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
