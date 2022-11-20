from ..audit import Audit
import uuid
from ..parser._constants import *
from pyspark.sql import DataFrame


class Source:
    def read(self) -> DataFrame:
        pass

    def is_source(self):
        return True

    def is_destination(self):
        return False


class Destination:
    def write(self):
        pass

    def is_source(self):
        return False

    def is_destination(self):
        return True


class _Base:
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
        self.context_id = dataset.get("context_id")
        self.dataflow_id = dataset.get("dataflow_id")
        self.timeslice = dataset.get("timeslice")
        # default format to delta if not
        fmt = dataset.get(FORMAT, Format.DELTA.name)
        self.format_type = Format[fmt.upper()]
        self._initial_load = False
        self.auto_io: bool
        self.dataframe: DataFrame

    @property
    def initial_load(self):

        return self._initial_load

    @initial_load.setter
    def initial_load(self, value: bool):
        self._initial_load = value

    @property
    def format(self):
        return self.format_type.value

    def get_metadata(self):
        pass
