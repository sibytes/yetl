from ._deltalake import DeltaLake
from ._read import Read
from ._write import Write
from ._table import Table
import logging
from ._table_type import TableType


class TableFactory:
    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._dataset = {}
        self._table = {}

    def register_table_type(self, io_type: TableType, table_type: type):
        self._logger.debug(f"Register table type {table_type} as {type}")
        self._table[io_type] = table_type

    def make(self, table_type: TableType, config: dict) -> Table:
        self._logger.debug(f"Get {table_type.name} from factory dataset")
        table_class = self._table.get(table_type)

        if not table_class:
            self._logger.debug(
                f"TableType {table_type.name} not registered in the table factory"
            )
            raise ValueError(table_type)

        return table_class(
            **config,
        )


factory = TableFactory()
factory.register_table_type(TableType.read, Read)
factory.register_table_type(TableType.delta_lake, DeltaLake)
factory.register_table_type(TableType.write, Write)
